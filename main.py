from pprint import *
from typing import *
import trio
import bencode
import sqlite3
import logging
import typing
import functools
from sortedcontainers import SortedSet
import secrets
import struct
from trio import socket
import sys
from dataclasses import dataclass, field
from typing import Union, Tuple
import argparse
import os
from abc import abstractmethod
import sql
from my_types import Addr
from util import chunk_bytes
from itertools import repeat
import hashlib

global_bootstrap_nodes: List[Addr] = [
    ("router.utorrent.com", 6881),
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("dht.aelitis.com", 6881),  # Vuze
    ("router.silotis.us", 6881),  # IPv6
    ("dht.libtorrent.org", 25401),  # @arvidn's
]

logger = logging.root


@dataclass(unsafe_hash=True)
class PeerId:
    BytesType = bytes  # bytes is overloaded in this scope
    bytes: BytesType

    def __lt__(self, other):
        if other is None:
            return False
        return self.bytes < other.bytes

    def __repr__(self):
        return self.bytes.hex()


@dataclass(unsafe_hash=True, order=True)
class NodeInfo:
    id: Optional[PeerId]
    addr: Addr


def distance(a, b: typing.ByteString) -> int:
    return int.from_bytes(a, "big") ^ int.from_bytes(b, "big")


def distance_hex(a, b) -> str:
    return distance(a, b).to_bytes(20, "big").hex()


TransactionId = NewType("TransactionId", bytes)


class Traversal:
    alpha: int = 3

    def __init__(self, socket, target: typing.ByteString, nursery, local_id: bytes):
        self.active: Dict[TransactionId, trio.MemorySendChannel] = {}
        self.queried: Set[Addr] = set()
        self.local_id = local_id
        self.socket = socket
        self.target = target
        # this is sorted so that the best candidates are at the end, since pop defaults to popping from the back
        self.backlog: SortedSet[NodeInfo] = SortedSet(key=self._distance_key)
        self.exhausted = trio.Condition()
        self.nursery = nursery
        self.responded: SortedSet[NodeInfo] = SortedSet(key=self._distance_key)

    def _distance_key(
        self, elem: NodeInfo
    ) -> Union[Tuple[Literal[False], int], Tuple[Literal[True], int, int]]:
        addr = hash(elem.addr)
        if elem.id is None:
            return False, addr
        else:
            return (
                True,
                -(distance(self.target, elem.id.bytes)),
                addr,
            )

    def new_transaction_id(self) -> TransactionId:
        return TransactionId(secrets.token_bytes(8))

    def add_candidates(self, *addrs: Addr):
        self.backlog.update([NodeInfo(None, addr) for addr in addrs])
        self.try_do_sends()

    def on_reply(self, reply, src):
        msg = typing.cast(bencode.Dict, bencode.parse_one_from_bytes(reply))
        # logging.debug("got reply:\n%s", pformat(msg))
        key = msg[b"t"]
        if key not in self.active:
            logging.warning("got unexpected reply: %r", key)
            return
        self.nursery.start_soon(self.active[key].send, msg)
        try:
            replier_id = msg[b"r"][b"id"]
        except KeyError:
            logging.warning(
                "got reply from %r with no replier id:\n%s", src, pformat(msg)
            )

    def start_query(self, addr, q, a=None):
        if a is None:
            a = {}
        a["id"] = self.local_id
        tid = self.new_transaction_id()
        msg = {"t": tid, "y": "q", "q": q, "a": a}
        key = tid
        if key in self.active:
            raise KeyError("already in use")
        send_channel, receive_channel = trio.open_memory_channel[bencode.Dict](0)
        self.active[key] = send_channel
        self.queried.add(addr)
        self.nursery.start_soon(
            self.do_query, b"".join(bencode.encode(msg)), addr, receive_channel, key
        )

    def process_reply_nodes(self, nodes):
        for id, packed_ip, port in struct.iter_unpack("!20s4sH", nodes):
            self.backlog.add(NodeInfo(PeerId(id), (socket.inet_ntoa(packed_ip), port),))
        self.try_do_sends()

    async def do_query(
        self,
        bytes,
        addr: Addr,
        response_receiver: trio.MemoryReceiveChannel,
        key: TransactionId,
    ):
        try:
            async with response_receiver:
                try:
                    await self.socket.sendto(bytes, addr)
                except socket.gaierror:
                    logging.warning("error sending to %r: %s", addr, sys.exc_info()[1])
                else:
                    with trio.move_on_after(5):
                        reply: bencode.Dict = await response_receiver.receive()
                        self.on_response(reply)
                        if b"r" in reply:
                            reply_id: bytes = reply[b"r"][b"id"]
                            self.responded.add(NodeInfo(PeerId(reply_id), addr))
                            try:
                                nodes = reply[b"r"][b"nodes"]
                            except KeyError:
                                pass
                            else:
                                self.process_reply_nodes(nodes)
                        if b"e" in reply:
                            logging.error(
                                "got error from %s: %s\nwe sent: %r", addr, reply, bytes
                            )
        finally:
            del self.active[key]
            await self._try_notify_exhausted()
        self.try_do_sends()

    def on_response(self, response: bencode.Dict):
        pass

    @abstractmethod
    def query(self) -> Tuple[str, bencode.Dict]:
        ...

    def find_node(self):
        return "find_node", {"target": self.target}

    def start_next(self):
        try:
            node_info = self.backlog.pop()
            # We don't drop nodes without an ID, since we don't know what it
            # is, it could actually be closer.
            if len(self.responded) >= 8 and node_info.id is not None:
                # Note that we use >, since we could have multiple candidates
                # with the same distance for silly reasons.
                if distance(node_info.id.bytes, self.target) > distance(
                    self.responded[-8].id.bytes, self.target
                ):
                    logging.debug("discarding divergent candidate %r", node_info)
                    return
            addr = node_info.addr
            if addr in self.queried:
                logging.warning("skipping already queried addr %r", addr)
                return
            logger.debug(
                "picked %r for next query (distance=%s)",
                node_info,
                None
                if node_info.id is None
                else distance_hex(self.target, node_info.id.bytes),
            )
            return self.start_query(addr, *self.query())
        finally:
            self.nursery.start_soon(self._try_notify_exhausted)

    def try_do_sends(self):
        while len(self.active) < self.alpha and self.backlog:
            self.start_next()

    async def wait_exhausted(self):
        async with self.exhausted:
            while not self._exhausted_predicate():
                await self.exhausted.wait()

    async def _try_notify_exhausted(self):
        async with self.exhausted:
            if self._exhausted_predicate():
                self.exhausted.notify_all()

    def _exhausted_predicate(self):
        return not self.active and not self.backlog


class Bootstrap(Traversal):

    query = Traversal.find_node


class SampleInfohashes(Traversal):
    def __init__(self, *args, **kwargs):
        self.__sampled_infohashes: Set[bytes] = set()
        self.__db_conn = args[0]
        super().__init__(*args[1:], **kwargs)

    def query(self):
        return "sample_infohashes", {"target": self.target}

    def on_response(self, response):
        if "r" not in response:
            return
        try:
            with self.__db_conn:
                cursor = self.__db_conn.cursor()
                cursor.execute(
                    "insert into sample_infohashes_response(time, query_t, num, interval) values (datetime('now'), ?, ?, ?)",
                    (
                        response["t"],
                        response["r"].get("num"),
                        response["r"].get("interval"),
                    ),
                )
                response_id = cursor.lastrowid
                try:
                    samples = response["r"]["samples"]
                except KeyError:
                    pass
                else:
                    infohashes = list(chunk_bytes(samples, 20, strict=True))
                    cursor.executemany(
                        "insert into sample_infohashes_response_infohash(response_id, infohash) values (?, ?)",
                        zip(repeat(response_id), infohashes),
                    )
                    for ih in infohashes:
                        if ih not in self.__sampled_infohashes:
                            logging.info("got new infohash %s", ih.hex())
                            self.__sampled_infohashes.add(ih)
        except Exception:
            logging.error("exception handling\n%s", pformat(response))
            raise


class RoutingTable:
    def __init__(self, root):
        self.buckets = []


async def ping_bootstrap_nodes(sender, db_conn):
    for addr in global_bootstrap_nodes:
        bytes = "".join(
            bencode.encode(
                {"t": "aa", "y": "q", "q": "ping", "a": {"id": "abcdefghij0123456789"},}
            )
        ).encode()
        sender.sendto(bytes, addr)


def string_to_address_tuple(s):
    host, port = s.rsplit(":")
    return host, int(port)


async def sample_infohashes(args, db_conn, socket):
    local_id = secrets.token_bytes(20)
    while True:
        target = secrets.token_bytes(20)
        logger.info("sampling toward %s", target.hex())
        async with trio.open_nursery() as nursery:
            traversal = SampleInfohashes(db_conn, socket, target, nursery, local_id)
            nursery.start_soon(receive_for_traversal, socket, traversal)
            traversal.add_candidates(
                *map(
                    lambda x: string_to_address_tuple(x[0]),
                    db_conn.execute(
                        "select remote_addr from operation where type='recv'"
                    ),
                )
            )
            await traversal.wait_exhausted()
            nursery.cancel_scope.cancel()


async def receive_for_traversal(socket, traversal):
    while True:
        bytes, addr = await socket.recvfrom(0x1000)
        traversal.on_reply(bytes, addr)


async def bootstrap(args, db_conn, socket):

    async with trio.open_nursery() as nursery:
        target_id = secrets.token_bytes(20)
        logging.info("bootstrap target id: %s", target_id.hex())
        bootstrap = Bootstrap(socket, target_id, nursery, secrets.token_bytes(20))
        nursery.start_soon(receive_for_traversal, socket, bootstrap)
        bootstrap.add_candidates(*global_bootstrap_nodes)
        await bootstrap.wait_exhausted()
        logging.debug("bootstrap exhausted")
        nursery.cancel_scope.cancel()
    for elem in bootstrap.responded:
        print(distance_hex(elem.id.bytes, target_id), elem)


async def single_query(args, db_conn, socket):
    async with trio.open_nursery() as nursery:
        tid_to_addr = {}

        async def handle_received():
            while True:
                bytes, addr = await socket.recvfrom(0x1000)
                reply = bencode.parse_bytes(bytes)
                print(
                    f"reply from {addr} (for send to {tid_to_addr[reply[0][b't']]}):\n{pformat(reply)}"
                )

        nursery.start_soon(handle_received)
        pending = 0
        for addr in args.addrs:

            query_args = {"id": args.id}
            if args.target is not None:
                query_args["target"] = args.target
            tid = secrets.token_bytes(8)
            msg = {
                "t": tid,
                "y": "q",
                "q": args.query,
                "a": query_args,
            }
            try:
                await socket.sendto(bencode.encode_to_bytes(msg), addr)
            except trio.socket.gaierror as exc:
                print(f"sending to {addr}: {exc}", file=sys.stderr)
            else:
                pending += 1
                tid_to_addr[tid] = addr
        await trio.sleep(10)
        nursery.cancel_scope.cancel()


class TargetAction(argparse.Action):
    def __call__(self, parser, namespace, value, option_string):
        if value == "random":
            value = secrets.token_bytes(20)
        else:
            value = bytes.fromhex(value)
        setattr(namespace, "target", value)


async def check_infos(args, db_conn, socket):
    total = 0
    correct = 0
    for infohash, bytes in db_conn.execute(
        "select infohash, bytes from info where bytes is not null"
    ):
        total += 1
        if hashlib.new("sha1", bytes).hexdigest() == infohash:
            correct += 1
        else:
            logging.error("bad info bytes for %s", infohash)
    print(f'{correct}/{total} infos are valid', file=sys.stderr)

async def list_files(args, db_conn, socket):
    total = 0
    correct = 0
    for infohash, bytes in db_conn.execute(
        "select infohash, bytes from info where bytes is not null"
    ):
        info = bencode.parse_one_from_bytes(bytes)
        print(infohash)
        print('', info['name'].decode())
        for file in info.get('files', []):
            print('', '', *(p.decode() for p in file['path']))

async def main():
    logging.Formatter.default_msec_format = "%s.%03d"
    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOGLEVEL", "INFO").upper()),
        style="{",
        format="{module}:{lineno} {message}",
    )

    parser = argparse.ArgumentParser()
    # parser.add_argument("--clobber-db", action="store_true")
    subparsers = parser.add_subparsers(required=True, dest="cmd")

    def add_command(command, func=None):
        cmd_parser = subparsers.add_parser(command)
        cmd_parser.set_defaults(func=func or globals()[command])
        return cmd_parser

    add_command("sample_infohashes")
    add_command("bootstrap")
    add_command("check_infos")
    add_command("list_files")
    single_query_parser = add_command("single_query")
    single_query_parser.add_argument("--addrs", default=global_bootstrap_nodes)
    single_query_parser.add_argument("query")
    single_query_parser.add_argument("--id", default=secrets.token_bytes(20))
    single_query_parser.add_argument("--target", action=TargetAction)
    args = parser.parse_args()

    db_conn = sql.connect()
    # if args.clobber_db:
    #     drop_tables(tables)
    # create_tables(tables, safe=not args.clobber_db)
    socket = trio.socket.socket(type=trio.socket.SOCK_DGRAM)
    await socket.bind(("", 42069))
    await args.func(args, db_conn, sql.RecordedSocket(socket, db_conn))


if __name__ == "__main__":
    trio.run(main)
