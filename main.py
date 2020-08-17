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

global_bootstrap_nodes = [
    ("router.utorrent.com", 6881),
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("dht.aelitis.com", 6881),  # Vuze
    ("router.silotis.us", 6881),  # IPv6
    ("dht.libtorrent.org", 25401),  # @arvidn's
]

logger = logging.root


def addr_for_db(addr: Tuple[str, int]) -> str:
    return addr[0] + ":" + str(addr[1])


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


Addr = Tuple[str, int]


@dataclass(unsafe_hash=True, order=True)
class NodeInfo:
    id: Optional[PeerId]
    addr: Addr


def distance(a, b: typing.ByteString) -> int:
    return int.from_bytes(a, "big") ^ int.from_bytes(b, "big")


TransactionId = NewType("TransactionId", bytes)


class Bootstrap:
    active: Dict[TransactionId, trio.MemorySendChannel] = {}
    alpha: int = 3
    queried: Set[Addr] = set()

    def __init__(self, socket, target: typing.ByteString, nursery):
        self.socket = socket
        self.target = target
        # this is sorted so that the best candidates are at the end, since pop defaults to popping from the back
        self.backlog: SortedSet[NodeInfo] = SortedSet(key=self._distance_key)
        self.exhausted = trio.Event()
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

    def add_candidates(self, *addrs):
        self.backlog.update([NodeInfo(None, addr) for addr in addrs])
        self.try_do_sends()

    def on_reply(self, reply, src):
        msg = typing.cast(bencode.Dict, bencode.parse_one_from_bytes(reply))
        # logging.debug("got reply:\n%s", pformat(msg))
        key = msg[b"t"]
        if key not in (active := self.active):
            logging.warning("got unexpected reply: %r", key)
            return
        self.nursery.start_soon(active[key].send, msg)
        try:
            replier_id = msg[b"r"][b"id"]
        except KeyError:
            logging.warning(
                "got reply from %r with no replier id:\n%s", src, pformat(msg)
            )
        else:
            pass
            # logging.debug("got reply from %s at %s", replier_id.hex(), src)

    def start_query(self, addr, q, a=None):
        if a is None:
            a = {}
        a["id"] = self.target
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

    async def do_query(self, bytes, addr: Addr, response_receiver, key: TransactionId):
        try:
            async with response_receiver:
                try:
                    await self.socket.sendto(bytes, addr)
                except socket.gaierror:
                    logging.warning("error sending to %r: %s", addr, sys.exc_info()[1])
                else:
                    with trio.move_on_after(5):
                        reply: bencode.Dict = await response_receiver.receive()
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
        self.try_do_sends()

    def find_node(self, addr):
        return self.start_query(addr, "find_node", a={"target": self.target})

    def start_next(self):
        node_info = self.backlog.pop()
        if len(self.responded) >= 8:
            if self._distance_key(node_info) <= self._distance_key(self.responded[-8]):
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
            else distance(self.target, node_info.id.bytes).to_bytes(20, "big").hex(),
        )
        return self.find_node(node_info.addr)

    def try_do_sends(self):
        while len(self.active) < self.alpha and self.backlog:
            self.start_next()


class RoutingTable:
    def __init__(self, root):
        self.buckets = []


@dataclass
class Sender:
    socket: typing.Any
    db_conn: typing.Any

    async def sendto(self, bytes, addr):
        try:
            await self.socket.sendto(
                bytes, addr,
            )
        finally:
            exc_value = sys.exc_info()[1]
            if exc_value is not None:
                exc_value = str(exc_value)
            with self.db_conn:
                try:
                    record_operation(
                        self.db_conn, "send", addr_for_db(addr), bytes, exc_value
                    )
                except ValueError:
                    breakpoint()
                    raise


async def ping_bootstrap_nodes(sender, db_conn):
    for addr in global_bootstrap_nodes:
        bytes = "".join(
            bencode.encode(
                {"t": "aa", "y": "q", "q": "ping", "a": {"id": "abcdefghij0123456789"},}
            )
        ).encode()
        sender.sendto(bytes, addr)


def new_message_id(db_conn: sqlite3.Connection) -> int:
    cursor = db_conn.cursor()
    cursor.execute("insert into messages default values")
    return cursor.lastrowid


def record_operation(
    db_conn, type: str, remote_addr: str, bytes: bytes, error: Union[str, None]
):
    with db_conn:
        message_id = new_message_id(db_conn)
        db_conn.execute(
            "insert into operations (message_id, remote_addr, type, error) values (?, ?, ?, ?)",
            [message_id, remote_addr, type, error],
        )
        record_packet(bytes, db_conn, message_id)


def record_packet(bytes, db_conn, top_id):
    # logging.debug("recording packet %r", bytes)
    bencode.StreamDecoder(bencode.BytesStreamReader(bytes)).visit(
        MessageWriter(db_conn.cursor(), top_id)
    )


@dataclass
class FieldContext:
    parent_id: typing.Union[None, int]
    index: int = 0


class MessageWriter:

    cursor: sqlite3.Cursor

    def __init__(self, cursor, top_id):
        self.cursor = cursor
        self.field_contexts = [FieldContext(top_id)]

    def _cur_parent_id(self) -> typing.Union[int, None]:
        return self.field_contexts[-1].parent_id

    def _cur_field_context(self) -> FieldContext:
        return self.field_contexts[-1]

    def _insert_code(self, code):
        self._insert(code, None)

    def _insert(self, code, value):
        parent_id = self._cur_parent_id()
        self.cursor.execute(
            """insert into messages (parent_id, "index", depth, type, value) values (?, ?, ?, ?, ?)""",
            [
                parent_id,
                self._cur_field_context().index,
                self._cur_depth(),
                code,
                value,
            ],
        )
        if parent_id is None:
            self.on_root_insert(self.cursor.lastrowid)
        self._cur_field_context().index += 1

    def _cur_depth(self):
        return len(self.field_contexts) - 1

    def _start(self, code):
        self._insert_code(code)
        self.field_contexts.append(FieldContext(self.cursor.lastrowid))

    def start_dict(self):
        self._start("d")

    def start_list(self):
        self._start("l")

    def end(self):
        self._insert_code("e")
        self.field_contexts.pop()

    def int(self, value):
        self._insert("i", value)

    def str(self, value):
        self._insert("s", value)


async def receiver(socket, db_conn):
    while True:
        bytes, addr = await socket.recvfrom(0x1000)
        record_operation(db_conn, "recv", addr_for_db(addr), bytes, None)
        yield bytes, addr


def string_to_address_tuple(s):
    host, port = s.rsplit(":")
    return host, int(port)


async def sample_infohashes_for_db(args, db_conn, socket):
    queries: Dict[bytes, Any] = {}
    sender = Sender(socket, db_conn)

    async def handle_received():
        async for bytes, addr in receiver(socket, db_conn):
            print(f"received from {addr}: {bytes}")

    async def sample_infohashes():
        for (addr,) in db_conn.execute(
            "select distinct remote_addr from operations where type='recv'"
        ):
            t = secrets.token_bytes(8)
            try:
                await sender.sendto(
                    bencode.encode_to_bytes(
                        {
                            "t": t,
                            "a": {
                                "id": secrets.token_bytes(20),
                                "target": secrets.token_bytes(20),
                            },
                            "y": "q",
                            "q": "sample_infohashes",
                        }
                    ),
                    string_to_address_tuple(addr),
                )
            except trio.socket.gaierror as exc:
                logger.warning("sending to %s: %s", addr, exc)
            else:
                print(f"messaged {addr}")
            await trio.sleep(0.1)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(sample_infohashes)
        await handle_received()


async def bootstrap(args, db_conn, socket):
    sender = Sender(socket, db_conn)
    async with trio.open_nursery() as nursery:
        target_id = secrets.token_bytes(20)
        logging.info("bootstrap target id: %s", target_id.hex())
        bootstrap = Bootstrap(sender, target_id, nursery=nursery)
        bootstrap.add_candidates(*global_bootstrap_nodes)
        async for bytes, addr in receiver(socket, db_conn):
            bootstrap.on_reply(bytes, addr)


async def main():
    logging.Formatter.default_msec_format = "%s.%03d"
    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOGLEVEL", "INFO")),
        style="{",
        format="{module}:{lineno} {message}",
    )
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True, dest="cmd")
    sample_infohashes_parser = subparsers.add_parser("sample_infohashes")
    sample_infohashes_parser.set_defaults(func=sample_infohashes_for_db)
    bootstrap_parser = subparsers.add_parser("bootstrap")
    bootstrap_parser.set_defaults(func=bootstrap)
    args = parser.parse_args()
    db_conn = sqlite3.connect("herp.db")
    socket = trio.socket.socket(type=trio.socket.SOCK_DGRAM)
    await socket.bind(("", 42069))
    await args.func(args, db_conn, socket)


if __name__ == "__main__":
    trio.run(main)
