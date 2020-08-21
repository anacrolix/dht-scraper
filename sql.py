from vtfunc import TableFunction
import apsw
from dataclasses import dataclass
import typing, sys
from typing import *
import bencode
from my_types import *
import sqlite3
import os

DB_PATH = "herp.db"
USE_APSW = os.environ.get("USE_APSW", False)

if USE_APSW:

    def connect():
        db = apsw.Connection("herp.db")
        db.createscalarfunction("bencode_get", bencode_get, deterministic=True)
        db.createmodule("chunk_bytes", ChunkBytes)
        return db


else:
    sqlite3.enable_callback_tracebacks(True)

    def connect():
        db = sqlite3.connect("herp.db")
        ChunkBytes.register(db)
        db.create_function("bencode_get", -1, bencode_get, deterministic=True)
        return db


def dropwhile(f, bytes):
    print("dropwhile", f, bytes)
    while f(bencode.parse_one_from_bytes(bytes)):
        bytes = discard(bytes)
    return bytes


def drop_until_end(bytes):
    while not bytes.startswith(b"e"):
        bytes = discard(bytes)
    return bytes[1:]


def discard(bytes, n=1):
    def discard_one(bytes):
        c = bytes[0]
        if c in map(ord, ["l", "d"]):
            return drop_until_end(bytes[1:])
        elif c == ord("i"):
            return bytes.partition(b"e")[2]
        else:
            len, _, bytes = bytes.partition(b":")
            return bytes[int(len) :]

    while n > 0:
        bytes = discard_one(bytes)
        n -= 1
    return bytes


def lookup(key, bytes):
    if isinstance(key, str):
        key = key.encode()
    while bytes[:1] != b"e":
        if key == bencode.parse_one_from_bytes(bytes):
            return discard(bytes)
        bytes = discard(bytes, 2)

def bencode_get_bytes(bytes, *path):
    if len(path) == 0:
        return bytes
    if isinstance(path[0], int):
        return discard(path[0], bytes[1:]
    key, *rest = path
    if isinstance(key, str):
        key = key.encode()
    return bencode_get_bytes(lookup(key, bytes[1:]))


def bencode_get(bytes, *path):
    if bytes is None:
        return
    bytes = bencode_get_bytes(bytes, *path)
    object = bencode.parse_one_from_bytes(bytes)
    if False and isinstance(object, (dict, list)):
        return None
    else:
        return object


class BencodeGet(TableFunction):
    columns = "key", "value"
    params = "bytes", "lookup"

    def initialize(self, bytes, lookup):
        self.bytes = bytes
        self.lookup = lookup
        self.parsed = bencode.parse_from_bytes(bytes)

    def iterate(self, idx):
        eval(self.lookup)


class ChunkBytes(TableFunction):
    params = ["bytes", "size"]
    columns = ["infohash"]
    name = "chunk_bytes"

    def initialize(self, bytes, size):
        self.bytes = bytes
        self.size = size

    def iterate(self, idx):
        if self.bytes is None or len(self.bytes) == 0:
            raise StopIteration
        bytes, self.bytes = self.bytes[: self.size], self.bytes[self.size :]
        return (bytes,)


@dataclass
class BaseRecordedSocket:
    socket: typing.Any
    db_conn: typing.Any


class Sender(BaseRecordedSocket):
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
                    raise


class Receiver(BaseRecordedSocket):
    async def recvfrom(self, amount) -> Tuple[bytes, Addr]:
        bytes, addr = await self.socket.recvfrom(amount)
        record_operation(self.db_conn, "recv", addr_for_db(addr), bytes, None)
        return bytes, addr


class RecordedSocket(Sender, Receiver):
    pass


def record_operation(
    db_conn, type: str, remote_addr: str, bytes: bytes, error: Union[str, None]
):
    with db_conn:
        db_conn.execute(
            "insert into operation (payload, remote_addr, type, error, when) values (?, ?, ?, ?, datetime('now'))",
            [bytes, remote_addr, type, error],
        )


def addr_for_db(addr: Tuple[str, int]) -> str:
    return addr[0] + ":" + str(addr[1])


if __name__ == "__main__":
    db = connect()
    if USE_APSW:
        shell = apsw.Shell(db=db)
        shell.process_command(".nullvalue NULL")
        shell.cmdloop()
    else:

        def execute(*args, **kwargs):
            for a in db.execute(*args, **kwargs):
                print(a)

        import code

        code.interact(local=locals())
