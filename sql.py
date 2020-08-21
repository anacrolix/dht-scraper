# def drop_tables(db):
# 	db.execute('drop table messages')
# 	db.execute('drop table operations')

# def create_tables(db):
# 	db.execute('create

from vtfunc import TableFunction
import apsw
from dataclasses import dataclass
import typing, sys
from typing import *
import bencode
from my_types import *
import sqlite3


class ShellWrapper:

    def __init__(self, wrappee):
        self.__wrappee = wrappee

    @property
    def filename(self):
        return 'herp.db'

    def __getattr__(self, name):
        return getattr(self.__wrappee, name)

def connect():
    db = sqlite3.connect("herp.db")
    return ShellWrapper(db)
    # db = apsw.Connection('herp.db')
    # ChunkBytes.register(db)
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


def discard(bytes):
    c = bytes[0]
    if c in map(ord, [b"l", b"d"]):
        return drop_until_end(bytes[1:])
    elif c == ord("i"):
        return bytes.partition(b"e")[2]
    else:
        len, _, bytes = bytes.partition(b":")
        return bytes[int(len) :]


def lookup(key, bytes):
    while not bytes.startswith(b"e"):
        if key == bencode.parse_one_from_bytes(bytes):
            return discard(bytes)
        bytes = discard(discard(bytes))


def bencode_get(bytes, *path):
    if bytes is None:
        return
    if len(path) == 0:
        object = bencode.parse_one_from_bytes(bytes)
        if isinstance(object, (dict, list)):
            return None
    if isinstance(path[0], int):
        return bencode_get(drop(path[0], bytes[1:]), path[1:])
    key, *rest = path
    if isinstance(key, str):
        key = key.encode()
    return bencode_get(lookup(key, bytes[1:]))


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
    shell = apsw.Shell(db=db)
    shell.process_command(".nullvalue NULL")
    shell.cmdloop()
