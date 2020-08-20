# def drop_tables(db):
# 	db.execute('drop table messages')
# 	db.execute('drop table operations')

# def create_tables(db):
# 	db.execute('create

from vtfunc import TableFunction
import sqlite3


def connect():
    db = sqlite3.connect("herp.db")
    ChunkBytes.register(db)
    return db


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
