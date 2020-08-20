from sql import *


def test_chunk_bytes():
    a = connect()
    a.execute("select quote(infohash) from messages, chunk_bytes(value, 20)")
