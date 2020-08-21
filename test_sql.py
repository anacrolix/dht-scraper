from sql import *


def test_chunk_bytes():
    a = connect()
    a.execute("select quote(infohash) from operation, chunk_bytes(payload, 20)")
