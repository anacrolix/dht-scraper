from sql import *


def test_chunk_bytes():
    a = connect(":memory:")
    a.execute("create table herp(derp)")
    a.execute("insert into herp values (x'deadbeef'), (x'c0ffee')")
    b = list(a.execute("select herp.rowid, chunk from herp, chunk(derp, 2)"))
    assert b == [(1, b"\xde\xad"), (1, b"\xbe\xef"), (2, b"\xc0\xff"), (2, b"\xee")]
