from bencode import *


def example_ping_query():
    return {
        b"a": {b"id": b"abcdefghij0123456789"},
        b"q": b"ping",
        b"t": b"aa",
        b"y": b"q",
    }


encoded_example_ping_query = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"


def test_bencode_ping():
    assert b"".join(encode(example_ping_query())) == encoded_example_ping_query


def test_tokenize_ping():
    pass


def test_parse_bytes_one():
    assert parse_one_from_bytes(b"de") == {}
    assert parse_one_from_bytes(encoded_example_ping_query) == example_ping_query()
