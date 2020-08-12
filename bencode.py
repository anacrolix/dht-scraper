from typing import Union, Any, Protocol, Optional, Iterable
from dataclasses import dataclass
import itertools
import typing


Dict = typing.Dict[bytes, Any]
Object = Union[Dict, int, bytes, list]


def encode_to_bytes(value: Any) -> bytes:
    return b"".join(encode(value))


def encode(value):
    return {
        dict: encode_dict,
        list: encode_list,
        int: encode_int,
        str: encode_str,
        bytes: encode_bytes,
    }[type(value)](value)


def encode_dict(value):
    yield b"d"
    for key, value in value.items():
        yield from encode(key)
        yield from encode(value)
    yield b"e"


def encode_list(value):
    yield b"l"
    for item in list:
        yield from encode(item)
    yield b"e"


def encode_int(int):
    yield b"i"
    yield str(int)
    yield b"e"


def encode_bytes(bytes):
    yield str(len(bytes)).encode()
    yield b":"
    yield bytes


def encode_str(str):
    yield from encode_bytes(str.encode())


class Visitor(typing.Protocol):
    def start_dict(self):
        ...

    def start_list(self):
        ...

    def end(self):
        ...

    def int(self, int):
        ...

    def str(self, bytes):
        ...


TokenCode = int


@dataclass
class Special:
    code: TokenCode


StartDict = Special(ord("d"))
StartList = Special(ord("l"))
End = Special(ord("e"))


class StreamReader(Protocol):
    def peek_one(self) -> Optional[int]:
        ...

    def advance_one(self):
        ...

    def read_exactly(self, amount: int) -> bytes:
        ...

    def read_until(self, until: bytes) -> bytes:
        ...


@dataclass
class BytesStreamReader:
    bytes: bytes

    def peek_one(self) -> Optional[int]:
        if len(self.bytes) == 0:
            return None
        return self.bytes[0]

    def read_until(self, until: bytes) -> bytes:
        index = self.bytes.index(until)
        ret = self.bytes[:index]
        self.bytes = self.bytes[index + len(until) :]
        return ret

    def read_exactly(self, amount):
        ret = self.bytes[:amount]
        self.bytes = self.bytes[amount:]
        return ret

    def advance_one(self):
        self.bytes = self.bytes[1:]


@dataclass
class StreamDecoder:
    stream: StreamReader

    def tokenize(self) -> Iterable[Union[Special, int, bytes]]:
        while (c := self.stream.peek_one()) is not None:
            if c in map(ord, {"d", "l", "e"}):
                self.stream.advance_one()
                yield Special(c)
            elif c == ord("i"):
                self.stream.advance_one()
                s = self.stream.read_until(b"e")
                yield int(s)
            else:
                l = int(self.stream.read_until(b":"))
                yield self.stream.read_exactly(l)

    def visit(self, visitor: Visitor):
        from functools import partial

        for token in self.tokenize():
            if isinstance(token, Special):
                dict(
                    map(
                        lambda x: (ord(x[0]), *x[1:]),
                        {
                            "d": visitor.start_dict,
                            "l": visitor.start_list,
                            "e": visitor.end,
                        }.items(),
                    )
                )[token.code]()
            elif isinstance(token, int):
                visitor.int(token)
            elif isinstance(token, bytes):
                visitor.str(token)
            else:
                raise TypeError(token)


def parse_until_end(tokens) -> Iterable[Object]:
    try:
        while (t := next(tokens)) != End:
            yield parse_one(itertools.chain([t], tokens))
    except StopIteration:
        raise ValueError("end not seen")


def parse_one(tokens) -> Object:
    t = next(tokens)
    if t == StartDict:
        it = parse_until_end(tokens)
        return dict(itertools.zip_longest(it, it))
    elif t == StartList:
        return list(parse_until_end(tokens))
    else:
        return t


def parse(tokens):
    while True:
        try:
            yield parse_one(tokens)
        except StopIteration:
            return


def parse_bytes(bytes):
    return list(parse(StreamDecoder(BytesStreamReader(bytes)).tokenize()))


def parse_one_from_bytes(bytes) -> Object:
    return parse_bytes(bytes)[0]
