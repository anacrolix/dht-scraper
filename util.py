def chunk_bytes(bytes, size, strict=False):
    if strict and len(bytes) % size:
        raise ValueError(f"len(input) ({len(bytes)}) is not multiple of {size}")
    while bytes:
        yield bytes[:size]
        bytes = bytes[size:]
