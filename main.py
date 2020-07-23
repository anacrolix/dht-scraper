global_bootstrap_nodes = [
    "router.utorrent.com:6881",
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.aelitis.com:6881",  # Vuze
    "router.silotis.us:6881",  # IPv6
    "dht.libtorrent.org:25401",  # @arvidn's
]


def bencode(value):
    {dict: bencode_dict, list: bencode_list, int: bencode_int,}.get(type(value))(value)


def test_bencode_ping():
    assert (
        bencode({"t": "aa", "y": "q", "q": "ping", "a": {"id": "abcdefghij0123456789"}})
        == "d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"
    )
