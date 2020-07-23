from bencode import bencode

def test_bencode_ping():
	assert ''.join(bencode({"a":{"id":"abcdefghij0123456789"},"q":"ping","t":"aa", "y":"q",  })) == 'd1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe'
