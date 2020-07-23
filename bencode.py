
def bencode(value):
	return {
		dict: bencode_dict,
		list: bencode_list,
		int: bencode_int,
		str: bencode_str,
	}[type(value)](value)

def bencode_dict(value):
	yield 'd'
	for key, value in value.items():
		yield from bencode(key)
		yield from bencode(value)
	yield 'e'

def bencode_list(value):
	yield 'l'
	for item in list:
		yield bencode(item)
	yield 'e'

def bencode_int(int):
	yield 'i'
	yield str(int)
	yield 'e'

def bencode_str(_str):
	yield str(len(_str))
	yield ':'
	yield _str
