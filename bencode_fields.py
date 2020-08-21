def new_message_id(db_conn: sqlite3.Connection) -> int:
    cursor = db_conn.cursor()
    cursor.execute("insert into messages default values")
    return cursor.lastrowid


@dataclass
class FieldContext:
    parent_id: typing.Union[None, int]
    index: int = 0


class MessageWriter:

    cursor: sqlite3.Cursor

    def __init__(self, cursor, top_id):
        self.cursor = cursor
        self.field_contexts = [FieldContext(top_id)]

    def _cur_parent_id(self) -> typing.Union[int, None]:
        return self.field_contexts[-1].parent_id

    def _cur_field_context(self) -> FieldContext:
        return self.field_contexts[-1]

    def _insert_code(self, code):
        self._insert(code, None)

    def _insert(self, code, value):
        parent_id = self._cur_parent_id()
        self.cursor.execute(
            """insert into messages (parent_id, "index", depth, type, value) values (?, ?, ?, ?, ?)""",
            [
                parent_id,
                self._cur_field_context().index,
                self._cur_depth(),
                code,
                value,
            ],
        )
        self._cur_field_context().index += 1

    def _cur_depth(self):
        return len(self.field_contexts) - 1

    def _start(self, code):
        self._insert_code(code)
        self.field_contexts.append(FieldContext(self.cursor.lastrowid))

    def start_dict(self):
        self._start("d")

    def start_list(self):
        self._start("l")

    def end(self):
        self._insert_code("e")
        self.field_contexts.pop()

    def int(self, value):
        self._insert("i", value)

    def str(self, value):
        self._insert("s", value)


def record_packet(bytes, db_conn, top_id):
    bencode.StreamDecoder(bencode.BytesStreamReader(bytes)).visit(
        MessageWriter(db_conn.cursor(), top_id)
    )
