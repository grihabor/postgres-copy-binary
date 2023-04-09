import io

import pyarrow
from postgres_copy_binary_extension_module import decode_buffer


def read_table(cursor, table: str) -> list[pyarrow.Array]:

    # get column types, we need them to decode postgres binary format
    cursor.execute(
        """
        select data_type from information_schema.columns
        where table_name = %s
        order by ordinal_position
    """,
        (table,),
    )
    types = [row[0] for row in cursor.fetchall()]

    # copy table into buffer
    buf = io.BytesIO()
    cursor.copy_expert(f"copy {table} to stdin WITH binary", buf)
    buffer = buf.getvalue()

    # decode buffer
    return decode_buffer(buffer, types)


__all__ = (
    "read_pyarrow",
    "decode_buffer",
)
