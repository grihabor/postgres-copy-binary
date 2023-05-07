import io

import pyarrow
from postgres_copy_binary_extension_module import decode_buffer


def read_table(cursor, table: str) -> list[pyarrow.Array]:

    # get column types, we need them to decode postgres binary format
    cursor.execute(
        """
        select column_name, data_type from information_schema.columns
        where table_name = %s
        order by ordinal_position
    """,
        (table,),
    )
    names, types = zip(*list(cursor.fetchall()))

    # copy table into buffer
    buf = io.BytesIO()
    cursor.copy_expert(f"copy {table} to stdin WITH binary", buf)
    buffer = buf.getvalue()

    # decode buffer
    columns = decode_buffer(buffer, types)
    return pyarrow.Table.from_arrays(columns, names=names)


__all__ = (
    "read_pyarrow",
    "decode_buffer",
)
