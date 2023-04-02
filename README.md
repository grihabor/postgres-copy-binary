# postgres-copy-binary

Provides a decoder for postgres `COPY FROM WITH BINARY` format that spits out pyarrow / pandas / numpy arrays

## Examples

### Copy and decode the whole table

```python
import psycopg2
from postgres_copy_binary import decode_buffer

def load_as_arrow(cursor, table: str):

    # get column types, we need them to decode postgres binary format
    cursor.execute('''
        select data_type from information_schema.columns 
        where table_name = %s
        order by ordinal_position
    ''', (table,))
    types = [row[0] for row in cursor.fetchall()]
    
    # copy table into buffer
    buf = io.BytesIO()
    cursor.copy_expert(f'copy {table} to stdin WITH binary', buf)
    buffer = buf.getvalue()
    
    # decode buffer
    return decode_buffer(buffer, types)

with psycopg2.connect(...) as conn:
    with conn.cursor() as cursor:
        columns = load_as_arrow(cursor, 'my_table')
```

## Links

- https://www.postgresql.org/docs/current/sql-copy.html
- https://docs.rs/postgres/latest/postgres/binary_copy/index.html
- https://docs.rs/tokio-postgres/latest/tokio_postgres/binary_copy/index.html
- https://github.com/spitz-dan-l/postgres-binary-parser
- https://github.com/sfackler/rust-postgres-binary-copy 

