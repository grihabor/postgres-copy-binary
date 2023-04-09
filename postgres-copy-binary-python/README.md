# postgres-copy-binary

Provides a decoder for postgres `COPY FROM WITH BINARY` format that spits out pyarrow / pandas / numpy arrays

## Examples

### Copy and decode the whole table

```python
import psycopg2
from postgres_copy_binary import read_table

with psycopg2.connect(...) as conn:
    with conn.cursor() as cursor:
        columns = read_table(cursor, 'my_table')
```

## Links

- https://www.postgresql.org/docs/current/sql-copy.html
- https://docs.rs/postgres/latest/postgres/binary_copy/index.html
- https://docs.rs/tokio-postgres/latest/tokio_postgres/binary_copy/index.html
- https://github.com/spitz-dan-l/postgres-binary-parser
- https://github.com/sfackler/rust-postgres-binary-copy 

