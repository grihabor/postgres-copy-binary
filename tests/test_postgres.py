from typing import final
import pytest
import pyarrow as pa
from postgres_copy_binary import read_table
from io import BytesIO, StringIO
import psycopg2


@pytest.fixture
def connection(pg_server):
    pg_params = pg_server["params"]
    with psycopg2.connect(**pg_params) as conn:
        yield conn


@pytest.fixture
def postgres_cursor(connection):
    with connection.cursor() as cursor:
        yield cursor


@pytest.fixture(autouse=True)
def create_table(postgres_cursor, column_type, values):
    cursor = postgres_cursor
    cursor.execute(f"create table test(col1 {column_type})")
    try:
        cursor.executemany("insert into test(col1) values (%s)", values)
        yield
    finally:
        cursor.execute("drop table test")


@pytest.mark.parametrize(
    "column_type, arrow_type, values",
    [
        ("integer", pa.int32(), [[2], [3], [5], [8], [13]]),
        ("bigint", pa.int64(), [[2], [3], [5], [8], [13]]),
        ("smallint", pa.int16(), [[2], [3], [5], [8], [13]]),
    ],
)
def test_int(postgres_cursor, arrow_type, column_type, values):
    cursor = postgres_cursor

    f = BytesIO()
    cursor.copy_expert("copy test to stdout (format binary)", f)
    data = read_table(cursor, "test")
    schema = pa.schema(
        [
            pa.field("col1", arrow_type),
        ]
    )
    assert data == pa.Table.from_pydict(
        {"col1": [row[0] for row in values]}, schema=schema
    )
