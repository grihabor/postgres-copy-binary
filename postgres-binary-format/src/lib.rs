use bytes::Bytes;
use futures::stream::Stream;
use postgres::binary_copy::BinaryCopyOutIter;
use postgres::fallible_iterator::FallibleIterator;
use postgres::{CopyOutReader, Error};
use postgres_types::Type;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::pin::Pin;
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use tokio_postgres::CopyOutStream;



#[pyfunction]
fn decode_all_rows(buffer: &[u8]) -> PyResult<Vec<i32>> {
    let mut it = BinaryCopyOutIter::new(buffer, &[Type::INT4]);
    let mut v = vec![];

    loop {
        match it.next() {
            Ok(Some(row)) => {
                println!("{:?}", row);
                let int: i32 = row.get(0);
                v.push(int);
            }
            Ok(None) => return Ok(v),
            Err(e) => return Err(PyValueError::new_err(e.to_string())),
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn postgres_binary_format(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode_all_rows, m)?)?;
    Ok(())
}

#[test]
fn test_row() {
    let buf: &[u8] = b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x01\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02\x00\x01\x00\x00\x00\x04\x00\x00\x00\x03\xff\xff";
    let actual = decode(buf);
    assert_eq!(actual.unwrap(), vec![1,2,3])
}