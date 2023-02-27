use std::pin::Pin;
use bytes::Bytes;
use futures::stream::Stream;
use postgres::fallible_iterator::FallibleIterator;
use postgres_types::Type;
use postgres::{CopyOutReader, Error};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use postgres::binary_copy::BinaryCopyOutIter;
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use tokio_postgres::CopyOutStream;


#[pyfunction]
fn decode(buffer: &[u8]) -> PyResult<Vec<i32>> {
    let b = buffer.to_owned();
    let input_stream = stream::once(async { Ok(Bytes::from(b)) });
    let mut it = BinaryCopyOutIter::from_stream(input_stream, &[Type::INT4]);
    let mut v = vec![];

    loop {
        match it.next() {
            Ok(row) => {
                let int: i32 = row.expect("row parsed").get(0);
                v.push(int);
            }
            Err(e) => {
                return if e.is_closed() {
                    Ok(v)
                } else {
                    Err(PyValueError::new_err(e.to_string()))
                }
            }
        }
    }
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn postgres_binary_format(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    Ok(())
}
