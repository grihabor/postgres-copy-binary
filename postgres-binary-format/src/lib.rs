use bytes::Bytes;
use futures::stream::{self};
use postgres::binary_copy::BinaryCopyOutIter;
use postgres::fallible_iterator::FallibleIterator;
use postgres_types::Type;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
fn decode(buffer: &[u8]) -> PyResult<Vec<i32>> {
    let b = buffer.to_owned();
    let input_stream = stream::once(async { Ok(Bytes::from(b)) });
    let mut it = BinaryCopyOutIter::new(input_stream, &[Type::INT4]);
    let mut v = vec![];
    while let Some(row) = it
        .next()
        .map_err(|e| PyValueError::new_err(e.to_string()))?
    {
        let int: i32 = row.get(0);
        v.push(int);
    }
    Ok(v)
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
