use arrow2::array::{Array, MutableArray, MutablePrimitiveArray, MutableUtf8Array};
use arrow2::datatypes::Field;
use arrow2::datatypes::PhysicalType;
use arrow2::ffi;
use arrow2::types::PrimitiveType;
use postgres_copy_binary_lib::{BinaryCopyOutIter, BinaryCopyOutRow};
use postgres_types::Type as PGType;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use std::str::FromStr;
use strum::VariantNames;
use strum_macros::{EnumString, EnumVariantNames};

/// Type is a supported subset of postgres_types::Type
#[derive(Debug, EnumString, EnumVariantNames, Copy, Clone)]
enum Type {
    // "bigint" => Type::INT8,
    // "bigserial" => Type::INT8,
    // "bit" => Type::BIT,
    // "bit varying" => Type::VARBIT,
    // "boolean" => Type::BOOL,
    // "box" => Type::BOX,
    // "bytea" => Type::BYTEA,
    #[strum(serialize = "character varying", serialize = "character")]
    VARCHAR,

    // "cidr" => Type::CIDR,
    // "circle" => Type::CIRCLE,
    // "date" => Type::DATE,
    #[strum(serialize = "double precision")]
    FLOAT8,

    // "inet" => Type::INET,
    #[strum(serialize = "integer")]
    INT4,

    // "interval" => Type::INTERVAL,
    // "json" => Type::JSON,
    // "jsonb" => Type::JSONB,
    // "line" => Type::LINE,
    // "lseg" => Type::LSEG,
    // "macaddr" => Type::MACADDR,
    // "macaddr8" => Type::MACADDR8,
    // "money" => Type::MONEY,
    // "numeric" => Type::NUMERIC,
    // "path" => Type::PATH,
    // "pg_lsn" => Type::PG_LSN,
    // "pg_snapshot" => Type::PG_SNAPSHOT,
    // "point" => Type::POINT,
    // "polygon" => Type::POLYGON,
    #[strum(serialize = "real")]
    FLOAT4,
    // "smallint" => Type::INT2,
    // "smallserial" => Type::INT2,
    // "serial" => Type::INT4,
    // "text" => Type::TEXT,
    // "time " => Type::TIME,
    // "timestamp " => Type::TIMESTAMP,
    // "tsquery" => Type::TSQUERY,
    // "tsvector" => Type::TS_VECTOR,
    // "txid_snapshot" => Type::TXID_SNAPSHOT,
    // "uuid" => Type::UUID,
    // "xml" => Type::XML,
}

impl Into<PGType> for &Type {
    fn into(self) -> PGType {
        match *self {
            Type::VARCHAR => PGType::VARCHAR,
            Type::FLOAT8 => PGType::FLOAT8,
            Type::INT4 => PGType::INT4,
            Type::FLOAT4 => PGType::FLOAT4,
        }
    }
}

fn new_array(ty: Type) -> Result<Box<dyn MutableArray>, String> {
    match ty {
        Type::VARCHAR => Ok(Box::new(MutableUtf8Array::<i32>::new())),
        Type::INT4 => Ok(Box::new(MutablePrimitiveArray::<i32>::new())),
        Type::FLOAT4 => Ok(Box::new(MutablePrimitiveArray::<f32>::new())),
        Type::FLOAT8 => Ok(Box::new(MutablePrimitiveArray::<f64>::new())),
    }
}

fn decode_buffer(buffer: &[u8], types: Vec<&str>) -> PyResult<Vec<Box<dyn Array>>> {
    let mut types_ = vec![];
    for name in types {
        let type_: Type = match Type::from_str(name) {
            Err(_) => {
                let msg = format!(
                    "unknown type {}, available types: {:?}",
                    name,
                    Type::VARIANTS
                );
                return Err(PyValueError::new_err(msg));
            }
            Ok(ty) => ty,
        };
        types_.push(type_);
    }

    let pg_types: Vec<_> = types_.iter().map(|t| t.into()).collect();
    let mut rows = BinaryCopyOutIter::new(buffer, pg_types.as_slice());
    let mut columns = types_
        .iter()
        .map(|t| new_array(*t))
        .collect::<Result<Vec<_>, String>>()
        .map_err(|err| PyValueError::new_err(err))?;

    loop {
        match rows.next() {
            Some(Ok(row)) => {
                push_row_values(&mut columns, &row)?;
            }
            None => {
                break;
            }
            Some(Err(e)) => return Err(PyValueError::new_err(e.to_string())),
        }
    }
    Ok(columns
        .into_iter()
        .map(|mut array| array.as_box())
        .collect())
}

#[pyfunction]
#[pyo3(name = "decode_buffer")]
fn py_decode_buffer(py: Python, buffer: &[u8], types: Vec<&str>) -> PyResult<Vec<PyObject>> {
    decode_buffer(buffer, types)?
        .into_iter()
        .map(|array| to_py_array(array, py))
        .collect()
}

fn push_row_values(
    columns: &mut Vec<Box<dyn MutableArray>>,
    row: &BinaryCopyOutRow,
) -> PyResult<()> {
    for (i, array) in columns.iter_mut().enumerate() {
        push_row_value(array, &row, i)?;
    }
    Ok(())
}

macro_rules! push_value {
    ($array:ident, $row:ident, $i:ident, $value_type:ty, $array_type:ty) => {{
        let v: Option<$value_type> = $row.get($i);
        $array
            .as_mut_any()
            .downcast_mut::<$array_type>()
            .unwrap()
            .push(v);
        Ok(())
    }};
}

fn push_row_value(
    array: &mut Box<dyn MutableArray>,
    row: &BinaryCopyOutRow,
    i: usize,
) -> PyResult<()> {
    match array.data_type().to_physical_type() {
        PhysicalType::Utf8 => push_value!(array, row, i, &str, MutableUtf8Array<i32>),
        PhysicalType::Primitive(PrimitiveType::Float64) => {
            push_value!(array, row, i, f64, MutablePrimitiveArray<f64>)
        }
        PhysicalType::Primitive(PrimitiveType::Int32) => {
            push_value!(array, row, i, i32, MutablePrimitiveArray<i32>)
        }
        PhysicalType::Primitive(PrimitiveType::Float32) => {
            push_value!(array, row, i, f32, MutablePrimitiveArray<f32>)
        }
        _ => Err(PyRuntimeError::new_err(format!(
            "array physical type is not handled: {:?}",
            array.data_type().to_physical_type()
        ))),
    }
}

fn to_py_array(array: Box<dyn Array>, py: Python) -> PyResult<PyObject> {
    let schema = Box::new(ffi::export_field_to_c(&Field::new(
        "",
        array.data_type().clone(),
        true,
    )));
    let array = Box::new(ffi::export_array_to_c(array));

    let schema_ptr: *const arrow2::ffi::ArrowSchema = &*schema;
    let array_ptr: *const arrow2::ffi::ArrowArray = &*array;

    let pa = py.import("pyarrow")?;

    let array = pa.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.to_object(py))
}

/// A Python module implemented in Rust.
#[pymodule]
fn postgres_copy_binary_extension_module(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_decode_buffer, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::decode_buffer;
    use arrow2::array::PrimitiveArray;

    #[test]
    fn test_row_i32() {
        let buf: &[u8] = b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x01\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02\x00\x01\x00\x00\x00\x04\x00\x00\x00\x03\xff\xff";
        let actual = decode_buffer(buf, vec!["integer"]).expect("no exception");
        assert_eq!(
            actual,
            vec![PrimitiveArray::from_vec(vec![1, 2, 3]).boxed()]
        )
    }
}
