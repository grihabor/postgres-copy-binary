use arrow2::array::{Array, MutableArray, MutablePrimitiveArray, PrimitiveArray};
use arrow2::datatypes::Field;
use arrow2::datatypes::PhysicalType;
use arrow2::ffi;
use arrow2::types::PrimitiveType;
use phf::phf_map;
use postgres_copy_binary_rs::{BinaryCopyOutIter, BinaryCopyOutRow};
use postgres_types::Type;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;

static TYPE_MAPPING: phf::Map<&'static str, Type> = phf_map! {
    // "bigint" => Type::INT8,
    // "bigserial" => Type::INT8,
    // "bit" => Type::BIT,
    // "bit varying" => Type::VARBIT,
    // "boolean" => Type::BOOL,
    // "box" => Type::BOX,
    // "bytea" => Type::BYTEA,
    // "character" => Type::VARCHAR,
    // "character varying" => Type::VARCHAR,
    // "cidr" => Type::CIDR,
    // "circle" => Type::CIRCLE,
    // "date" => Type::DATE,
    // "double precision" => Type::FLOAT8,
    // "inet" => Type::INET,
    "integer" => Type::INT4,
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
    "real" => Type::FLOAT4,
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
};

fn new_array(ty: &Type) -> Result<Box<dyn MutableArray>, String> {
    match *ty {
        Type::INT4 => Ok(Box::new(MutablePrimitiveArray::<i32>::new())),
        Type::FLOAT4 => Ok(Box::new(MutablePrimitiveArray::<f32>::new())),
        _ => Err(format!("array for type {} is not implemented yet", ty)),
    }
}

fn decode_buffer(buffer: &[u8], types: Vec<&str>) -> PyResult<Vec<Box<dyn Array>>> {
    let mut types_ = vec![];
    for name in types {
        let type_ = match TYPE_MAPPING.get(name) {
            None => {
                let keys: String = TYPE_MAPPING
                    .keys()
                    .cloned()
                    .collect::<Vec<&str>>()
                    .join(",");
                let msg = format!("unknown type {}, available types: {}", name, keys,);
                return Err(PyValueError::new_err(msg));
            }
            Some(ty) => ty.clone(),
        };
        types_.push(type_);
    }

    let mut rows = BinaryCopyOutIter::new(buffer, types_.as_slice());
    let mut columns = types_
        .iter()
        .map(|t| new_array(&t))
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

fn push_row_value(
    array: &mut Box<dyn MutableArray>,
    row: &BinaryCopyOutRow,
    i: usize,
) -> PyResult<()> {
    match array.data_type().to_physical_type() {
        PhysicalType::Primitive(PrimitiveType::Int32) => {
            let v: Option<i32> = row.get(i);
            array
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<i32>>()
                .unwrap()
                .push(v);
            Ok(())
        }
        PhysicalType::Primitive(PrimitiveType::Float32) => {
            let v: Option<f32> = row.get(i);
            array
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<f32>>()
                .unwrap()
                .push(v);
            Ok(())
        }
        _ => Err(PyRuntimeError::new_err(
            "array physical type is not handled",
        )),
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
fn postgres_copy_binary(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_decode_buffer, m)?)?;
    Ok(())
}

#[test]
fn test_row_i32() {
    let buf: &[u8] = b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x01\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02\x00\x01\x00\x00\x00\x04\x00\x00\x00\x03\xff\xff";
    let actual = decode_buffer(buf, vec!["integer"]).expect("no exception");
    assert_eq!(
        actual,
        vec![PrimitiveArray::from_vec(vec![1, 2, 3]).boxed()]
    )
}
