use anyhow::anyhow;
use arrow2::array::{Array, MutableArray, MutablePrimitiveArray};
use postgres::binary_copy::BinaryCopyOutIter;
use postgres::fallible_iterator::FallibleIterator;
use postgres_types::Type;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use arrow2::datatypes::PhysicalType;
// use arrow2::ffi::export_array_to_c;
use arrow2::types::PrimitiveType;
use phf::phf_map;

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

fn new_array(ty: &Type) -> anyhow::Result<Box<dyn Array>> {
    match *ty {
        Type::INT4 => Ok(MutablePrimitiveArray::<i32>::new().as_box()),
        Type::FLOAT4 => Ok(MutablePrimitiveArray::<f32>::new().as_box()),
        _ => Err(anyhow!("array for type {} is not implemented yet", ty)),
    }
}

#[pyfunction]
fn decode_all_rows(buffer: &[u8], types: Vec<&str>) -> PyResult<Vec<i32>> {
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
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(|err| PyValueError::new_err(err.to_string()))?;

    loop {
        match rows.next() {
            Ok(Some(row)) => {
                for (i, column) in columns.iter_mut().enumerate() {
                    match column.data_type().to_physical_type() {
                        PhysicalType::Primitive(PrimitiveType::Int32) => {
                            let v: Option<i32> = row.get(i);
                            println!("try downcast i32 {:?}", column);
                            column.as_any_mut().downcast_mut::<MutablePrimitiveArray<i32>>().unwrap().push(v);
                        }
                        PhysicalType::Primitive(PrimitiveType::Float32) => {
                            let v: Option<f32> = row.get(i);
                            println!("try downcast f32 {:?}", column);
                            column.as_any_mut().downcast_mut::<MutablePrimitiveArray<f32>>().unwrap().push(v);
                        }
                        _ => return Err(PyRuntimeError::new_err("array physical type is not handled"))
                    }
                }
            }
            Ok(None) => {
                break;
            }
            Err(e) => return Err(PyValueError::new_err(e.to_string())),
        }
    }
    for (i, column) in columns.into_iter().enumerate() {
        println!("column {}: {:?}", i, column);
    }
    // let _ = export_array_to_c(first_column);
    // Err(PyNotImplementedError::new_err(""))
    Ok(vec![])
}

/// A Python module implemented in Rust.
#[pymodule]
fn postgres_binary_format(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode_all_rows, m)?)?;
    Ok(())
}

#[test]
fn test_row_i32() {
    let buf: &[u8] = b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x01\x00\x01\x00\x00\x00\x04\x00\x00\x00\x02\x00\x01\x00\x00\x00\x04\x00\x00\x00\x03\xff\xff";
    let actual = decode_all_rows(buf).expect("no exception");
    assert_eq!(actual, vec![1, 2, 3])
}
