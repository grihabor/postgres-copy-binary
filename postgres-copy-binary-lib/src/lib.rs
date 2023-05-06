use bytes::BytesMut;
use core::result::Result;
use futures_util::future::LocalBoxFuture;
use futures_util::StreamExt;
use futures_util::{FutureExt, Stream};
use postgres_types::{FromSql, Type};
use simple_error::SimpleError;
use std::any::type_name;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tokio::io;
use tokio::io::{AsyncRead, AsyncReadExt};

const MAGIC: &[u8] = b"PGCOPY\n\xff\r\n\0";
#[derive(Copy, Clone)]
struct Header {
    has_oids: bool,
}

/// A type which deserializes rows from the PostgreSQL binary copy format.
pub struct BinaryCopyOutStream<'f, 'r, R>
where
    R: AsyncReadExt + Unpin + 'r,
    'r: 'f,
{
    _r: PhantomData<&'r ()>,
    reader: Rc<RefCell<R>>,
    types: Rc<Vec<Type>>,
    header: Rc<RefCell<Option<Header>>>,
    future: Option<LocalBoxFuture<'f, Option<Result<BinaryCopyOutRow, io::Error>>>>,
}

impl<'f, 'r, R> BinaryCopyOutStream<'f, 'r, R>
where
    R: AsyncReadExt + Unpin + 'r,
{
    /// Creates a stream from a raw copy out stream and the types of the columns being returned.
    pub fn new(reader: R, types: &[Type]) -> BinaryCopyOutStream<'f, 'r, R> {
        BinaryCopyOutStream {
            _r: PhantomData,
            reader: Rc::new(RefCell::new(reader)),
            types: Rc::new(types.to_vec()),
            header: Rc::new(RefCell::new(None)),
            future: None,
        }
    }
}

impl<'f, 'r, R> Stream for BinaryCopyOutStream<'f, 'r, R>
where
    R: AsyncRead + Unpin + 'r,
{
    type Item = io::Result<BinaryCopyOutRow>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self;

        if this.future.is_none() {
            let future =
                poll_next_row(this.reader.clone(), this.types.clone(), this.header.clone())
                    .boxed_local();
            this.future = Some(future);
        }

        match Pin::new(&mut this.future.borrow_mut().as_mut().unwrap()).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(option) => {
                this.future.take();
                Poll::Ready(option)
            }
        }
    }
}

macro_rules! try_ {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        }
    };
}

async fn poll_next_row<R>(
    reader: Rc<RefCell<R>>,
    types: Rc<Vec<Type>>,
    header: Rc<RefCell<Option<Header>>>,
) -> Option<io::Result<BinaryCopyOutRow>>
where
    R: AsyncRead + Unpin,
{
    let mut reader = reader.as_ref().borrow_mut();

    let has_oids = if header.borrow().is_none() {
        let mut magic: &mut [u8] = &mut [0; MAGIC.len()];
        try_!(reader.read_exact(&mut magic).await);
        if magic != MAGIC {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic value",
            )));
        }

        let flags = try_!(reader.read_u32().await);
        let has_oids = (flags & (1 << 16)) != 0;
        let header_extension_size = try_!(reader.read_u32().await);

        // skip header extension
        let mut header_extension: Box<[u8]> =
            vec![0; header_extension_size as usize].into_boxed_slice();
        try_!(reader.read_exact(&mut header_extension).await);

        header.replace(Some(Header { has_oids }));
        has_oids
    } else {
        header.borrow().unwrap().has_oids
    };

    let mut field_count = try_!(reader.read_u16().await);
    if field_count == u16::MAX {
        // end of binary stream
        return None;
    }

    if has_oids {
        field_count += 1;
    }
    if field_count as usize != types.len() {
        return Some(Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected {} values but got {}", types.len(), field_count),
        )));
    }

    let mut field_buf = BytesMut::new();
    let mut field_indices = vec![];
    for _ in 0..field_count {
        let field_size = try_!(reader.read_u32().await);

        let start = field_buf.len();
        if field_size == u32::MAX {
            field_indices.push(FieldIndex::Null(start));
            continue;
        }
        let field_size = field_size as usize;
        field_buf.resize(start + field_size, 0);
        try_!(
            reader
                .read_exact(&mut field_buf[start..start + field_size])
                .await
        );
        field_indices.push(FieldIndex::Value(start));
    }

    Some(Ok(BinaryCopyOutRow {
        fields: Fields {
            buf: field_buf,
            indices: field_indices,
        },
        types: types.clone(),
    }))
}

#[derive(Debug)]
enum FieldIndex {
    Value(usize),
    Null(usize),
}

impl FieldIndex {
    fn index(&self) -> usize {
        match self {
            FieldIndex::Value(index) => *index,
            FieldIndex::Null(index) => *index,
        }
    }
}

#[derive(Debug)]
struct Fields {
    buf: BytesMut,
    indices: Vec<FieldIndex>,
}

impl Fields {
    fn field(&self, idx: usize) -> &[u8] {
        if idx + 1 < self.indices.len() {
            &self.buf[self.indices[idx].index()..self.indices[idx + 1].index()]
        } else {
            &self.buf[self.indices[idx].index()..]
        }
    }
}

/// A row of data parsed from a binary copy out stream.
#[derive(Debug)]
pub struct BinaryCopyOutRow {
    fields: Fields,
    types: Rc<Vec<Type>>,
}

impl BinaryCopyOutRow {
    /// Like `get`, but returns a `Result` rather than panicking.
    pub fn try_get<'a, T>(&'a self, idx: usize) -> Result<T, Box<dyn Error + Send + Sync>>
    where
        T: FromSql<'a>,
    {
        let type_ = match self.types.get(idx) {
            Some(type_) => type_,
            None => {
                return Err(Box::new(SimpleError::new(format!(
                    "row has only {} items, tried to get index {}",
                    self.types.len(),
                    idx
                ))))
            }
        };

        if !T::accepts(type_) {
            return Err(Box::new(SimpleError::new(format!(
                "can't convert {} into {}",
                type_,
                type_name::<T>()
            ))));
        }

        match &self.fields.indices[idx] {
            FieldIndex::Value(_) => T::from_sql(type_, self.fields.field(idx)),
            FieldIndex::Null(_) => T::from_sql_null(type_),
        }
    }

    /// Deserializes a value from the row.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<'a, T>(&'a self, idx: usize) -> T
    where
        T: FromSql<'a>,
    {
        match self.try_get(idx) {
            Ok(value) => value,
            Err(e) => panic!("error retrieving column {}: {}", idx, e),
        }
    }
}

/// An iterator of rows deserialized from the PostgreSQL binary copy format.
pub struct BinaryCopyOutIter<'f, 'r, R>
where
    R: AsyncRead + Unpin,
{
    stream: Pin<Box<BinaryCopyOutStream<'f, 'r, R>>>,
}

impl<'f, 't, 'r, R> BinaryCopyOutIter<'f, 'r, R>
where
    R: AsyncRead + Unpin + 'r,
{
    /// Creates a new iterator from a raw copy out reader and the types of the columns being returned.
    pub fn new(reader: R, types: &'t [Type]) -> BinaryCopyOutIter<'f, 'r, R>
    where
        't: 'f,
        't: 'r,
    {
        BinaryCopyOutIter {
            stream: Box::pin(BinaryCopyOutStream::new(reader, types)),
        }
    }
}

impl<'f, 'r, R> Iterator for BinaryCopyOutIter<'f, 'r, R>
where
    R: AsyncRead + Unpin + 'r,
{
    type Item = io::Result<BinaryCopyOutRow>;

    fn next(&mut self) -> Option<io::Result<BinaryCopyOutRow>> {
        let stream = &mut self.stream;
        futures::executor::block_on(async { stream.next().await })
    }
}
