//! bb8-diesel allows the bb8 asynchronous connection pool
//! to be used underneath Diesel.
//!
//! This is currently implemented against Diesel's synchronous
//! API, with calls to [`tokio::task::spawn_blocking`] to safely
//! perform synchronous operations from an asynchronous task.

#[allow(unused_imports)]
#[macro_use]
extern crate diesel;

use async_trait::async_trait;
use diesel::{
    connection::{Connection, SimpleConnection},
    dsl::Limit,
    query_dsl::{
        methods::{ExecuteDsl, LimitDsl, LoadQuery},
        RunQueryDsl,
    },
    r2d2::{self, ManageConnection, R2D2Connection},
    QueryResult,
};
use std::sync::{Arc, Mutex};
use tokio::task;

/// A connection manager which implements [`bb8::ManageConnection`] to
/// integrate with bb8.
///
/// ```no_run
/// #[macro_use]
/// extern crate diesel;
///
/// use bb8_diesel::AsyncRunQueryDsl;
/// use diesel::prelude::*;
/// use diesel::pg::PgConnection;
///
/// table! {
///     users (id) {
///         id -> Integer,
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     use users::dsl;
///
///     // Creates a Diesel-specific connection manager for bb8.
///     let mgr = bb8_diesel::DieselConnectionManager::<PgConnection>::new("localhost:1234");
///     let pool = bb8::Pool::builder().build(mgr).await.unwrap();
///
///     diesel::insert_into(dsl::users)
///         .values(dsl::id.eq(1337))
///         .execute_async(&pool)
///         .await
///         .unwrap();
/// }
/// ```
#[derive(Clone)]
pub struct DieselConnectionManager<T> {
    inner: Arc<Mutex<r2d2::ConnectionManager<T>>>,
}

impl<T: Send + 'static> DieselConnectionManager<T> {
    pub fn new<S: Into<String>>(database_url: S) -> Self {
        Self {
            inner: Arc::new(Mutex::new(r2d2::ConnectionManager::new(database_url))),
        }
    }

    async fn run_blocking<R, F>(&self, f: F) -> R
    where
        R: Send + 'static,
        F: Send + 'static + FnOnce(&r2d2::ConnectionManager<T>) -> R,
    {
        let cloned = self.inner.clone();
        tokio::task::spawn_blocking(move || f(&*cloned.lock().unwrap()))
            .await
            // Intentionally panic if the inner closure panics.
            .unwrap()
    }

    // TODO: I'd really like to remove this function.
    async fn run_blocking_in_place<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&r2d2::ConnectionManager<T>) -> R,
    {
        task::block_in_place(|| f(&*self.inner.lock().unwrap()))
    }
}

#[async_trait]
impl<T> bb8::ManageConnection for DieselConnectionManager<T>
where
    T: R2D2Connection + Send + 'static,
{
    type Connection = DieselConnection<T>;
    type Error = diesel::r2d2::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.run_blocking(|m| m.connect())
            .await
            .map(|c| DieselConnection::new(c))
    }

    async fn is_valid(
        &self,
        conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        self.run_blocking_in_place(|m| {
            m.is_valid(&mut *conn.inner())?;
            Ok(())
        })
        .await
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        // Diesel returns this value internally. We have no way of calling the
        // inner method without blocking as this method is not async, but `bb8`
        // indicates that this method is not mandatory.
        false
    }
}

/// An async-safe analogue of any connection that implements
/// [`diesel::Connection`].
///
/// These connections are created by [`DieselConnectionManager`].
///
/// All blocking methods within this type delegate to
/// [`tokio::task::block_in_place`]. The number of threads is not unbounded,
/// however, as they are controlled by the truly asynchronous [`bb8::Pool`]
/// owner.  This type makes it easy to use diesel without fear of blocking the
/// runtime and without fear of spawning too many child threads.
///
/// Note that trying to construct this type via
/// [`diesel::connection::Connection::establish`] will return an error.
///
/// The only correct way to construct this type is by using a bb8 pool.
pub struct DieselConnection<C>(pub(crate) Arc<Mutex<C>>);

impl<C> DieselConnection<C> {
    pub fn new(c: C) -> Self {
        Self(Arc::new(Mutex::new(c)))
    }

    fn inner(&self) -> std::sync::MutexGuard<'_, C> {
        self.0.lock().unwrap()
    }
}

/// Syntactic sugar around a Result returning an [`AsyncError`].
pub type AsyncResult<R> = Result<R, AsyncError>;

/// Describes an error from sending a request to Diesel.
// #[derive(Debug)]
// pub enum AsyncError {
//     /// Failed to checkout a connection.
//     // TODO Populate
//     Checkout,
//
//     /// Query failure.
//     Error(diesel::result::Error),
// }

pub type AsyncError = diesel::result::Error;

/// An async variant of [`diesel::connection::SimpleConnection`].
#[async_trait]
pub trait AsyncSimpleConnection<Conn>
where
    Conn: 'static + SimpleConnection,
{
    async fn batch_execute_async(&self, query: &str) -> AsyncResult<()>;
}

#[async_trait]
impl<Conn> AsyncSimpleConnection<Conn> for bb8::Pool<DieselConnectionManager<Conn>>
where
    Conn: 'static + R2D2Connection,
{
    #[inline]
    async fn batch_execute_async(&self, query: &str) -> AsyncResult<()> {
        let self_ = self.clone();
        let query = query.to_string();
        let conn = self_.get_owned().await.unwrap(); //map_err(|_| AsyncError::Checkout)?;
        task::spawn_blocking(move || {
            conn.inner().batch_execute(&query)
        })
        .await
        .unwrap() // Propagate panics
    }
}

/// An async variant of [`diesel::connection::Connection`].
#[async_trait]
pub trait AsyncConnection<Conn>: AsyncSimpleConnection<Conn>
where
    Conn: 'static + Connection,
{
    async fn run<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static;

    async fn transaction<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static;
}

#[async_trait]
impl<Conn> AsyncConnection<Conn> for bb8::Pool<DieselConnectionManager<Conn>>
where
    Conn: 'static + R2D2Connection,
{
    #[inline]
    async fn run<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static,
    {
        let self_ = self.clone();
        let conn = self_.get_owned().await.unwrap(); //map_err(|_| AsyncError::Checkout)?;
        task::spawn_blocking(move || {
            f(&mut *conn.inner())
        })
        .await
        .unwrap() // Propagate panics
    }

    #[inline]
    async fn transaction<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&mut Conn) -> QueryResult<R> + Send + 'static,
    {
        let self_ = self.clone();
        let conn = self_.get_owned().await.unwrap(); //map_err(|_| AsyncError::Checkout)?;
        task::spawn_blocking(move || {
            let mut conn = conn.inner();
            conn.transaction(|c| f(c))
        })
        .await
        .unwrap() // Propagate panics
    }
}

/// An async variant of [`diesel::query_dsl::RunQueryDsl`].
#[async_trait]
pub trait AsyncRunQueryDsl<Conn, AsyncConn>
where
    Conn: 'static + Connection,
{
    async fn execute_async(self, asc: &AsyncConn) -> AsyncResult<usize>
    where
        Self: ExecuteDsl<Conn>;

    async fn load_async<'a, U>(self, asc: &AsyncConn) -> AsyncResult<Vec<U>>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>;

    async fn get_result_async<U>(self, asc: &AsyncConn) -> AsyncResult<U>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>;

    async fn get_results_async<U>(self, asc: &AsyncConn) -> AsyncResult<Vec<U>>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>;

    async fn first_async<U>(self, asc: &AsyncConn) -> AsyncResult<U>
    where
        U: Send + 'static,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>;
}

#[async_trait]
impl<T, AsyncConn, Conn> AsyncRunQueryDsl<Conn, AsyncConn> for T
where
    T: Send + RunQueryDsl<Conn> + 'static,
    Conn: Connection + 'static,
    AsyncConn: AsyncConnection<Conn> + Sync + 'static + Send,
{
    async fn execute_async(self, asc: &AsyncConn) -> AsyncResult<usize>
    where
        Self: ExecuteDsl<Conn>,
    {
        asc.run(|conn| self.execute(conn)).await
    }

    async fn load_async<'a, U>(self, asc: &AsyncConn) -> AsyncResult<Vec<U>>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.load(conn)).await
    }

    async fn get_result_async<U>(self, asc: &AsyncConn) -> AsyncResult<U>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_result(conn)).await
    }

    async fn get_results_async<U>(self, asc: &AsyncConn) -> AsyncResult<Vec<U>>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_results(conn)).await
    }

    async fn first_async<U>(self, asc: &AsyncConn) -> AsyncResult<U>
    where
        U: Send + 'static,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.first(conn)).await
    }
}
