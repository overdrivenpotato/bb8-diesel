//! bb8-diesel allows the bb8 asynchronous connection pool
//! to be used underneath Diesel.
//!
//! This is currently implemented against Diesel's synchronous
//! API, with calls to [`tokio::task::spawn_blocking`] to safely
//! perform synchronous operations from an asynchronous task.

use async_trait::async_trait;
use diesel::{
    backend::UsesAnsiSavepointSyntax,
    connection::{AnsiTransactionManager, Connection, SimpleConnection},
    deserialize::QueryableByName,
    dsl::Limit,
    query_builder::{AsQuery, QueryFragment, QueryId},
    query_dsl::{
        methods::{ExecuteDsl, LimitDsl, LoadQuery},
        RunQueryDsl, UpdateAndFetchResults,
    },
    r2d2::{self, ManageConnection},
    sql_types::HasSqlType,
    ConnectionError, ConnectionResult, QueryResult, Queryable,
};
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};
use tokio::task;

/// A connection manager which implements [`bb8::ManageConnection`] to
/// integrate with bb8.
///
/// ```no_run
/// #[macro_use]
/// extern crate diesel;
///
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
///     let conn = pool.get().await.unwrap();
///
///     diesel::insert_into(dsl::users)
///         .values(dsl::id.eq(1337))
///         .execute(&*conn)
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
    T: Connection + Send + 'static,
{
    type Connection = DieselConnection<T>;
    type Error = <r2d2::ConnectionManager<T> as r2d2::ManageConnection>::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.run_blocking(|m| m.connect())
            .await
            .map(DieselConnection)
    }

    async fn is_valid(
        &self,
        conn: &mut bb8::PooledConnection<'_, Self>,
    ) -> Result<(), Self::Error> {
        self.run_blocking_in_place(|m| {
            m.is_valid(&mut *conn)?;
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
pub struct DieselConnection<C>(pub(crate) C);

impl<C> Deref for DieselConnection<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C> DerefMut for DieselConnection<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<C> SimpleConnection for DieselConnection<C>
where
    C: SimpleConnection,
{
    fn batch_execute(&self, query: &str) -> QueryResult<()> {
        task::block_in_place(|| self.0.batch_execute(query))
    }
}

impl<Conn, Changes, Output> UpdateAndFetchResults<Changes, Output> for DieselConnection<Conn>
where
    Conn: UpdateAndFetchResults<Changes, Output>,
    Conn: Connection<TransactionManager = AnsiTransactionManager>,
    Conn::Backend: UsesAnsiSavepointSyntax,
{
    fn update_and_fetch(&self, changeset: Changes) -> QueryResult<Output> {
        task::block_in_place(|| self.0.update_and_fetch(changeset))
    }
}

impl<C> Connection for DieselConnection<C>
where
    C: Connection<TransactionManager = AnsiTransactionManager>,
    C::Backend: UsesAnsiSavepointSyntax,
{
    type Backend = C::Backend;

    // This type is hidden in the docs so we can assume it is only called via
    // the implemented methods below.
    type TransactionManager = AnsiTransactionManager;

    fn establish(_database_url: &str) -> ConnectionResult<Self> {
        // This is taken from `diesel::r2d2`
        Err(ConnectionError::BadConnection(String::from(
            "Cannot directly establish a pooled connection",
        )))
    }

    fn transaction<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        task::block_in_place(|| self.0.transaction(f))
    }

    fn begin_test_transaction(&self) -> QueryResult<()> {
        task::block_in_place(|| self.0.begin_test_transaction())
    }

    fn test_transaction<T, E, F>(&self, f: F) -> T
    where
        F: FnOnce() -> Result<T, E>,
        E: Debug,
    {
        task::block_in_place(|| self.0.test_transaction(f))
    }

    fn execute(&self, query: &str) -> QueryResult<usize> {
        task::block_in_place(|| self.0.execute(query))
    }

    fn query_by_index<T, U>(&self, source: T) -> QueryResult<Vec<U>>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
        Self::Backend: HasSqlType<T::SqlType>,
        U: Queryable<T::SqlType, Self::Backend>,
    {
        task::block_in_place(|| self.0.query_by_index(source))
    }

    fn query_by_name<T, U>(&self, source: &T) -> QueryResult<Vec<U>>
    where
        T: QueryFragment<Self::Backend> + QueryId,
        U: QueryableByName<Self::Backend>,
    {
        task::block_in_place(|| self.0.query_by_name(source))
    }

    fn execute_returning_count<T>(&self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        task::block_in_place(|| self.0.execute_returning_count(source))
    }

    fn transaction_manager(&self) -> &Self::TransactionManager {
        &self.0.transaction_manager()
    }
}

pub type AsyncResult<R> = Result<R, AsyncError>;

// TODO
pub enum AsyncError {
    /// Failed to checkout a connection.
    // TODO Populate
    Checkout,

    /// Query failure.
    Error(diesel::result::Error),
}

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
    Conn: 'static + Connection,
{
    #[inline]
    async fn batch_execute_async(&self, query: &str) -> AsyncResult<()> {
        let self_ = self.clone();
        let query = query.to_string();
        let conn = self_.get_owned().await.map_err(|_| AsyncError::Checkout)?;
        task::spawn_blocking(move || {
            conn.batch_execute(&query).map_err(AsyncError::Error)
        })
        .await
        .unwrap()
    }
}

#[async_trait]
pub trait AsyncConnection<Conn>: AsyncSimpleConnection<Conn>
where
    Conn: 'static + Connection,
{
    async fn run<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&Conn) -> QueryResult<R> + Send + 'static;

    async fn transaction<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&Conn) -> QueryResult<R> + Send + 'static;
}

#[async_trait]
impl<Conn> AsyncConnection<Conn> for bb8::Pool<DieselConnectionManager<Conn>>
where
    Conn: 'static + Connection,
{
    #[inline]
    async fn run<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&Conn) -> QueryResult<R> + Send + 'static,
    {
        let self_ = self.clone();
        let conn = self_.get_owned().await.map_err(|_| AsyncError::Checkout)?;
        task::spawn_blocking(move || {
            f(&*conn).map_err(AsyncError::Error)
        })
        .await
        .unwrap() // Propagate panics
    }

    #[inline]
    async fn transaction<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: Send + 'static,
        Func: FnOnce(&Conn) -> QueryResult<R> + Send + 'static,
    {
        let self_ = self.clone();
        let conn = self_.get_owned().await.map_err(|_| AsyncError::Checkout)?;
        task::spawn_blocking(move || {
            conn.transaction(|| f(&*conn)).map_err(AsyncError::Error)
        })
        .await
        .unwrap() // Propagate panics
    }
}

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
impl<T, Conn> AsyncRunQueryDsl<Conn, bb8::Pool<DieselConnectionManager<Conn>>> for T
where
    T: Send + RunQueryDsl<Conn> + 'static,
    Conn: 'static + Connection,
{
    async fn execute_async(self, asc: &bb8::Pool<DieselConnectionManager<Conn>>) -> AsyncResult<usize>
    where
        Self: ExecuteDsl<Conn>,
    {
        asc.run(|conn| self.execute(&*conn)).await
    }

    async fn load_async<'a, U>(self, asc: &bb8::Pool<DieselConnectionManager<Conn>>) -> AsyncResult<Vec<U>>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.load(&*conn)).await
    }

    async fn get_result_async<U>(self, asc: &bb8::Pool<DieselConnectionManager<Conn>>) -> AsyncResult<U>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_result(&*conn)).await
    }

    async fn get_results_async<U>(self, asc: &bb8::Pool<DieselConnectionManager<Conn>>) -> AsyncResult<Vec<U>>
    where
        U: Send + 'static,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_results(&*conn)).await
    }

    async fn first_async<U>(self, asc: &bb8::Pool<DieselConnectionManager<Conn>>) -> AsyncResult<U>
    where
        U: Send + 'static,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.first(&*conn)).await
    }
}
