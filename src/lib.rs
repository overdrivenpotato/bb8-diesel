//! bb8-diesel allows the bb8 asynchronous connection pool
//! to be used underneath Diesel.
//!
//! This is currently implemented against Diesel's synchronous
//! API, with calls to [`tokio::task::spawn_blocking`] to safely
//! perform synchronous operations from an asynchronous task.

use async_trait::async_trait;
use diesel::{
    connection::{
        AnsiTransactionManager, ConnectionGatWorkaround, LoadConnection, LoadRowIter,
        SimpleConnection, TransactionManager,
    },
    expression::QueryMetadata,
    query_builder::{Query, QueryFragment, QueryId},
    query_dsl::UpdateAndFetchResults,
    r2d2::{self, ManageConnection},
    result::Error,
    ConnectionError, ConnectionResult, QueryResult,
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
///     let mut conn = pool.get().await.unwrap();
///
///     diesel::insert_into(dsl::users)
///         .values(dsl::id.eq(1337))
///         .execute(&mut *conn)
///         .unwrap();
/// }
/// ```
#[derive(Clone)]
pub struct DieselConnectionManager<T: diesel::Connection + Send + 'static + diesel::r2d2::R2D2Connection> {
    inner: Arc<Mutex<r2d2::ConnectionManager<T>>>,
}

impl<T: diesel::Connection + Send + 'static + diesel::r2d2::R2D2Connection> DieselConnectionManager<T> {
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
    T: diesel::Connection + Send + 'static + diesel::r2d2::R2D2Connection,
{
    type Connection = DieselConnection<T>;
    type Error = <r2d2::ConnectionManager<T> as r2d2::ManageConnection>::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.run_blocking(|m| m.connect())
            .await
            .map(DieselConnection)
    }

    async fn is_valid<'life0, 'life1>(
        &'life0 self,
        conn: &'life1 mut Self::Connection,
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
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        task::block_in_place(|| self.0.batch_execute(query))
    }
}

impl<'a, 'b, C> ConnectionGatWorkaround<'a, 'b, C::Backend> for DieselConnection<C>
where
    C: diesel::Connection<TransactionManager = AnsiTransactionManager>,
{
    type Cursor = <C as ConnectionGatWorkaround<'a, 'b, C::Backend>>::Cursor;
    type Row = <C as ConnectionGatWorkaround<'a, 'b, C::Backend>>::Row;
}

impl<C> diesel::Connection for DieselConnection<C>
where
    C: diesel::Connection<TransactionManager = AnsiTransactionManager>,
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

    fn transaction<T, E, F>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        task::block_in_place(|| Self::TransactionManager::transaction(self, f))
    }

    fn begin_test_transaction(&mut self) -> QueryResult<()> {
        task::block_in_place(|| self.0.begin_test_transaction())
    }

    fn test_transaction<T, E, F>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: Debug,
    {
        // taken from the default impl of `test_transaction` (with `task::block_in_place` added)
        let mut user_result = None;
        let _ = task::block_in_place(|| {
            self.transaction::<(), _, _>(|conn| {
                user_result = f(conn).ok();
                Err(Error::RollbackTransaction)
            })
        });
        user_result.expect("Transaction did not succeed")
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        task::block_in_place(|| self.0.execute_returning_count(source))
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData {
        task::block_in_place(|| self.0.transaction_state())
    }
}

impl<C> LoadConnection for DieselConnection<C>
where
    C: LoadConnection,
    C: diesel::Connection<TransactionManager = AnsiTransactionManager>,
{
    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<LoadRowIter<'conn, 'query, Self, Self::Backend>>
    where
        T: Query + QueryFragment<Self::Backend> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        task::block_in_place(|| self.0.load(source))
    }
}

impl<Conn, Changes, Output> UpdateAndFetchResults<Changes, Output> for DieselConnection<Conn>
where
    Conn: diesel::Connection<TransactionManager = AnsiTransactionManager>,
    Conn: UpdateAndFetchResults<Changes, Output>,
{
    fn update_and_fetch(&mut self, changeset: Changes) -> QueryResult<Output> {
        task::block_in_place(|| self.0.update_and_fetch(changeset))
    }
}
