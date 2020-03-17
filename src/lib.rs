use async_trait::async_trait;
use diesel::{
    backend::UsesAnsiSavepointSyntax,
    connection::{AnsiTransactionManager, SimpleConnection},
    deserialize::QueryableByName,
    query_builder::{AsQuery, QueryFragment, QueryId},
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

pub use bb8;
pub use diesel;

#[derive(Clone)]
pub struct DieselConnectionManager<T> {
    inner: Arc<Mutex<r2d2::ConnectionManager<T>>>,
}

impl<T: Send + 'static> DieselConnectionManager<T> {
    pub fn new<S: Into<String>>(database_url: S) -> Self {
        Self {
            inner: Arc::new(Mutex::new(r2d2::ConnectionManager::new(
                database_url,
            ))),
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
}

#[async_trait]
impl<T> bb8::ManageConnection for DieselConnectionManager<T>
where
    T: diesel::Connection + Send + 'static,
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
        mut conn: Self::Connection,
    ) -> Result<Self::Connection, Self::Error> {
        self.run_blocking(|m| {
            m.is_valid(&mut conn)?;
            Ok(conn)
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
/// `diesel::Connection`.
///
/// All blocking methods within this type delegate to `block_in_place`. The
/// number of threads is not unbounded, however, as they are controlled by the
/// truly asynchronous `bb8::Pool` owner. This type makes it easy to use diesel
/// without fear of blocking the runtime and without fear of spawning too many
/// child threads.
///
/// Note that trying to construct this type via `Connection::establish` will
/// panic. The only correct way to construct this type is by using a bb8 pool.
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

impl<C> diesel::Connection for DieselConnection<C>
where
    C: diesel::Connection<TransactionManager = AnsiTransactionManager>,
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
