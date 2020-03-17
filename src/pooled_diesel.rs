use bb8::PooledConnection;
use diesel::{
    connection::{SimpleConnection, AnsiTransactionManager},
    ConnectionResult, QueryResult,
};
use diesel::{
    deserialize::QueryableByName,
    query_builder::{AsQuery, QueryFragment, QueryId},
    sql_types::HasSqlType,
    backend::UsesAnsiSavepointSyntax,
    Queryable,
    ConnectionError,
};
use std::{fmt::Debug, ops::{Deref, DerefMut}};
use tokio::task;

/// Utility wrapper to implement `Connection` and `SimpleConnection` on top of
/// `PooledConnection`.
///
/// All blocking methods within this type delegate to `block_in_place`. The
/// number of threads is not unbounded, however, as they are controlled by the
/// truly asynchronous `bb8::Pool` owner. This type makes it easy to use diesel
/// without fear of blocking the runtime and without fear of spawning too many
/// child threads.
///
/// Note that trying to construct this type via `Connection::establish` will
/// panic. The only correct way to construct this type is via tuple constructor
/// (the inner `PooledConnection` is public).
pub struct PooledDieselConnection<'a, M>(pub PooledConnection<'a, M>)
where
    M: bb8::ManageConnection;

impl<'a, M> Deref for PooledDieselConnection<'a, M>
where
    M: bb8::ManageConnection,
{
    type Target = PooledConnection<'a, M>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<M> DerefMut for PooledDieselConnection<'_, M>
where
    M: bb8::ManageConnection,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<M> SimpleConnection for PooledDieselConnection<'_, M>
where
    M: bb8::ManageConnection,
    M::Connection: diesel::Connection,
{
    fn batch_execute(&self, query: &str) -> QueryResult<()> {
        task::block_in_place(|| self.0.batch_execute(query))
    }
}

impl<M> diesel::Connection for PooledDieselConnection<'_, M>
where
    M: bb8::ManageConnection,
    M::Connection: diesel::Connection<TransactionManager = AnsiTransactionManager>,
    <M::Connection as diesel::Connection>::Backend: UsesAnsiSavepointSyntax,
{
    type Backend = <M::Connection as diesel::Connection>::Backend;
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
