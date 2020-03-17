use bb8::PooledConnection;
use diesel::{
    connection::{SimpleConnection, TransactionManager},
    ConnectionResult, QueryResult,
};
use diesel::{
    deserialize::QueryableByName,
    query_builder::{AsQuery, QueryFragment, QueryId},
    sql_types::HasSqlType,
    Queryable,
};
use std::ops::{Deref, DerefMut};

/// Utility wrapper to implement `Connection` and `SimpleConnection` on top of
/// `PooledConnection`.
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
        self.0.batch_execute(query)
    }
}

impl<M> diesel::Connection for PooledDieselConnection<'_, M>
where
    M: bb8::ManageConnection,
    M::Connection: diesel::Connection,
    <M::Connection as diesel::Connection>::TransactionManager:
        TransactionManager<Self>,
{
    type Backend = <M::Connection as diesel::Connection>::Backend;
    type TransactionManager =
        <M::Connection as diesel::Connection>::TransactionManager;

    fn establish(_database_url: &str) -> ConnectionResult<Self> {
        // Really not much else we can do here.
        panic!("tried to establish pool connection outside of pool")
    }

    fn execute(&self, query: &str) -> QueryResult<usize> {
        self.0.execute(query)
    }

    fn query_by_index<T, U>(&self, source: T) -> QueryResult<Vec<U>>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
        Self::Backend: HasSqlType<T::SqlType>,
        U: Queryable<T::SqlType, Self::Backend>,
    {
        self.0.query_by_index(source)
    }

    fn query_by_name<T, U>(&self, source: &T) -> QueryResult<Vec<U>>
    where
        T: QueryFragment<Self::Backend> + QueryId,
        U: QueryableByName<Self::Backend>,
    {
        self.0.query_by_name(source)
    }

    fn execute_returning_count<T>(&self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        self.0.execute_returning_count(source)
    }

    fn transaction_manager(&self) -> &Self::TransactionManager {
        &self.0.transaction_manager()
    }
}
