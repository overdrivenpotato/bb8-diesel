use async_trait::async_trait;
use diesel::r2d2::{self, ManageConnection};
use std::sync::{Arc, Mutex};

mod pooled_diesel;

pub use bb8;
pub use diesel;
pub use pooled_diesel::PooledDieselConnection;

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
    type Connection = T;
    type Error = <r2d2::ConnectionManager<T> as r2d2::ManageConnection>::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.run_blocking(|m| m.connect()).await
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
