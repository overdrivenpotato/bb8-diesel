#[macro_use]
extern crate diesel;

use diesel::pg::PgConnection;
use diesel::prelude::*;

table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

#[derive(AsChangeset, Identifiable, Queryable, PartialEq)]
#[table_name = "users"]
pub struct User {
    pub id: i32,
    pub name: String,
}

#[tokio::main]
async fn main() {
/*
    use users::dsl;

    let manager = bb8_diesel::DieselConnectionManager::<PgConnection>::new("localhost:1234");
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let conn = pool.get().await.unwrap();

    // Insert
    let _ = diesel::insert_into(dsl::users)
        .values((dsl::id.eq(0), dsl::name.eq("Jim")))
        .execute(&*conn)
        .unwrap();

    // Load
    let _ = dsl::users.get_result::<User>(&*conn).unwrap();

    // Update
    let _ = diesel::update(dsl::users)
        .filter(dsl::id.eq(0))
        .set(dsl::name.eq("Jim, But Different"))
        .execute(&*conn)
        .unwrap();

    // Update via save_changes
    let _ = User {
        id: 0,
        name: "Jim".to_string(),
    }
    .save_changes::<User>(&*conn)
    .unwrap();

    // Delete
    let _ = diesel::delete(dsl::users)
        .filter(dsl::id.eq(0))
        .execute(&*conn)
        .unwrap();

    // Transaction with multiple operations
    conn.transaction::<_, diesel::result::Error, _>(|| {
        diesel::insert_into(dsl::users)
            .values((dsl::id.eq(0), dsl::name.eq("Jim")))
            .execute(&*conn)
            .unwrap();
        diesel::insert_into(dsl::users)
            .values((dsl::id.eq(1), dsl::name.eq("Another Jim")))
            .execute(&*conn)
            .unwrap();
        Ok(())
    })
    .unwrap();
*/
}
