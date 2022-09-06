use diesel::pg::PgConnection;
use diesel::prelude::*;

table! {
    users (id) {
        id -> Integer,
        name -> Text,
    }
}

#[derive(AsChangeset, Identifiable, Queryable, Eq, PartialEq)]
#[diesel(table_name = users)]
pub struct User {
    pub id: i32,
    pub name: String,
}

#[tokio::main]
async fn main() {
    use users::dsl;

    let manager = bb8_diesel::DieselConnectionManager::<PgConnection>::new("localhost:1234");
    let pool = bb8::Pool::builder().build(manager).await.unwrap();
    let mut conn = pool.get().await.unwrap();

    // Insert
    let _ = diesel::insert_into(dsl::users)
        .values((dsl::id.eq(0), dsl::name.eq("Jim")))
        .execute(&mut *conn)
        .unwrap();

    // Load
    let _ = dsl::users.load::<User>(&mut *conn).unwrap();

    // Update
    let _ = diesel::update(dsl::users)
        .filter(dsl::id.eq(0))
        .set(dsl::name.eq("Jim, But Different"))
        .execute(&mut *conn)
        .unwrap();

    // Update via save_changes
    let _ = User {
        id: 0,
        name: "Jim".to_string(),
    }
    .save_changes::<User>(&mut *conn)
    .unwrap();

    // Delete
    let _ = diesel::delete(dsl::users)
        .filter(dsl::id.eq(0))
        .execute(&mut *conn)
        .unwrap();

    // Transaction with multiple operations
    conn.transaction::<_, diesel::result::Error, _>(|conn| {
        diesel::insert_into(dsl::users)
            .values((dsl::id.eq(0), dsl::name.eq("Jim")))
            .execute(&mut *conn)
            .unwrap();
        diesel::insert_into(dsl::users)
            .values((dsl::id.eq(1), dsl::name.eq("Another Jim")))
            .execute(&mut *conn)
            .unwrap();
        Ok(())
    })
    .unwrap();
}
