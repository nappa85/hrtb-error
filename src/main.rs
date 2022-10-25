use std::convert::Infallible;

use futures_util::TryStreamExt;

use sea_orm::{
    ConnectionTrait, Database, DatabaseConnection, DbErr, EntityTrait, TransactionTrait,
};

use warp::{Filter, Rejection, Reply};

mod entity;

pub async fn get_conn() -> DatabaseConnection {
    let url = std::env::var("DATABASE_URL");
    Database::connect(url.as_deref().unwrap_or("mysql://root@127.0.0.l/test"))
        .await
        .unwrap()
}

async fn lock_and_list<C>(conn: &C) -> Result<Vec<String>, DbErr>
where
    C: ConnectionTrait + TransactionTrait,
{
    let txn = conn.begin().await?;

    let lock = logic_lock::Lock::build("test", txn, None)
        .await
        .map_err(|e| DbErr::Custom(format!("{}", e)))?;
 
    let res = entity::Entity::find()
        .stream(&lock)
        .await?
        .map_ok(|m| format!("{:?}", m))
        .try_collect()
        .await;

    let _txn = lock.release().await.unwrap();

    res
}

async fn list() -> Result<impl warp::Reply, Infallible> {
    let conn = get_conn().await;

    let list = lock_and_list(&conn).await.unwrap();

    Ok(warp::reply::json(&list))
}

fn build_routes() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone + Send {
    warp::get().and_then(list)
}

#[tokio::main]
async fn main() {
    let routes = build_routes();

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}
