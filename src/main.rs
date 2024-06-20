use std::sync::Arc;
use tokio::net::TcpListener;

pub mod config;
pub mod redis;
pub mod resp;
use crate::config::*;
use crate::redis::Redis;

#[tokio::main]
async fn main() -> Result<(), Box<(dyn std::error::Error + 'static)>> {
    let config = Arc::new(Config::parse());
    let host = String::from("127.0.0.1");
    let listener = TcpListener::bind(format!("{}:{}", host, &config.port)).await?;
    let mut redis = Redis::new(Arc::clone(&config), listener).await.expect("");
    redis.listen().await?;
    Ok(())
}
