mod handlers;
mod models;
mod repository;

use crate::handlers::{AppState, create_operation, health_check, list_operations};
use crate::repository::OperationRepository;
use axum::routing::{get, post};
use axum::Router;
use sqlx::SqlitePool;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:operations.db".to_string());
    info!("连接数据库: {}", database_url);

    let pool = SqlitePool::connect(&database_url).await?;
    let repo = OperationRepository::new(pool);

    info!("初始化数据库表...");
    repo.init_db().await?;
    info!("数据库初始化完成");

    let app_state: AppState = Arc::new(repo);

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/operations", post(create_operation))
        .route("/api/operations/:document_id", get(list_operations))
        .with_state(app_state);

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse::<u16>()
        .expect("无效的端口号");

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", host, port)).await?;
    info!("服务器启动在 http://{}:{}", host, port);

    axum::serve(listener, app).await?;

    Ok(())
}
