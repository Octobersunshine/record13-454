use crate::models::{DocumentOperation, OperationType, PendingOperation, ReplayQuery};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{SqlitePool, Row};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

pub struct OperationRepository {
    pool: SqlitePool,
    tx: mpsc::Sender<PendingOperation>,
}

impl Clone for OperationRepository {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl std::fmt::Debug for OperationRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationRepository").finish()
    }
}

pub const DEFAULT_BATCH_SIZE: usize = 100;
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 200;
pub const DEFAULT_QUEUE_CAPACITY: usize = 10000;
pub const DEFAULT_MAX_RETRIES: u32 = 5;
pub const DEFAULT_RETRY_BASE_DELAY_MS: u64 = 50;

impl OperationRepository {
    pub async fn new(database_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = Self::create_pool(database_url).await?;
        let (tx, rx) = mpsc::channel::<PendingOperation>(DEFAULT_QUEUE_CAPACITY);
        let repo = Self { pool, tx };
        repo.init_db().await?;
        repo.enable_wal().await?;
        repo.start_worker(rx);
        Ok(repo)
    }

    async fn create_pool(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
        let options = database_url
            .parse::<SqliteConnectOptions>()?
            .busy_timeout(Duration::from_secs(5))
            .synchronous(SqliteSynchronous::Normal)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .min_connections(2)
            .acquire_timeout(Duration::from_secs(8))
            .idle_timeout(Duration::from_secs(60))
            .connect_with(options)
            .await?;

        Ok(pool)
    }

    async fn enable_wal(&self) -> Result<(), sqlx::Error> {
        sqlx::query("PRAGMA journal_mode = WAL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("PRAGMA synchronous = NORMAL;")
            .execute(&self.pool)
            .await?;

        sqlx::query("PRAGMA busy_timeout = 5000;")
            .execute(&self.pool)
            .await?;

        sqlx::query("PRAGMA cache_size = -64000;")
            .execute(&self.pool)
            .await?;

        info!("SQLite WAL 模式已启用");
        Ok(())
    }

    pub async fn enqueue_operation(
        &self,
        pending: PendingOperation,
    ) -> Result<(), mpsc::error::TrySendError<PendingOperation>> {
        self.tx.try_send(pending)
    }

    pub fn start_worker(&self, mut rx: mpsc::Receiver<PendingOperation>) {
        let pool = self.pool.clone();
        tokio::spawn(async move {
            info!("后台批量写入 worker 已启动");
            let mut batch: Vec<PendingOperation> = Vec::with_capacity(DEFAULT_BATCH_SIZE);
            let mut interval =
                tokio::time::interval(Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS));

            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(op) => {
                                batch.push(op);
                                if batch.len() >= DEFAULT_BATCH_SIZE {
                                    Self::flush_batch(&pool, &mut batch).await;
                                }
                            }
                            None => {
                                info!("写入队列已关闭，刷新剩余 {} 条记录", batch.len());
                                Self::flush_batch(&pool, &mut batch).await;
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        if !batch.is_empty() {
                            Self::flush_batch(&pool, &mut batch).await;
                        }
                    }
                }
            }
            info!("后台批量写入 worker 已退出");
        });
    }

    async fn flush_batch(pool: &SqlitePool, batch: &mut Vec<PendingOperation>) {
        if batch.is_empty() {
            return;
        }

        let size = batch.len();
        debug!("开始批量写入 {} 条操作记录", size);

        match Self::insert_batch_with_retry(pool, batch).await {
            Ok(_) => {
                debug!("批量写入成功，{} 条记录", size);
                batch.clear();
            }
            Err(e) => {
                error!("批量写入最终失败，丢弃 {} 条记录: {}", size, e);
                batch.clear();
            }
        }
    }

    async fn insert_batch_with_retry(
        pool: &SqlitePool,
        batch: &[PendingOperation],
    ) -> Result<(), sqlx::Error> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match Self::insert_batch(pool, batch).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt >= DEFAULT_MAX_RETRIES {
                        return Err(e);
                    }
                    let is_busy = matches!(
                        e,
                        sqlx::Error::Database(ref db_err)
                            if db_err.message().contains("database is locked")
                                || db_err.message().contains("busy")
                    );
                    if !is_busy {
                        return Err(e);
                    }
                    let delay =
                        Duration::from_millis(DEFAULT_RETRY_BASE_DELAY_MS * 2u64.pow(attempt - 1));
                    warn!(
                        "数据库锁定，第 {}/{} 次重试，等待 {:?} 后重试",
                        attempt, DEFAULT_MAX_RETRIES, delay
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn insert_batch(
        pool: &SqlitePool,
        batch: &[PendingOperation],
    ) -> Result<(), sqlx::Error> {
        let mut tx = pool.begin().await?;

        for op in batch {
            let operation_type_str = op.operation_type.to_string();
            sqlx::query(
                r#"
                INSERT INTO document_operations
                    (id, document_id, user_id, operation_type, content_before, content_after, change_summary, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(op.id.to_string())
            .bind(&op.document_id)
            .bind(&op.user_id)
            .bind(&operation_type_str)
            .bind(&op.content_before)
            .bind(&op.content_after)
            .bind(&op.change_summary)
            .bind(op.created_at)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn list_by_document_id(
        &self,
        document_id: &str,
        page: i64,
        page_size: i64,
    ) -> Result<(Vec<DocumentOperation>, i64), sqlx::Error> {
        let offset = (page - 1) * page_size;
        let page_size = page_size.max(1).min(100);

        let total_row = sqlx::query(
            "SELECT COUNT(*) as count FROM document_operations WHERE document_id = ?",
        )
        .bind(document_id)
        .fetch_one(&self.pool)
        .await?;

        let total: i64 = total_row.try_get("count")?;

        let rows = sqlx::query(
            r#"
            SELECT id, document_id, user_id, operation_type, content_before,
                   content_after, change_summary, created_at
            FROM document_operations
            WHERE document_id = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(document_id)
        .bind(page_size)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let operation_type_str: String = row.try_get("operation_type")?;
            let operation_type = OperationType::from_str(&operation_type_str)
                .unwrap_or(OperationType::Edit);

            let id_str: String = row.try_get("id")?;
            let id = Uuid::parse_str(&id_str).unwrap_or_else(|_| uuid::Uuid::new_v4());

            items.push(DocumentOperation {
                id,
                document_id: row.try_get("document_id")?,
                user_id: row.try_get("user_id")?,
                operation_type,
                content_before: row.try_get("content_before")?,
                content_after: row.try_get("content_after")?,
                change_summary: row.try_get("change_summary")?,
                created_at: row.try_get("created_at")?,
            });
        }

        Ok((items, total))
    }

    pub async fn replay_operations(
        &self,
        document_id: &str,
        query: ReplayQuery,
    ) -> Result<Vec<DocumentOperation>, sqlx::Error> {
        let mut sql = r#"
            SELECT id, document_id, user_id, operation_type, content_before,
                   content_after, change_summary, created_at
            FROM document_operations
            WHERE document_id = ?
        "#
        .to_string();

        let mut conditions: Vec<String> = Vec::new();
        if query.start_time.is_some() {
            conditions.push("created_at >= ?".to_string());
        }
        if query.end_time.is_some() {
            conditions.push("created_at <= ?".to_string());
        }

        if !conditions.is_empty() {
            sql.push_str(" AND ");
            sql.push_str(&conditions.join(" AND "));
        }

        sql.push_str(" ORDER BY created_at ASC");

        let mut sql_query = sqlx::query(&sql).bind(document_id);

        if let Some(start_time) = query.start_time {
            sql_query = sql_query.bind(start_time);
        }
        if let Some(end_time) = query.end_time {
            sql_query = sql_query.bind(end_time);
        }

        let rows = sql_query.fetch_all(&self.pool).await?;

        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let operation_type_str: String = row.try_get("operation_type")?;
            let operation_type = OperationType::from_str(&operation_type_str)
                .unwrap_or(OperationType::Edit);

            let id_str: String = row.try_get("id")?;
            let id = Uuid::parse_str(&id_str).unwrap_or_else(|_| uuid::Uuid::new_v4());

            let content_before = if query.include_content {
                row.try_get("content_before")?
            } else {
                None
            };
            let content_after = if query.include_content {
                row.try_get("content_after")?
            } else {
                None
            };

            items.push(DocumentOperation {
                id,
                document_id: row.try_get("document_id")?,
                user_id: row.try_get("user_id")?,
                operation_type,
                content_before,
                content_after,
                change_summary: row.try_get("change_summary")?,
                created_at: row.try_get("created_at")?,
            });
        }

        Ok(items)
    }

    pub async fn init_db(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS document_operations (
                id TEXT PRIMARY KEY,
                document_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                operation_type TEXT NOT NULL,
                content_before TEXT,
                content_after TEXT,
                change_summary TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_document_id ON document_operations (document_id)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_created_at ON document_operations (created_at)",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
