use crate::models::{CreateOperationRequest, DocumentOperation, OperationType};
use chrono::Utc;
use sqlx::{SqlitePool, Row};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct OperationRepository {
    pool: SqlitePool,
}

impl OperationRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn create_operation(
        &self,
        req: CreateOperationRequest,
    ) -> Result<DocumentOperation, sqlx::Error> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        let operation_type_str = req.operation_type.to_string();

        sqlx::query(
            r#"
            INSERT INTO document_operations
                (id, document_id, user_id, operation_type, content_before, content_after, change_summary, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(id.to_string())
        .bind(&req.document_id)
        .bind(&req.user_id)
        .bind(&operation_type_str)
        .bind(&req.content_before)
        .bind(&req.content_after)
        .bind(&req.change_summary)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(DocumentOperation {
            id,
            document_id: req.document_id,
            user_id: req.user_id,
            operation_type: req.operation_type,
            content_before: req.content_before,
            content_after: req.content_after,
            change_summary: req.change_summary,
            created_at: now,
        })
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
            let id = Uuid::parse_str(&id_str).unwrap_or_else(|_| Uuid::new_v4());

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
