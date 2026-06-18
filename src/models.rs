use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentOperation {
    pub id: Uuid,
    pub document_id: String,
    pub user_id: String,
    pub operation_type: OperationType,
    pub content_before: Option<String>,
    pub content_after: Option<String>,
    pub change_summary: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationType {
    Create,
    Edit,
    Delete,
    Rename,
    Format,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Create => write!(f, "create"),
            OperationType::Edit => write!(f, "edit"),
            OperationType::Delete => write!(f, "delete"),
            OperationType::Rename => write!(f, "rename"),
            OperationType::Format => write!(f, "format"),
        }
    }
}

impl OperationType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "create" => Some(OperationType::Create),
            "edit" => Some(OperationType::Edit),
            "delete" => Some(OperationType::Delete),
            "rename" => Some(OperationType::Rename),
            "format" => Some(OperationType::Format),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateOperationRequest {
    pub document_id: String,
    pub user_id: String,
    pub operation_type: OperationType,
    pub content_before: Option<String>,
    pub content_after: Option<String>,
    pub change_summary: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PendingOperation {
    pub id: Uuid,
    pub document_id: String,
    pub user_id: String,
    pub operation_type: OperationType,
    pub content_before: Option<String>,
    pub content_after: Option<String>,
    pub change_summary: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl PendingOperation {
    pub fn from_request(req: CreateOperationRequest) -> Self {
        Self {
            id: Uuid::new_v4(),
            document_id: req.document_id,
            user_id: req.user_id,
            operation_type: req.operation_type,
            content_before: req.content_before,
            content_after: req.content_after,
            change_summary: req.change_summary,
            created_at: Utc::now(),
        }
    }

    pub fn to_document_operation(&self) -> DocumentOperation {
        DocumentOperation {
            id: self.id,
            document_id: self.document_id.clone(),
            user_id: self.user_id.clone(),
            operation_type: self.operation_type,
            content_before: self.content_before.clone(),
            content_after: self.content_after.clone(),
            change_summary: self.change_summary.clone(),
            created_at: self.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateOperationResponse {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaginationQuery {
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_page_size")]
    pub page_size: i64,
}

fn default_page() -> i64 {
    1
}

fn default_page_size() -> i64 {
    20
}

#[derive(Debug, Clone, Serialize)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
    pub total_pages: i64,
}

impl<T> PaginatedResponse<T> {
    pub fn new(items: Vec<T>, total: i64, page: i64, page_size: i64) -> Self {
        let total_pages = if total == 0 {
            0
        } else {
            (total + page_size - 1) / page_size
        };
        Self {
            items,
            total,
            page,
            page_size,
            total_pages,
        }
    }
}
