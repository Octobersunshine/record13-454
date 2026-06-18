use crate::models::{
    CreateOperationRequest, CreateOperationResponse, DocumentOperation, PaginatedResponse,
    PaginationQuery,
};
use crate::repository::OperationRepository;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use std::sync::Arc;
use tracing::{error, info};

pub type AppState = Arc<OperationRepository>;

pub async fn create_operation(
    State(repo): State<AppState>,
    Json(req): Json<CreateOperationRequest>,
) -> Result<Json<CreateOperationResponse>, StatusCode> {
    info!(
        document_id = %req.document_id,
        user_id = %req.user_id,
        operation_type = ?req.operation_type,
        "创建文档操作记录"
    );

    match repo.create_operation(req).await {
        Ok(op) => Ok(Json(CreateOperationResponse {
            id: op.id,
            created_at: op.created_at,
        })),
        Err(e) => {
            error!("创建操作记录失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn list_operations(
    State(repo): State<AppState>,
    Path(document_id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> Result<Json<PaginatedResponse<DocumentOperation>>, StatusCode> {
    let page = query.page.max(1);
    let page_size = query.page_size.max(1).min(100);

    info!(
        document_id = %document_id,
        page = page,
        page_size = page_size,
        "查询文档操作记录"
    );

    match repo.list_by_document_id(&document_id, page, page_size).await {
        Ok((items, total)) => Ok(Json(PaginatedResponse::new(items, total, page, page_size))),
        Err(e) => {
            error!("查询操作记录失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn health_check() -> StatusCode {
    StatusCode::OK
}
