use crate::models::{
    CreateOperationRequest, CreateOperationResponse, DocumentOperation, PaginatedResponse,
    PaginationQuery, PendingOperation, ReplayOperation, ReplayQuery, ReplayTimeline,
};
use crate::repository::OperationRepository;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::{error, info, warn};

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

    let pending = PendingOperation::from_request(req);
    let response = CreateOperationResponse {
        id: pending.id,
        created_at: pending.created_at,
    };

    match repo.enqueue_operation(pending).await {
        Ok(_) => Ok(Json(response)),
        Err(mpsc::error::TrySendError::Full(_)) => {
            warn!("写入队列已满，返回 503 服务不可用");
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            error!("写入队列已关闭");
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

    let query_future = repo.list_by_document_id(&document_id, page, page_size);

    match timeout(Duration::from_secs(5), query_future).await {
        Ok(Ok((items, total))) => Ok(Json(PaginatedResponse::new(items, total, page, page_size))),
        Ok(Err(e)) => {
            error!("查询操作记录失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
        Err(_) => {
            error!("查询操作记录超时");
            Err(StatusCode::GATEWAY_TIMEOUT)
        }
    }
}

pub async fn replay_operations(
    State(repo): State<AppState>,
    Path(document_id): Path<String>,
    Query(query): Query<ReplayQuery>,
) -> Result<Json<ReplayTimeline>, StatusCode> {
    info!(
        document_id = %document_id,
        start_time = ?query.start_time,
        end_time = ?query.end_time,
        include_content = query.include_content,
        "回放文档操作记录"
    );

    let query_future = repo.replay_operations(&document_id, query);

    match timeout(Duration::from_secs(10), query_future).await {
        Ok(Ok(operations)) => {
            if operations.is_empty() {
                return Err(StatusCode::NOT_FOUND);
            }

            let total_operations = operations.len() as i64;
            let start_time = operations.first().unwrap().created_at;
            let end_time = operations.last().unwrap().created_at;
            let total_duration_ms = end_time
                .signed_duration_since(start_time)
                .num_milliseconds();

            let mut replay_ops = Vec::with_capacity(operations.len());
            for (i, op) in operations.iter().enumerate() {
                let seq = (i + 1) as i64;
                let time_delta_ms = if i == 0 {
                    0
                } else {
                    op.created_at
                        .signed_duration_since(operations[i - 1].created_at)
                        .num_milliseconds()
                };

                replay_ops.push(ReplayOperation {
                    sequence: seq,
                    operation_id: op.id,
                    user_id: op.user_id.clone(),
                    operation_type: op.operation_type,
                    timestamp: op.created_at,
                    time_delta_ms,
                    content_before: op.content_before.clone(),
                    content_after: op.content_after.clone(),
                    change_summary: op.change_summary.clone(),
                });
            }

            let timeline = ReplayTimeline {
                document_id,
                total_operations,
                start_time,
                end_time,
                total_duration_ms,
                operations: replay_ops,
            };

            Ok(Json(timeline))
        }
        Ok(Err(e)) => {
            error!("回放操作记录失败: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
        Err(_) => {
            error!("回放操作记录超时");
            Err(StatusCode::GATEWAY_TIMEOUT)
        }
    }
}

pub async fn health_check() -> StatusCode {
    StatusCode::OK
}
