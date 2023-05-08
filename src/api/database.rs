use axum::http::StatusCode;
use axum::{Extension, Json, Router};
use crate::raft::Raft;

//
pub fn api() -> Router {
    Router::new()
}