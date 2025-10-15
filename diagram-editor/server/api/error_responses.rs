use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use crossflow::Cancellation;

pub(super) struct WorkflowCancelledResponse<'a>(pub(super) &'a Cancellation);

impl<'a> IntoResponse for WorkflowCancelledResponse<'a> {
    fn into_response(self) -> Response {
        Response::builder()
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .body(format!("workflow cancelled: {}", self.0.cause))
            .map_or(StatusCode::INTERNAL_SERVER_ERROR.into_response(), |resp| {
                resp.into_response()
            })
    }
}
