use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bevy_impulse::{Cancellation, CancellationCause};

pub(super) struct WorkflowCancelledResponse<'a>(pub(super) &'a Cancellation);

impl<'a> IntoResponse for WorkflowCancelledResponse<'a> {
    fn into_response(self) -> Response {
        let msg = match &*self.0.cause {
            CancellationCause::TargetDropped(_) => "target dropped",
            CancellationCause::Unreachable(_) => "unreachable",
            CancellationCause::Filtered(_) => "filtered",
            CancellationCause::Triggered(e) => {
                dbg!(e);
                "triggered"
            }
            CancellationCause::Supplanted(_) => "supplanted",
            CancellationCause::InvalidSpan(_) => "invalid span",
            CancellationCause::CircularCollect(_) => "circular collect",
            CancellationCause::Undeliverable => "undeliverable",
            CancellationCause::PoisonedMutexInPromise => "poisoned mutex in promise",
            CancellationCause::Broken(_) => "broken",
        };
        Response::builder()
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .body(format!("workflow cancelled: {}", msg))
            .map_or(StatusCode::INTERNAL_SERVER_ERROR.into_response(), |resp| {
                resp.into_response()
            })
    }
}
