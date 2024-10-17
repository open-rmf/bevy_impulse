use bevy_impulse::{BlockingMap, StreamPack};

use crate::{DynType, InferDynRequest};

impl<Request, Streams> InferDynRequest<Request> for BlockingMap<Request, Streams>
where
    Request: DynType,
    Streams: StreamPack,
{
}
