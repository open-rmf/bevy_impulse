/*
 * Copyright (C) 2025 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

use bevy_ecs::{
    prelude::{Commands, Entity, World},
    system::Command,
};

use std::{borrow::Cow, cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use crate::{
    AddOperation, Builder, DefaultStreamContainer, DeferredRoster, ExitTargetStorage, InnerChannel,
    Input, InputBundle, InputSlot, ManageInput, OperationRequest, OperationResult, OperationRoster,
    OperationSetup, OrBroken, Output, RedirectWorkflowStream, ReportUnhandled, ScopeStorage,
    SingleInputStorage, Stream, StreamEffect, StreamRedirect, StreamRequest, StreamTargetMap,
    UnusedStreams, UnusedTarget,
};

pub struct NamedStream<S: StreamEffect>(std::marker::PhantomData<fn(S)>);

