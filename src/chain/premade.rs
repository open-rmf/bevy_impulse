/*
 * Copyright (C) 2024 Open Source Robotics Foundation
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

use bevy_ecs::{prelude::In, query::QueryEntityError};

use smallvec::SmallVec;

use crate::{BufferAccessMut, BufferKey};

pub(super) fn consume_buffer<const N: usize, T>(
    In(key): In<BufferKey<T>>,
    mut access: BufferAccessMut<T>,
) -> SmallVec<[T; N]>
where
    T: 'static + Send + Sync,
{
    let Ok(mut buffer) = access.get_mut(&key) else {
        return SmallVec::new();
    };

    buffer.drain(..).collect()
}

pub fn push_into_buffer<T: 'static + Send + Sync>(
    In((input, key)): In<(T, BufferKey<T>)>,
    mut access: BufferAccessMut<T>,
) -> Result<(), QueryEntityError> {
    access.get_mut(&key)?.push(input);
    Ok(())
}
