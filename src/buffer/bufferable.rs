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

use crate::{
    Buffer, CloneFromBuffer, Output, Builder, BufferSettings, Buffered, Join,
    UnusedTarget, AddOperation,
};

pub trait Bufferable {
    type BufferType: Buffered;

    /// Convert these bufferable workflow elements into buffers if they are not
    /// buffers already.
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType;

    /// Join these bufferable workflow elements.
    fn join(
        self,
        builder: &mut Builder,
    ) -> Output<<Self::BufferType as Buffered>::Item>
    where
        Self: Sized,
        Self::BufferType: 'static + Send + Sync,
        <Self::BufferType as Buffered>::Item: 'static + Send + Sync,
    {
        let buffers = self.as_buffer(builder);
        let join = builder.commands.spawn(()).id();
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.add(AddOperation::new(join, Join::new(buffers, target)));

        Output::new(builder.scope, target)
    }
}

impl<T: 'static + Send + Sync> Bufferable for Buffer<T> {
    type BufferType = Self;
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope, builder.scope());
        self
    }
}

impl<T: 'static + Send + Sync + Clone> Bufferable for CloneFromBuffer<T> {
    type BufferType = Self;
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope, builder.scope());
        self
    }
}

impl<T: 'static + Send + Sync> Bufferable for Output<T> {
    type BufferType = Buffer<T>;
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope(), builder.scope());
        let buffer = builder.create_buffer::<T>(BufferSettings::default());
        builder.connect(self, buffer.input_slot());
        buffer
    }
}

impl<T0, T1> Bufferable for (T0, T1)
where
    T0: Bufferable,
    T1: Bufferable,
{
    type BufferType = (T0::BufferType, T1::BufferType);
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        (
            self.0.as_buffer(builder),
            self.1.as_buffer(builder),
        )
    }
}

impl<T0, T1, T2> Bufferable for (T0, T1, T2)
where
    T0: Bufferable,
    T1: Bufferable,
    T2: Bufferable,
{
    type BufferType = (T0::BufferType, T1::BufferType, T2::BufferType);
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        (
            self.0.as_buffer(builder),
            self.1.as_buffer(builder),
            self.2.as_buffer(builder),
        )
    }
}


impl<T: Bufferable, const N: usize> Bufferable for [T; N] {
    type BufferType = [T::BufferType; N];
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        self.map(|b| b.as_buffer(builder))
    }
}
