/*
 * Copyright (C) 2023 Open Source Robotics Foundation
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
    LabelBuilder, DeliveryInstructions, RequestLabel, Chosen, private,
};

use bevy::ecs::system::EntityCommands;

/// This trait is used to put a label onto a request. After using
/// [`bevy::prelude::Commands`]`::request`, you can apply a label using the
/// [`crate::Chain`] that you receive:
///
/// ```text
/// commands
///     .request(request_data, provider)
///     .label(my_label) // This takes in an impl ApplyLabel
///     .detach();
/// ```
///
/// You can either pass in a [`RequestLabel`] struct directly or use
/// [`LabelBuilder`] to assign it queue/ensure qualities.
pub trait ApplyLabel: private::Sealed<()> {
    fn apply<'w, 's, 'a>(self, commands: &mut EntityCommands<'w, 's, 'a>);
}

impl<T: RequestLabel> ApplyLabel for T {
    fn apply<'w, 's, 'a>(self, commands: &mut EntityCommands<'w, 's, 'a>) {
        LabelBuilder::new(self).apply(commands)
    }
}
impl<T: RequestLabel> private::Sealed<()> for T { }

impl<Q, E> ApplyLabel for LabelBuilder<Q, E> {
    fn apply<'w, 's, 'a>(self, commands: &mut EntityCommands<'w, 's, 'a>) {
        commands.insert(DeliveryInstructions {
            label: self.label,
            queue: self.queue,
            ensure: self.ensure,
        });
    }
}
impl<Q, E> private::Sealed<()> for LabelBuilder<Q, E> { }

/// This trait gives a convenient way to convert a label into a [`LabelBuilder`]
/// which can add more specifications about how a labeled request should behave.
pub trait BuildLabel: private::Sealed<()> {
    fn queue(self) -> LabelBuilder<Chosen, ()>;
    fn ensure(self) -> LabelBuilder<(), Chosen>;
}

impl<T: RequestLabel> BuildLabel for T {
    /// Specify that the labeled request should queue itself instead of
    /// canceling prior requests with the same label.
    fn queue(self) -> LabelBuilder<Chosen, ()> {
        LabelBuilder::new(self).queue()
    }

    /// Specify that the labeled request should not allow itself to be canceled
    /// by later requests with the same label.
    fn ensure(self) -> LabelBuilder<(), Chosen> {
        LabelBuilder::new(self).ensure()
    }
}
