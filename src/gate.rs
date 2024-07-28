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

/// Indicate whether a buffer gate should open or close.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateAction {
    /// Open the buffer gate so that listeners (including [join][1] operations)
    /// can resume getting woken when the value in a buffer changes. They will
    /// receive a wakeup immediately when a gate switches from closed to open,
    /// even if none of the data inside the buffer has changed.
    ///
    /// [1]: crate::Bufferable::join
    Open,
    /// Close the buffer gate so that listeners (including [join][1] operations)
    /// will not be woken up when the data in the buffer gets modified. This
    /// effectively blocks the workflow nodes that are downstream of the buffer.
    /// Data will build up in the buffer according to its [`BufferSettings`][2].
    ///
    /// [1]: crate::Bufferable::join
    /// [2]: crate::BufferSettings
    Close,
}

impl GateAction {
    /// Is this action supposed to open a gate?
    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open)
    }

    /// Is this action supposed to close a gate?
    pub fn is_close(&self) -> bool {
        matches!(self, Self::Close)
    }
}

/// Pass this as input into a dynamic gate node. Dynamic gate nodes may open or
/// close a buffer gate based on what action you pass into it. The data will be
/// passed along as output from the dynamic gate node once the action is
/// complete. Dynamic gate nodes are created using [`create_gate`][1] or [`then_gate`][2].
///
/// If you know that you always want the gate to open or close at a certain
/// point in the workflow, then you can use static gate nodes instead using
/// [`create_gate_open`][3], [`create_gate_close`][4], [`then_gate_open`][5],
/// or [`then_gate_close`][6].
///
/// See [`GateAction`] to understand what hapens when a gate is open or closed.
///
/// [1]: crate::Builder::create_gate
/// [2]: crate::Chain::then_gate
/// [3]: crate::Builder::create_gate_open
/// [4]: crate::Builder::create_gate_close
/// [5]: crate::chain::then_gate_open
/// [6]: crate::chain::then_gate_close
pub struct GateRequest<T> {
    /// Indicate what action the gate should take
    pub action: GateAction,
    /// Indicate what data should be passed along after the gate action has
    /// completed.
    pub data: T,
}
