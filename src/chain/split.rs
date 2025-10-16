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

use bevy_ecs::prelude::Entity;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
};
use thiserror::Error as ThisError;

use crate::{Builder, Chain, ConnectToSplit, OperationResult, Output, UnusedTarget};

/// Implementing this trait on a struct will allow the [`Chain::split`] operation
/// to be performed on [outputs][crate::Output] of that type.
pub trait Splittable: Sized {
    /// The key used to identify different elements in the split
    type Key: 'static + Send + Sync + Eq + Hash + Clone + Debug;

    /// An identifier that will be included along with the item in the messages
    /// that are produced by the split. In other words, each message in the split
    /// will be a tuple of (Identiifer, Item). You can then choose to map away
    /// the identifier if you don't need it.
    ///
    /// This may be different from the key, because a key can represent entire
    /// groups of items.
    type Identifier: 'static + Send + Sync;

    /// The type that the value gets split into
    type Item: 'static + Send + Sync;

    /// Return true if the key is feasible for this type of split, otherwise
    /// return false. Returning false will cause the user to receive a
    /// [`SplitConnectionError::KeyOutOfBounds`]. This will also cause iterating
    /// to cease.
    fn validate(key: &Self::Key) -> bool;

    /// Get the next key value that would follow the provided one. If [`None`]
    /// is passed in then return the first key. If you return [`None`] then
    /// the connections will stop iterating.
    fn next(key: &Option<Self::Key>) -> Option<Self::Key>;

    /// Split the value into its parts
    fn split(
        self,
        dispatcher: SplitDispatcher<'_, Self::Key, Self::Identifier, Self::Item>,
    ) -> OperationResult;
}

/// This is returned by [`Chain::split`] and allows you to connect to the
/// split pieces.
#[must_use]
pub struct SplitBuilder<'w, 's, 'a, 'b, T: Splittable> {
    outputs: SplitOutputs<T>,
    builder: &'b mut Builder<'w, 's, 'a>,
}

impl<'w, 's, 'a, 'b, T: Splittable> std::fmt::Debug for SplitBuilder<'w, 's, 'a, 'b, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitBuilder")
            .field("outputs", &self.outputs)
            .finish()
    }
}

impl<'w, 's, 'a, 'b, T: 'static + Splittable> SplitBuilder<'w, 's, 'a, 'b, T> {
    /// Get the state of connections for this split. You can resume building
    /// more connections later by calling [`SplitOutputs::build`] on the
    /// connections.
    pub fn outputs(self) -> SplitOutputs<T> {
        self.outputs
    }

    /// Unpack the split outputs from the builder so the two can be used
    /// independently.
    pub fn unpack(self) -> (SplitOutputs<T>, &'b mut Builder<'w, 's, 'a>) {
        (self.outputs, self.builder)
    }

    /// Build a branch for one of the keys in the split.
    pub fn chain_for<U>(
        &mut self,
        key: T::Key,
        f: impl FnOnce(Chain<(T::Identifier, T::Item)>) -> U,
    ) -> SplitChainResult<U> {
        let output = match self.output_for(key) {
            Ok(output) => output,
            Err(err) => return Err(err),
        };
        let u = f(self.builder.chain(output));
        Ok(u)
    }

    /// This is a convenience function for splits whose keys implement
    /// [`FromSpecific`] to build a branch for a specific key in the split. This
    /// can be used by [`MapSplitKey`].
    pub fn specific_chain<U>(
        &mut self,
        specific_key: <T::Key as FromSpecific>::SpecificKey,
        f: impl FnOnce(Chain<(T::Identifier, T::Item)>) -> U,
    ) -> SplitChainResult<U>
    where
        T::Key: FromSpecific,
    {
        self.chain_for(T::Key::from_specific(specific_key), f)
    }

    /// This is a convenience function for splits whose keys implement
    /// [`FromSequential`] to build a branch for an anonymous sequential key in
    /// the split. This can be used by [`ListSplitKey`] and [`MapSplitKey`].
    pub fn sequential_chain<U>(
        &mut self,
        sequence_number: usize,
        f: impl FnOnce(Chain<(T::Identifier, T::Item)>) -> U,
    ) -> SplitChainResult<U>
    where
        T::Key: FromSequential,
    {
        self.chain_for(T::Key::from_sequential(sequence_number), f)
    }

    /// This is a convenience function for splits whose keys implement
    /// [`ForRemaining`] to build a branch that takes in all items that did not
    /// have a more specific connection available. This can be used by
    /// [`ListSplitKey`] and [`MapSplitKey`].
    ///
    /// This can only be set once, so subsequent attempts to set the remaining
    /// branch will return an error. It will also return an error after
    /// [`Self::remaining_output`] has been used.
    pub fn remaining_chain<U>(
        &mut self,
        f: impl FnOnce(Chain<(T::Identifier, T::Item)>) -> U,
    ) -> SplitChainResult<U>
    where
        T::Key: ForRemaining,
    {
        self.chain_for(T::Key::for_remaining(), f)
    }

    /// Build a branch for the next key in the split, if such a key may
    /// be available. The first argument tells you what the key would be, but
    /// you can safely ignore this if it doesn't matter to you.
    ///
    /// This changes what the next element will be if you later use this
    /// [`SplitBuilder`] as an iterator.
    pub fn next_chain<U>(
        mut self,
        f: impl FnOnce(T::Key, Chain<(T::Identifier, T::Item)>) -> U,
    ) -> SplitChainResult<U> {
        let Some((key, output)) = self.next() else {
            return Err(SplitConnectionError::KeyOutOfBounds);
        };

        Ok(f(key, self.builder.chain(output)))
    }

    /// Build a branch for one of the keys in the split.
    pub fn branch_for(
        mut self,
        key: T::Key,
        f: impl FnOnce(Chain<(T::Identifier, T::Item)>),
    ) -> SplitBranchResult<'w, 's, 'a, 'b, T> {
        let output = match self.output_for(key) {
            Ok(output) => output,
            Err(err) => return Err((self, err)),
        };
        f(output.chain(self.builder));
        Ok(self)
    }

    /// This is a convenience function for splits whose keys implement
    /// [`FromSpecific`] to build a branch for a specific key in the split. This
    /// can be used by [`MapSplitKey`].
    pub fn specific_branch(
        self,
        specific_key: <T::Key as FromSpecific>::SpecificKey,
        f: impl FnOnce(Chain<(T::Identifier, T::Item)>),
    ) -> SplitBranchResult<'w, 's, 'a, 'b, T>
    where
        T::Key: FromSpecific,
    {
        self.branch_for(T::Key::from_specific(specific_key), f)
    }

    /// This is a convenience function for splits whose keys implement
    /// [`FromSequential`] to build a branch for an anonymous sequential key in
    /// the split. This can be used by [`ListSplitKey`] and [`MapSplitKey`].
    pub fn sequential_branch(
        self,
        sequence_number: usize,
        f: impl FnOnce(Chain<(T::Identifier, T::Item)>),
    ) -> SplitBranchResult<'w, 's, 'a, 'b, T>
    where
        T::Key: FromSequential,
    {
        self.branch_for(T::Key::from_sequential(sequence_number), f)
    }

    /// This is a convenience function for splits whose keys implement
    /// [`ForRemaining`] to build a branch that takes in all items that did not
    /// have a more specific connection available. This can be used by
    /// [`ListSplitKey`] and [`MapSplitKey`].
    ///
    /// This can only be set once, so subsequent attempts to set the remaining
    /// branch will return an error. It will also return an error after
    /// [`Self::remaining_output`] has been used.
    pub fn remaining_branch(
        self,
        f: impl FnOnce(Chain<(T::Identifier, T::Item)>),
    ) -> SplitBranchResult<'w, 's, 'a, 'b, T>
    where
        T::Key: ForRemaining,
    {
        self.branch_for(T::Key::for_remaining(), f)
    }

    /// Build a branch for the next key in the split, if such a key may
    /// be available. The first argument tells you what the key would be, but
    /// you can safely ignore this if it doesn't matter to you.
    ///
    /// This changes what the next element will be if you later use this
    /// [`SplitBuilder`] as an iterator.
    pub fn next_branch(
        mut self,
        f: impl FnOnce(T::Key, Chain<(T::Identifier, T::Item)>),
    ) -> SplitBranchResult<'w, 's, 'a, 'b, T> {
        let Some((key, output)) = self.next() else {
            return Err((self, SplitConnectionError::KeyOutOfBounds));
        };

        f(key, output.chain(self.builder));
        Ok(self)
    }

    /// Get the output slot for an element in the split.
    pub fn output_for(
        &mut self,
        key: T::Key,
    ) -> Result<Output<(T::Identifier, T::Item)>, SplitConnectionError> {
        if !T::validate(&key) {
            return Err(SplitConnectionError::KeyOutOfBounds);
        }

        if !self.outputs.used.insert(key.clone()) {
            return Err(SplitConnectionError::KeyAlreadyUsed);
        }

        let target = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.commands.queue(ConnectToSplit::<T> {
            source: self.outputs.source,
            target,
            key,
        });
        Ok(Output::new(self.outputs.scope, target))
    }

    /// This is a convenience function for splits whose keys implement
    /// [`FromSpecific`] to get the output for a specific key in the split. This
    /// can be used by [`MapSplitKey`].
    pub fn specific_output(
        &mut self,
        specific_key: <T::Key as FromSpecific>::SpecificKey,
    ) -> Result<Output<(T::Identifier, T::Item)>, SplitConnectionError>
    where
        T::Key: FromSpecific,
    {
        self.output_for(T::Key::from_specific(specific_key))
    }

    /// This is a convenience function for splits whose keys implement
    /// [`FromSequential`] to get the output for an anonymous sequential key in
    /// the split. This can be used by [`ListSplitKey`] and [`MapSplitKey`].
    pub fn sequential_output(
        &mut self,
        sequence_number: usize,
    ) -> Result<Output<(T::Identifier, T::Item)>, SplitConnectionError>
    where
        T::Key: FromSequential,
    {
        self.output_for(T::Key::from_sequential(sequence_number))
    }

    /// This is a convenience function for splits whose keys implement
    /// [`ForRemaining`] to get the output for all keys remaining without a
    /// connection after all the more specific connections have been considered.
    /// This can be used by [`ListSplitKey`] and [`MapSplitKey`].
    ///
    /// This can only be used once, after which it will return an error. It will
    /// also return an error after [`Self::remaining_branch`] has been used.
    pub fn remaining_output(
        &mut self,
    ) -> Result<Output<(T::Identifier, T::Item)>, SplitConnectionError>
    where
        T::Key: ForRemaining,
    {
        self.output_for(T::Key::for_remaining())
    }

    /// Explicitly stop building the split by indicating that you it to remain
    /// unused.
    pub fn unused(self) {
        // Do nothing
    }

    /// Used internally to create a new split connector
    pub(crate) fn new(source: Entity, builder: &'b mut Builder<'w, 's, 'a>) -> Self {
        Self {
            outputs: SplitOutputs::new(builder.scope(), source),
            builder,
        }
    }
}

impl<'w, 's, 'a, 'b, T: 'static + Splittable> Iterator for SplitBuilder<'w, 's, 'a, 'b, T> {
    type Item = (T::Key, Output<(T::Identifier, T::Item)>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next_key = T::next(&self.outputs.last_key)?;
            self.outputs.last_key = Some(next_key.clone());

            match self.output_for(next_key.clone()) {
                Ok(output) => {
                    return Some((next_key, output));
                }
                Err(SplitConnectionError::KeyAlreadyUsed) => {
                    // Restart the loop and get the next key which might not be
                    // used yet.
                    continue;
                }
                Err(SplitConnectionError::KeyOutOfBounds) => {
                    // We have reached the end of the valid range so just quit
                    // iterating.
                    return None;
                }
            }
        }
    }
}

/// This tracks the connections that have been made to a split. This can be
/// retrieved from [`SplitBuilder`] by calling [`SplitBuilder::outputs`].
/// You can then continue building connections by calling [`SplitOutputs::build`].
#[must_use]
pub struct SplitOutputs<T: Splittable> {
    scope: Entity,
    source: Entity,
    last_key: Option<T::Key>,
    used: HashSet<T::Key>,
}

impl<T: Splittable> std::fmt::Debug for SplitOutputs<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("SplitOutputs<{}>", std::any::type_name::<T>()))
            .field("scope", &self.scope)
            .field("source", &self.source)
            .field("last_key", &self.last_key)
            .field("used", &self.used)
            .finish()
    }
}

impl<T: Splittable> SplitOutputs<T> {
    /// Resume building connections for this split.
    pub fn build<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> SplitBuilder<'w, 's, 'a, 'b, T> {
        assert_eq!(self.scope, builder.scope());
        SplitBuilder {
            outputs: self,
            builder,
        }
    }

    pub(crate) fn new(scope: Entity, source: Entity) -> Self {
        Self {
            scope,
            source,
            last_key: None,
            used: Default::default(),
        }
    }
}

/// This is a type alias for the result returned by the branching [`SplitBuilder`]
/// functions. If the last connection succeeded, you will receive [`Ok`] with
/// the [`SplitBuilder`] which you can keep building off of. Otherwise if the
/// last connection failed, you will receive an [`Err`] with the [`SplitBuilder`]
/// bundled with [`SplitConnectionError`] to tell you what went wrong. You can
/// continue building with the [`SplitBuilder`] even if an error occurred.
///
/// You can use `ignore_result` from the [`IgnoreSplitChainResult`] trait to
/// just keep chaining without checking whether the connection suceeded.
pub type SplitBranchResult<'w, 's, 'a, 'b, T> = Result<
    SplitBuilder<'w, 's, 'a, 'b, T>,
    (SplitBuilder<'w, 's, 'a, 'b, T>, SplitConnectionError),
>;

/// This is a type alias for the chain-building methods in [`SplitBuilder`].
/// It will either return the output of the chain building function or an error.
pub type SplitChainResult<U> = Result<U, SplitConnectionError>;

/// A helper trait that allows users to ignore any failures while chaining
/// connections to a split.
pub trait IgnoreSplitChainResult<'w, 's, 'a, 'b, T: Splittable> {
    /// Ignore whether the result of the chaining connection was [`Ok`] or [`Err`]
    /// and just keep chaining.
    fn ignore_result(self) -> SplitBuilder<'w, 's, 'a, 'b, T>;
}

impl<'w, 's, 'a, 'b, T: Splittable> IgnoreSplitChainResult<'w, 's, 'a, 'b, T>
    for SplitBranchResult<'w, 's, 'a, 'b, T>
{
    fn ignore_result(self) -> SplitBuilder<'w, 's, 'a, 'b, T> {
        match self {
            Ok(split) => split,
            Err((split, _)) => split,
        }
    }
}

/// Information about why a connection to a split failed
#[derive(ThisError, Debug, Clone)]
#[error("An error occurred while trying to connect to a split")]
pub enum SplitConnectionError {
    /// The requested index was already connected to.
    KeyAlreadyUsed,
    /// The requested index is out of bounds.
    KeyOutOfBounds,
}

/// Used by implementers of the [`Splittable`] trait to help them send their
/// split values to the proper input slots.
pub struct SplitDispatcher<'a, Key, Identifier, Item> {
    pub(crate) connections: &'a HashMap<Key, usize>,
    pub(crate) outputs: &'a mut Vec<Vec<(Identifier, Item)>>,
}

impl<'a, Key, Identifier, Item> SplitDispatcher<'a, Key, Identifier, Item>
where
    Key: 'static + Send + Sync + Eq + Hash + Clone + Debug,
    Identifier: 'static + Send + Sync,
    Item: 'static + Send + Sync,
{
    /// Get the output buffer a certain key. If there are no connections for the
    /// given key, then this will return [`None`].
    ///
    /// Push items into the output buffer to send them to the input connected to
    /// this key.
    pub fn outputs_for<'o>(&'o mut self, key: &Key) -> Option<&'o mut Vec<(Identifier, Item)>> {
        let index = *self.connections.get(key)?;

        if self.outputs.len() <= index {
            // We do this just in case something bad happened with the cache
            // that reset its size.
            self.outputs.resize_with(index + 1, Vec::new);
        }

        self.outputs.get_mut(index)
    }
}

/// Turn a sequence index into a split key. Implemented by [`ListSplitKey`] and
/// [`MapSplitKey`].
pub trait FromSequential {
    /// Convert the sequence index into the split key type.
    fn from_sequential(seq: usize) -> Self;
}

/// Get the key that represents all remaining/unspecified keys. Implemented by
/// [`ListSplitKey`] and [`MapSplitKey`].
pub trait ForRemaining {
    /// Get the key for remaining items.
    fn for_remaining() -> Self;
}

/// Turn a specific key into a split key. Implemented by [`MapSplitKey`].
pub trait FromSpecific {
    /// The specific key type
    type SpecificKey;

    /// Convert the specific key into the split key type
    fn from_specific(specific: Self::SpecificKey) -> Self;
}

/// This enum allows users to key into splittable list-like structures based on
/// the sequence in which an item appears in the list. It also has an option for
/// keying into any items that were left over in the sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ListSplitKey {
    /// Key into an item at a specific point in the sequence.
    Sequential(usize),
    /// Key into any remaining items that were not covered by the sequential keys.
    Remaining,
}

impl FromSequential for ListSplitKey {
    fn from_sequential(seq: usize) -> Self {
        ListSplitKey::Sequential(seq)
    }
}

impl ForRemaining for ListSplitKey {
    fn for_remaining() -> Self {
        ListSplitKey::Remaining
    }
}

/// This is a newtype that implements [`Splittable`] for anything that can be
/// turned into an iterator, but always splits it as if the iterator is list-like.
///
/// If used with a map-type (e.g. [`HashMap`] or [BTreeMap]), the items will be
/// the `(key, value)` pairs of the maps, and the key for the split will be the
/// order in which the map is iterated through. For [`BTreeMap`] this will always
/// be a sorted order (e.g. alphabetical or numerical) but for [`HashMap`] this
/// order can be completely arbitrary and may be unstable.
pub struct SplitAsList<T: 'static + Send + Sync + IntoIterator> {
    pub contents: T,
}

impl<T: 'static + Send + Sync + IntoIterator> SplitAsList<T> {
    pub fn new(contents: T) -> Self {
        Self { contents }
    }
}

impl<T> Splittable for SplitAsList<T>
where
    T: 'static + Send + Sync + IntoIterator,
    T::Item: 'static + Send + Sync,
{
    type Key = ListSplitKey;
    type Identifier = usize;
    type Item = T::Item;

    fn validate(_: &Self::Key) -> bool {
        // We don't know if there are any restrictions for the iterable, so just
        // return say all keys are valid
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        if let Some(key) = key {
            match key {
                ListSplitKey::Sequential(k) => Some(ListSplitKey::Sequential(*k + 1)),
                ListSplitKey::Remaining => None,
            }
        } else {
            Some(ListSplitKey::Sequential(0))
        }
    }

    fn split(
        self,
        mut dispatcher: SplitDispatcher<'_, Self::Key, Self::Identifier, Self::Item>,
    ) -> OperationResult {
        for (index, value) in self.contents.into_iter().enumerate() {
            match dispatcher.outputs_for(&ListSplitKey::Sequential(index)) {
                Some(outputs) => {
                    outputs.push((index, value));
                }
                None => {
                    if let Some(outputs) = dispatcher.outputs_for(&ListSplitKey::Remaining) {
                        outputs.push((index, value));
                    }
                }
            }
        }

        Ok(())
    }
}

impl<T: 'static + Send + Sync> Splittable for Vec<T> {
    type Key = ListSplitKey;
    type Identifier = usize;
    type Item = T;

    fn validate(_: &Self::Key) -> bool {
        // Vec has no restrictions on what index is valid
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        SplitAsList::<Self>::next(key)
    }

    fn split(
        self,
        dispatcher: SplitDispatcher<'_, Self::Key, Self::Identifier, Self::Item>,
    ) -> OperationResult {
        SplitAsList::new(self).split(dispatcher)
    }
}

impl<T: 'static + Send + Sync, const N: usize> Splittable for smallvec::SmallVec<[T; N]> {
    type Key = ListSplitKey;
    type Identifier = usize;
    type Item = T;

    fn validate(_: &Self::Key) -> bool {
        // SmallVec has no restrictions on what index is valid
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        SplitAsList::<Self>::next(key)
    }

    fn split(
        self,
        dispatcher: SplitDispatcher<'_, Self::Key, Self::Identifier, Self::Item>,
    ) -> OperationResult {
        SplitAsList::new(self).split(dispatcher)
    }
}

impl<T: 'static + Send + Sync, const N: usize> Splittable for [T; N] {
    type Key = ListSplitKey;
    type Identifier = usize;
    type Item = T;

    fn validate(key: &Self::Key) -> bool {
        // Static arrays have a firm limit of N
        match key {
            ListSplitKey::Sequential(s) => *s < N,
            ListSplitKey::Remaining => true,
        }
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        // Static arrays have a firm limit of N
        let mut key = SplitAsList::<Self>::next(key);
        if key.map_or(false, |key| Self::validate(&key)) {
            key.take()
        } else {
            None
        }
    }

    fn split(
        self,
        dispatcher: SplitDispatcher<'_, Self::Key, Self::Identifier, Self::Item>,
    ) -> OperationResult {
        SplitAsList::new(self).split(dispatcher)
    }
}

/// This enum allows users to key into splittable map-like structures based on
/// the presence of a specific value or based on the sequence in which a value
/// is reached that wasn't associated with a specific key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MapSplitKey<K> {
    /// Key into the item associated with this specific key.
    Specific(K),
    /// Key into an anonymous sequential item. Anonymous items are items for
    /// which no specific connection was made to their key.
    Sequential(usize),
    /// Key into any items that were not covered by one of the other types.
    Remaining,
}

impl<K> MapSplitKey<K> {
    pub fn specific(self) -> Option<K> {
        match self {
            MapSplitKey::Specific(key) => Some(key),
            _ => None,
        }
    }

    pub fn next(this: &Option<Self>) -> Option<Self> {
        match this {
            Some(key) => {
                match key {
                    // Give the next key in the sequence
                    MapSplitKey::Sequential(index) => Some(MapSplitKey::Sequential(index + 1)),
                    // For an arbitrary map we don't know what would follow a specific key, so
                    // just stop iterating. This should never be reached in practice anyway.
                    MapSplitKey::Specific(_) => None,
                    MapSplitKey::Remaining => None,
                }
            }
            None => Some(MapSplitKey::Sequential(0)),
        }
    }
}

impl<K> From<K> for MapSplitKey<K> {
    fn from(value: K) -> Self {
        MapSplitKey::Specific(value)
    }
}

impl<K> FromSpecific for MapSplitKey<K> {
    type SpecificKey = K;
    fn from_specific(specific: Self::SpecificKey) -> Self {
        Self::Specific(specific)
    }
}

impl<K> FromSequential for MapSplitKey<K> {
    fn from_sequential(seq: usize) -> Self {
        Self::Sequential(seq)
    }
}

impl<K> ForRemaining for MapSplitKey<K> {
    fn for_remaining() -> Self {
        Self::Remaining
    }
}

/// This is a newtype that implements [`Splittable`] for anything that can be
/// turned into an iterator whose items take the form of a `(key, value)` pair
/// where `key` meets all the bounds needed for a [`Splittable`] key.
///
/// This is used to implement [`Splittable`] for map-like structures.
pub struct SplitAsMap<K, V, M>
where
    K: 'static + Send + Sync + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync,
    M: 'static + Send + Sync + IntoIterator<Item = (K, V)>,
{
    pub contents: M,
    _ignore: std::marker::PhantomData<(K, V)>,
}

impl<K, V, M> SplitAsMap<K, V, M>
where
    K: 'static + Send + Sync + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync,
    M: 'static + Send + Sync + IntoIterator<Item = (K, V)>,
{
    pub fn new(contents: M) -> Self {
        Self {
            contents,
            _ignore: Default::default(),
        }
    }
}

impl<K, V, M> Splittable for SplitAsMap<K, V, M>
where
    K: 'static + Send + Sync + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync,
    M: 'static + Send + Sync + IntoIterator<Item = (K, V)>,
{
    type Key = MapSplitKey<K>;
    type Identifier = K;
    type Item = V;

    fn validate(_: &Self::Key) -> bool {
        // We have no way of knowing what the key bounds are for an arbitrary map
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        MapSplitKey::next(key)
    }

    fn split(
        self,
        mut dispatcher: SplitDispatcher<'_, Self::Key, Self::Identifier, Self::Item>,
    ) -> OperationResult {
        let mut next_seq = 0;
        for (specific_key, value) in self.contents.into_iter() {
            let key = MapSplitKey::Specific(specific_key);
            match dispatcher.outputs_for(&key) {
                Some(outputs) => {
                    outputs.push((key.specific().unwrap(), value));
                }
                None => {
                    // No connection to the specific key, so let's check for a
                    // sequential connection.
                    let seq = MapSplitKey::Sequential(next_seq);
                    next_seq += 1;
                    match dispatcher.outputs_for(&seq) {
                        Some(outputs) => {
                            outputs.push((key.specific().unwrap(), value));
                        }
                        None => {
                            // No connection to this point in the sequence, so
                            // let's send it to any remaining connection.
                            let remaining = MapSplitKey::Remaining;
                            if let Some(outputs) = dispatcher.outputs_for(&remaining) {
                                outputs.push((key.specific().unwrap(), value));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl<K, V> Splittable for HashMap<K, V>
where
    K: 'static + Send + Sync + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync,
{
    type Key = MapSplitKey<K>;
    type Identifier = K;
    type Item = V;

    fn validate(_: &Self::Key) -> bool {
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        SplitAsMap::<K, V, Self>::next(key)
    }

    fn split(
        self,
        dispatcher: SplitDispatcher<'_, Self::Key, Self::Identifier, Self::Item>,
    ) -> OperationResult {
        SplitAsMap::new(self).split(dispatcher)
    }
}

impl<K, V> Splittable for BTreeMap<K, V>
where
    K: 'static + Send + Sync + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync,
{
    type Key = MapSplitKey<K>;
    type Identifier = K;
    type Item = V;

    fn validate(_: &Self::Key) -> bool {
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        SplitAsMap::<K, V, Self>::next(key)
    }

    fn split(
        self,
        dispatcher: SplitDispatcher<'_, Self::Key, Self::Identifier, Self::Item>,
    ) -> OperationResult {
        SplitAsMap::new(self).split(dispatcher)
    }
}

#[cfg(test)]
mod tests {
    use crate::{testing::*, *};
    use std::collections::{BTreeMap, HashMap};

    #[test]
    fn test_split_array() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope
                .input
                .chain(builder)
                .split(|split| {
                    let mut outputs = Vec::new();
                    split
                        .sequential_branch(0, |chain| {
                            outputs.push(chain.value().map_block(|v| v + 0.0).output());
                        })
                        .unwrap()
                        .sequential_branch(2, |chain| {
                            outputs
                                .push(chain.value().map_async(|v| async move { v + 2.0 }).output());
                        })
                        .unwrap()
                        .sequential_branch(4, |chain| {
                            outputs.push(chain.value().map_block(|v| v + 4.0).output());
                        })
                        .unwrap()
                        .unused();

                    outputs
                })
                .join_vec::<5>(builder)
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
                .request([5.0, 4.0, 3.0, 2.0, 1.0], workflow)
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(30));
        assert!(context.no_unhandled_errors());
        let value = promise.take().available().unwrap();
        assert_eq!(value, [5.0, 5.0, 5.0].into());

        let workflow = context.spawn_io_workflow(|scope: Scope<[f64; 3], f64>, builder| {
            scope.input.chain(builder).split(|split| {
                let split = split
                    .sequential_branch(0, |chain| {
                        chain
                            // Do some nonsense with the first element in the split
                            .fork_clone((
                                |chain: Chain<_>| chain.unused(),
                                |chain: Chain<_>| chain.unused(),
                                |chain: Chain<_>| chain.unused(),
                            ));
                    })
                    .unwrap();

                // This is outside the valid range for the array, so it should
                // fail
                let err = split.sequential_branch(3, |chain| {
                    chain.value().connect(scope.terminate);
                });
                assert!(matches!(
                    &err,
                    Err((_, SplitConnectionError::KeyOutOfBounds))
                ));

                let split = err
                    .ignore_result()
                    .sequential_branch(1, |chain| {
                        chain.unused();
                    })
                    .unwrap();

                // We already connected to this key, so it should fail
                let err = split.sequential_branch(0, |chain| {
                    chain.value().connect(scope.terminate);
                });
                assert!(matches!(
                    &err,
                    Err((_, SplitConnectionError::KeyAlreadyUsed))
                ));

                // Connect the last element in the split to the termination node
                err.ignore_result()
                    .sequential_branch(2, |chain| {
                        chain.value().connect(scope.terminate);
                    })
                    .unwrap()
                    .unused();
            });
        });

        let mut promise =
            context.command(|commands| commands.request([1.0, 2.0, 3.0], workflow).take_response());

        context.run_with_conditions(&mut promise, 1);
        assert!(context.no_unhandled_errors());
        // Only the third element in the split gets connected to the workflow
        // termination, the rest are discarded. This ensures that SplitBuilder
        // connections still work after multiple failed connection attempts.
        assert_eq!(promise.take().available().unwrap(), 3.0);
    }

    #[test]
    fn test_split_map() {
        let mut context = TestingContext::minimal_plugins();

        let km_to_miles = 0.621371;
        let per_second_to_per_hour = 3600.0;
        let convert_speed = move |v: f64| v * km_to_miles * per_second_to_per_hour;
        let convert_distance = move |d: f64| d * km_to_miles;

        let workflow =
            context.spawn_io_workflow(|scope: Scope<BTreeMap<String, f64>, _>, builder| {
                let collector = builder.create_collect_all::<_, 16>();

                scope.input.chain(builder).split(|split| {
                    split
                        .specific_branch("speed".to_owned(), |chain| {
                            chain
                                .map_block(move |(k, v)| (k, convert_speed(v)))
                                .connect(collector.input);
                        })
                        .ignore_result()
                        .specific_branch("velocity".to_owned(), |chain| {
                            chain
                                .map_async(move |(k, v)| async move { (k, convert_speed(v)) })
                                .connect(collector.input);
                        })
                        .unwrap()
                        .specific_branch("distance".to_owned(), |chain| {
                            chain
                                .map_block(move |(k, v)| (k, convert_distance(v)))
                                .connect(collector.input);
                        })
                        .unwrap()
                        .sequential_branch(0, |chain| {
                            chain
                                .map_block(move |(k, v)| (k, 0.0 * v))
                                .connect(collector.input);
                        })
                        .unwrap()
                        .sequential_branch(1, |chain| {
                            chain
                                .map_async(move |(k, v)| async move { (k, 1.0 * v) })
                                .connect(collector.input);
                        })
                        .unwrap()
                        .sequential_branch(2, |chain| {
                            chain
                                .map_block(move |(k, v)| (k, 2.0 * v))
                                .connect(collector.input);
                        })
                        .unwrap()
                        .remaining_branch(|chain| {
                            chain.connect(collector.input);
                        })
                        .unwrap()
                        .unused();
                });

                collector
                    .output
                    .chain(builder)
                    .map_block(|v| HashMap::<String, f64>::from_iter(v))
                    .connect(scope.terminate);
            });

        // We input a BTreeMap so we can ensure the first three sequence items
        // are always the same: a, b, and c. Make sure that no other keys in the
        // map come before c alphabetically.
        let input_map: BTreeMap<String, f64> = [
            ("a", 3.14159),
            ("b", 2.71828),
            ("c", 4.0),
            ("speed", 16.1),
            ("velocity", -32.4),
            ("distance", 4325.78),
            ("foo", 42.0),
            ("fib", 78.3),
            ("dib", -22.1),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect();

        let mut promise = context.command(|commands| {
            commands
                .request(input_map.clone(), workflow)
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(30));
        assert!(context.no_unhandled_errors());

        let result = promise.take().available().unwrap();
        assert_eq!(result.len(), input_map.len());
        assert_eq!(result["a"], input_map["a"] * 0.0);
        assert_eq!(result["b"], input_map["b"] * 1.0);
        assert_eq!(result["c"], input_map["c"] * 2.0);
        assert_eq!(result["speed"], convert_speed(input_map["speed"]));
        assert_eq!(result["velocity"], convert_speed(input_map["velocity"]));
        assert_eq!(result["distance"], convert_distance(input_map["distance"]));
        assert_eq!(result["foo"], input_map["foo"]);
        assert_eq!(result["fib"], input_map["fib"]);
        assert_eq!(result["dib"], input_map["dib"]);
    }

    #[test]
    fn test_array_split_limit() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder).split(|split| {
                let err = split
                    .next_branch(|_, chain| {
                        chain.value().connect(scope.terminate);
                    })
                    .unwrap()
                    .next_branch(|_, chain| {
                        chain.value().connect(scope.terminate);
                    })
                    .unwrap()
                    .next_branch(|_, chain| {
                        chain.value().connect(scope.terminate);
                    })
                    .unwrap()
                    .next_branch(|_, chain| {
                        chain.value().connect(scope.terminate);
                    })
                    .unwrap()
                    // This last one should fail because it should exceed the
                    // array limit
                    .next_branch(|_, chain| {
                        chain.value().connect(scope.terminate);
                    });

                assert!(matches!(err, Err(_)));
            })
        });

        let mut promise =
            context.command(|commands| commands.request([1, 2, 3, 4], workflow).take_response());

        context.run_with_conditions(&mut promise, 1);
        assert!(context.no_unhandled_errors());

        let result = promise.take().available().unwrap();
        // All the values in the array are racing to finish, but the first value
        // should finish first since it will naturally get queued first.
        assert_eq!(result, 1);
    }
}
