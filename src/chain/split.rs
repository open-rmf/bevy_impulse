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

use thiserror::Error as ThisError;
use std::{hash::Hash, fmt::Debug, collections::{HashSet, HashMap, BTreeMap}};
use bevy_ecs::prelude::Entity;

use crate::{
    Builder, Chain, ConnectToSplit, Output, UnusedTarget, OperationResult,
};

/// Implementing this trait on a struct will allow the [`Chain::split`] operation
/// to be performed on [outputs][crate::Output] of that type.
pub trait Splittable: Sized {
    /// The key used to identify different elements in the split
    type Key: 'static + Send + Sync + Eq + Hash + Clone + Debug;

    /// The type that the value gets split into
    type Item: 'static + Send + Sync;

    /// Return true if the key is feasible for this type of split, otherwise
    /// return false. Returning false will cause the user to receive a
    /// [`SplitConnectionError::IndexOutOfBounds`]. This will also cause iterating
    /// to cease.
    fn validate(key: &Self::Key) -> bool;

    /// Get the next key value that would follow the provided one. If [`None`]
    /// is passed in then return the first key. If you return [`None`] then
    /// the connections will stop iterating.
    fn next(key: &Option<Self::Key>) -> Option<Self::Key>;

    /// Split the value into its parts
    fn split(self, dispatcher: SplitDispatcher<'_, Self::Key, Self::Item>) -> OperationResult;
}

/// This is returned by [`Chain::split`] and allows you to connect to the
/// split pieces.
#[must_use]
pub struct SplitConnector<'w, 's, 'a, 'b, T: Splittable> {
    connections: SplitConnections<T>,
    builder: &'b mut Builder<'w, 's, 'a>,
}

impl<'w, 's, 'a, 'b, T: 'static + Splittable> SplitConnector<'w, 's, 'a, 'b, T> {
    /// Get the state of connections for this split. You can resume building
    /// more connections later by calling [`SplitConnections::build`] on the
    /// connections.
    pub fn connections(self) -> SplitConnections<T> {
        self.connections
    }

    /// Build a branch for one of the elements in the split.
    pub fn branch(
        mut self,
        key: impl Into<T::Key>,
        f: impl FnOnce(Chain<T::Item>),
    ) -> Result<SplitConnector<'w, 's, 'a, 'b, T>, SplitConnectionError> {
        let output = self.output(key)?;
        f(output.chain(self.builder));
        Ok(self)
    }

    /// Build a branch for the next element in the split, if such an element may
    /// be available. The second argument tells you what the key would be, but
    /// you can safely ignore this if it doesn't matter to you.
    ///
    /// This changes what the next element will be if you later use this
    /// [`SplitConnector`] as an iterator.
    pub fn next_branch(
        mut self,
        f: impl FnOnce(Chain<T::Item>, T::Key),
    ) -> Result<SplitConnector<'w, 's, 'a, 'b, T>, SplitConnectionError> {
        let Some((key, output)) = self.next() else {
            return Err(SplitConnectionError::KeyOutOfBounds);
        };

        f(output.chain(self.builder), key);
        Ok(self)
    }

    /// Get the output slot for an element in the split.
    pub fn output(
        &mut self,
        key: impl Into<T::Key>,
    ) -> Result<Output<T::Item>, SplitConnectionError> {
        let key: T::Key = key.into();
        if !T::validate(&key) {
            return Err(SplitConnectionError::KeyOutOfBounds);
        }

        if !self.connections.used.insert(key.clone()) {
            return Err(SplitConnectionError::KeyAlreadyUsed);
        }

        let target = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.commands.add(ConnectToSplit::<T> {
            source: self.connections.source,
            target,
            key
        });
        Ok(Output::new(self.builder.scope, target))
    }

    /// Used internally to create a new split connector
    pub(crate) fn new(
        source: Entity,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Self {
        Self {
            connections: SplitConnections {
                source,
                last_key: None,
                used: Default::default(),
            },
            builder,
        }
    }
}

impl<'w, 's, 'a, 'b, T, K> SplitConnector<'w, 's, 'a, 'b, T>
where
    T: 'static + Splittable<Key = MapSplitKey<K>>,
{
    /// This is a convenience function for map-like splits that use [`MapSplitKey`]
    /// to build a branch for an anonymous sequential element in the split.
    pub fn sequential_branch(
        self,
        sequence_number: usize,
        f: impl FnOnce(Chain<T::Item>),
    ) -> Result<SplitConnector<'w, 's, 'a, 'b, T>, SplitConnectionError> {
        self.branch(MapSplitKey::Sequential(sequence_number), f)
    }
}

impl<'w, 's, 'a, 'b, T: 'static + Splittable> Iterator for SplitConnector<'w, 's, 'a, 'b, T> {
    type Item = (T::Key, Output<T::Item>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next_key = T::next(&self.connections.last_key)?;
            self.connections.last_key = Some(next_key.clone());

            match self.output(next_key.clone()) {
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
/// retrieved from [`SplitConnector`] by calling [`SplitConnector::connections`].
/// You can then continue building connections by calling [`SplitConnector::build`].
#[must_use]
pub struct SplitConnections<T: Splittable> {
    source: Entity,
    last_key: Option<T::Key>,
    used: HashSet<T::Key>,
}

impl<T: Splittable> SplitConnections<T> {
    /// Resume building connections for this split.
    pub fn build<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> SplitConnector<'w, 's, 'a, 'b, T> {
        SplitConnector { connections: self, builder }
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
pub struct SplitDispatcher<'a, Key, Item> {
    pub(crate) connections: &'a HashMap<Key, usize>,
    pub(crate) outputs: &'a mut Vec<Vec<Item>>,
}

impl<'a, Key, Item> SplitDispatcher<'a, Key, Item>
where
    Key: 'static + Send + Sync + Eq + Hash + Clone + Debug,
    Item: 'static + Send + Sync,
{
    /// Send a value for a key. The split operation will make sure that this
    /// value will be sent to the target associated with the key.
    ///
    /// If there is no connection associated with the specified key, the value
    /// will be returned as [`Err`].
    pub fn send(
        &mut self,
        key: &Key,
        value: Item,
    ) -> Result<(), Item> {
        let Some(index) = self.connections.get(key) else {
            return Err(value);
        };
        let index = *index;

        if self.outputs.len() <= index {
            // We do this just in case something bad happened with the cache
            // that reset its size.
            self.outputs.resize_with(index+1, || Vec::new());
        }

        self.outputs[index].push(value);

        Ok(())
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
    pub contents: T
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
    type Key = usize;
    type Item = T::Item;

    fn validate(_: &Self::Key) -> bool {
        // We don't know if there are any restrictions for the iterable, so just
        // return say all keys are valid
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        if let Some(key) = key {
            Some(*key + 1)
        } else {
            Some(0)
        }
    }

    fn split(self, mut dispatcher: SplitDispatcher<'_, Self::Key, Self::Item>) -> OperationResult {
        for (index, value) in self.contents.into_iter().enumerate() {
            dispatcher.send(&index, value).ok();
        }

        Ok(())
    }
}

impl<T: 'static + Send + Sync> Splittable for Vec<T> {
    type Key = usize;
    type Item = T;

    fn validate(_: &Self::Key) -> bool {
        // Vec has no restrictions on what index is valid
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        SplitAsList::<Self>::next(key)
    }

    fn split(self, dispatcher: SplitDispatcher<'_, Self::Key, Self::Item>) -> OperationResult {
        SplitAsList::new(self).split(dispatcher)
    }
}

impl<T: 'static + Send + Sync, const N: usize> Splittable for smallvec::SmallVec<[T; N]> {
    type Key = usize;
    type Item = T;

    fn validate(_: &Self::Key) -> bool {
        // SmallVec has no restrictions on what index is valid
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        SplitAsList::<Self>::next(key)
    }

    fn split(self, dispatcher: SplitDispatcher<'_, Self::Key, Self::Item>) -> OperationResult {
        SplitAsList::new(self).split(dispatcher)
    }
}

impl<T: 'static + Send + Sync, const N: usize> Splittable for [T; N] {
    type Key = usize;
    type Item = T;

    fn validate(key: &Self::Key) -> bool {
        // Static arrays have a firm limit of N
        return *key < N;
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        // Static arrays have a firm limit of N
        SplitAsList::<Self>::next(key).take_if(|key| *key < N)
    }

    fn split(self, dispatcher: SplitDispatcher<'_, Self::Key, Self::Item>) -> OperationResult {
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
}

impl<K> From<K> for MapSplitKey<K> {
    fn from(value: K) -> Self {
        MapSplitKey::Specific(value)
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
        Self { contents, _ignore: Default::default() }
    }
}

impl<K, V, M> Splittable for SplitAsMap<K, V, M>
where
    K: 'static + Send + Sync + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync,
    M: 'static + Send + Sync + IntoIterator<Item = (K, V)>,
{
    type Key = MapSplitKey<K>;
    type Item = V;

    fn validate(_: &Self::Key) -> bool {
        // We have no way of knowing what the key bounds are for an arbitrary map
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        match key {
            Some(key) => {
                match key {
                    // Give the next key in the sequence
                    MapSplitKey::Sequential(index) => Some(MapSplitKey::Sequential(index + 1)),
                    // For an arbitrary map we don't know what would follow a specific key, so
                    // just stop iterating. This should never be reached in practice anyway.
                    MapSplitKey::Specific(_) => None,
                }
            }
            None => Some(MapSplitKey::Sequential(0))
        }
    }

    fn split(self, mut dispatcher: SplitDispatcher<'_, Self::Key, Self::Item>) -> OperationResult {
        let mut next_seq = 0;
        for (key, value) in self.contents.into_iter() {
            if let Err(value) = dispatcher.send(&MapSplitKey::Specific(key), value) {
                // The specific key is not being used, so send this as a sequential
                // value instead.
                let seq = MapSplitKey::Sequential(next_seq);
                next_seq += 1;
                // We don't care if whether we get an error from this
                dispatcher.send(&seq, value).ok();
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
    type Item = V;

    fn validate(_: &Self::Key) -> bool {
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        SplitAsMap::<K, V, Self>::next(key)
    }

    fn split(self, dispatcher: SplitDispatcher<'_, Self::Key, Self::Item>) -> OperationResult {
        SplitAsMap::new(self).split(dispatcher)
    }
}

impl<K, V> Splittable for BTreeMap<K, V>
where
    K: 'static + Send + Sync + Eq + Hash + Clone + Debug,
    V: 'static + Send + Sync,
{
    type Key = MapSplitKey<K>;
    type Item = V;

    fn validate(_: &Self::Key) -> bool {
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        SplitAsMap::<K, V, Self>::next(key)
    }

    fn split(self, dispatcher: SplitDispatcher<'_, Self::Key, Self::Item>) -> OperationResult {
        SplitAsMap::new(self).split(dispatcher)
    }
}

