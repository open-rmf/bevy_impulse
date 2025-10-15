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

use bevy_derive::{Deref, DerefMut};

use std::{borrow::Cow, sync::Arc};

use super::{BuiltinTarget, NamespacedOperation, NextOperation, OperationName};

use smallvec::{smallvec, SmallVec};

use serde::{Deserialize, Serialize};

use schemars::{json_schema, JsonSchema};

/// This enum allows every operation within a workflow to have a unique key,
/// even if it is nested inside other operations.
#[derive(
    Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum OperationRef {
    Named(NamedOperationRef),
    Terminate(NamespaceList),
    Dispose,
    Cancel(NamespaceList),
    StreamOut(StreamOutRef),
}

impl OperationRef {
    pub fn in_namespaces(self, parent_namespaces: &[Arc<str>]) -> Self {
        match self {
            Self::Named(named) => Self::Named(named.in_namespaces(parent_namespaces)),
            Self::Terminate(namespaces) => {
                Self::Terminate(with_parent_namespaces(parent_namespaces, namespaces))
            }
            Self::Dispose => Self::Dispose,
            Self::Cancel(namespaces) => {
                Self::Cancel(with_parent_namespaces(parent_namespaces, namespaces))
            }
            Self::StreamOut(stream_out) => {
                Self::StreamOut(stream_out.in_namespaces(parent_namespaces))
            }
        }
    }

    pub fn terminate_for(namespace: Arc<str>) -> Self {
        Self::Terminate(NamespaceList::for_child_of(namespace))
    }
}

impl<'a> From<&'a NextOperation> for OperationRef {
    fn from(value: &'a NextOperation) -> Self {
        match value {
            NextOperation::Name(name) => OperationRef::Named(name.into()),
            NextOperation::Namespace(id) => OperationRef::Named(id.into()),
            NextOperation::Builtin { builtin } => match builtin {
                BuiltinTarget::Terminate => OperationRef::Terminate(NamespaceList::default()),
                BuiltinTarget::Dispose => OperationRef::Dispose,
                BuiltinTarget::Cancel => OperationRef::Cancel(NamespaceList::default()),
            },
        }
    }
}

impl<'a> From<&'a OperationName> for OperationRef {
    fn from(value: &'a OperationName) -> Self {
        OperationRef::Named(value.into())
    }
}

impl From<NamedOperationRef> for OperationRef {
    fn from(value: NamedOperationRef) -> Self {
        OperationRef::Named(value)
    }
}

impl std::fmt::Display for OperationRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Named(named) => write!(f, "{named}"),
            Self::Terminate(namespaces) => {
                write!(f, "{}(terminate)", namespaces)
            }
            Self::Cancel(namespaces) => write!(f, "{}(cancel)", namespaces),
            Self::Dispose => write!(f, "(dispose)"),
            Self::StreamOut(stream_out) => write!(f, "{stream_out}"),
        }
    }
}

#[derive(
    Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct NamedOperationRef {
    pub namespaces: NamespaceList,
    /// If this references an exposed operation, such as an exposed input or
    /// output of a session, this will contain the session name. Suppose we have
    /// a section named `sec` and it has an exposed output named `out`. Then
    /// there are two values of `NamedOperationRef` to consider:
    ///
    /// ```
    /// # use crossflow::diagram::{NamedOperationRef, NamespaceList};
    /// # use smallvec::smallvec;
    ///
    /// let op_id = NamedOperationRef {
    ///     namespaces: NamespaceList::for_child_of("sec".into()),
    ///     exposed_namespace: None,
    ///     name: "out".into(),
    /// };
    /// ```
    ///
    /// is the internal reference to `sec:out` that will be used by other
    /// operations inside of `sec`. On the other hand, operations that are
    /// siblings of `sec` would instead connect to
    ///
    /// ```
    /// # use crossflow::diagram::NamedOperationRef;
    /// # use smallvec::smallvec;
    ///
    /// let op_id = NamedOperationRef {
    ///     namespaces: Default::default(),
    ///     exposed_namespace: Some("sec".into()),
    ///     name: "out".into(),
    /// };
    /// ```
    ///
    /// We need to make this distinction because operations inside `sec` do not
    /// know which of their siblings are exposed, and we don't want operations
    /// outside of `sec` to accidentally connect to operations that are supposed
    /// to be internal to `sec`.
    pub exposed_namespace: Option<Arc<str>>,
    pub name: Arc<str>,
}

impl NamedOperationRef {
    pub fn in_namespaces(mut self, parent_namespaces: &[Arc<str>]) -> Self {
        apply_parent_namespaces(parent_namespaces, &mut self.namespaces);
        self
    }
}

impl<'a> From<&'a OperationName> for NamedOperationRef {
    fn from(name: &'a OperationName) -> Self {
        NamedOperationRef {
            namespaces: NamespaceList::default(),
            exposed_namespace: None,
            name: Arc::clone(name),
        }
    }
}

impl From<OperationName> for NamedOperationRef {
    fn from(name: OperationName) -> Self {
        NamedOperationRef {
            namespaces: NamespaceList::default(),
            exposed_namespace: None,
            name,
        }
    }
}

impl<'a> From<&'a NamespacedOperation> for NamedOperationRef {
    fn from(id: &'a NamespacedOperation) -> Self {
        NamedOperationRef {
            namespaces: NamespaceList::default(),
            // This is referring to an exposed operation, so the namespace
            // mentioned in the operation goes into the exposed_namespace field
            exposed_namespace: Some(Arc::clone(&id.namespace)),
            name: Arc::clone(&id.operation),
        }
    }
}

impl<'a> From<&'a NamespacedOperation> for OperationRef {
    fn from(value: &'a NamespacedOperation) -> Self {
        OperationRef::Named(value.into())
    }
}

impl std::fmt::Display for NamedOperationRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for namespace in &self.namespaces {
            write!(f, "{namespace}:")?;
        }

        if let Some(exposed) = &self.exposed_namespace {
            write!(f, "{exposed}:(exposed):")?;
        }

        f.write_str(&self.name)
    }
}

#[derive(
    Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct StreamOutRef {
    /// The namespace of the scope is being streamed out of
    pub namespaces: NamespaceList,
    /// The name of the stream (within its scope) that is being referred to
    pub name: Arc<str>,
}

impl StreamOutRef {
    pub fn new_for_root(stream_name: impl Into<Arc<str>>) -> Self {
        Self {
            namespaces: NamespaceList::default(),
            name: stream_name.into(),
        }
    }

    pub fn new_for_scope(
        scope_name: impl Into<Arc<str>>,
        stream_name: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            namespaces: NamespaceList::for_child_of(scope_name.into()),
            name: stream_name.into(),
        }
    }

    fn in_namespaces(mut self, parent_namespaces: &[Arc<str>]) -> Self {
        apply_parent_namespaces(parent_namespaces, &mut self.namespaces);
        self
    }
}

impl std::fmt::Display for StreamOutRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for namespace in &self.namespaces {
            write!(f, "{namespace}:")?;
        }

        write!(f, "(stream_out:{})", self.name)
    }
}

impl From<StreamOutRef> for OperationRef {
    fn from(value: StreamOutRef) -> Self {
        OperationRef::StreamOut(value)
    }
}

fn apply_parent_namespaces(parent_namespaces: &[Arc<str>], namespaces: &mut NamespaceList) {
    // Put the parent namespaces at the front and append the operation's
    // existing namespaces at the back.
    let new_namespaces = NamespaceList(
        parent_namespaces
            .iter()
            .cloned()
            .chain(namespaces.drain(..))
            .collect(),
    );

    *namespaces = new_namespaces;
}

fn with_parent_namespaces(
    parent_namespaces: &[Arc<str>],
    mut namespaces: NamespaceList,
) -> NamespaceList {
    apply_parent_namespaces(parent_namespaces, &mut namespaces);
    namespaces
}

#[derive(
    Debug,
    Default,
    Clone,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deref,
    DerefMut,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct NamespaceList(pub SmallVec<[OperationName; 4]>);

impl NamespaceList {
    pub fn for_child_of(parent: Arc<str>) -> Self {
        Self(smallvec![parent])
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Arc<str>> {
        self.0.iter()
    }
}

impl std::fmt::Display for &'_ NamespaceList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for namespace in &self.0 {
            write!(f, "{namespace}:")?;
        }

        Ok(())
    }
}

impl IntoIterator for NamespaceList {
    type Item = Arc<str>;
    type IntoIter = smallvec::IntoIter<[Arc<str>; 4]>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a NamespaceList {
    type Item = &'a Arc<str>;
    type IntoIter = std::slice::Iter<'a, Arc<str>>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl JsonSchema for NamespaceList {
    fn schema_name() -> Cow<'static, str> {
        "NamespaceList".into()
    }

    fn schema_id() -> Cow<'static, str> {
        concat!(module_path!(), "::NamespaceList").into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        json_schema!({
            "type": "array",
            "items": {
                "type": "string"
            }
        })
    }
}
