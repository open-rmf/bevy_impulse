use std::{
    any::{type_name, Any, TypeId},
    fmt::Display,
    hash::Hash,
};

use serde::Serialize;

#[derive(Copy, Clone, Debug, Eq)]
pub struct TypeInfo {
    pub type_id: TypeId,
    pub type_name: &'static str,
}

impl TypeInfo {
    pub(super) fn of<T>() -> Self
    where
        T: Any,
    {
        Self {
            type_id: TypeId::of::<T>(),
            type_name: type_name::<T>(),
        }
    }
}

impl Hash for TypeInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.type_id.hash(state)
    }
}

impl PartialEq for TypeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
    }
}

impl Display for TypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.type_name.fmt(f)
    }
}

impl Serialize for TypeInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.type_name)
    }
}
