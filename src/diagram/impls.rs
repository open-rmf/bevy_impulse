use std::marker::PhantomData;

/// A struct to provide the default implementation for various operations.
pub struct DefaultImpl;

/// A struct to provide the default implementation for various operations.
pub struct DefaultImplMarker<T> {
    _unused: PhantomData<T>,
}

impl<T> DefaultImplMarker<T> {
    pub(super) fn new() -> Self {
        Self {
            _unused: Default::default(),
        }
    }
}

/// A struct to provide "not supported" implementations for various operations.
pub struct NotSupported;

/// A struct to provide "not supported" implementations for various operations.
pub struct NotSupportedMarker<T> {
    _unused: PhantomData<T>,
}
