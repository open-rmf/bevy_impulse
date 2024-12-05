use std::any::TypeId;

use crate::Builder;

use super::{
    impls::{DefaultImpl, NotSupported},
    DiagramError, DynOutput,
};

pub trait DynForkClone<T> {
    const CLONEABLE: bool;

    fn dyn_fork_clone(
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError>;
}

impl<T> DynForkClone<T> for NotSupported {
    const CLONEABLE: bool = false;

    fn dyn_fork_clone(
        _builder: &mut Builder,
        _output: DynOutput,
        _amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        Err(DiagramError::NotCloneable)
    }
}

impl<T> DynForkClone<T> for DefaultImpl
where
    T: Send + Sync + 'static + Clone,
{
    const CLONEABLE: bool = true;

    fn dyn_fork_clone(
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        assert_eq!(output.type_id, TypeId::of::<T>());

        let fork_clone = output.into_output::<T>().fork_clone(builder);
        Ok((0..amount)
            .map(|_| fork_clone.clone_output(builder).into())
            .collect())
    }
}
