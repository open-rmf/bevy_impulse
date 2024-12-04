use crate::Builder;

use super::{
    impls::{DefaultImpl, NotSupported},
    DiagramError, DynOutput,
};

pub trait DynForkResult<T> {
    const SUPPORTED: bool;

    fn dyn_fork_result(
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError>;
}

impl<T> DynForkResult<T> for NotSupported {
    const SUPPORTED: bool = false;

    fn dyn_fork_result(
        _builder: &mut Builder,
        _output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError> {
        Err(DiagramError::CannotForkResult)
    }
}

impl<T, E> DynForkResult<Result<T, E>> for DefaultImpl
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    const SUPPORTED: bool = true;

    fn dyn_fork_result(
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError> {
        let chain = output.into_output::<Result<T, E>>().chain(builder);
        let (ok, err) = chain.fork_result(|c| c.output(), |c| c.output());
        Ok((ok.into(), err.into()))
    }
}
