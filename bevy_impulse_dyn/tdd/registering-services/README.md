## Registering Services

In order to run a serialized workflow, we need to map the services defined in the workflow to the service code loaded in the app, for that we need a service registry. The service registry need to know the inputs, outputs and params of a service in order for us to verify the workflow. It is very hard to write a "service manifest" correctly so ideally we want to generate it automatically.

### Service Registry

The service registry stores all the registered services, their request and response types, and the definitions for those types.

```rs
pub struct ServiceRegistry {
    services: .., // some map of the service id and definition
    types: .., // list of types used in registered services
}
```

The service id must be unique across all registered services, the types will be represented using JSON schema. JSON schema is chosen as it is well supported in many languages (most importantly javascript), this allows us to write more dynamic frontends that can do complex validations.

### Getting the service id

To automatically get a unique id, we can use `std::any::type_name`. The nice thing is that every function has it's own unique *function item* type (not the function pointer type) so we should be able to uniquely identify any services.

### Getting the Request, Response and Params

`schemars` is used to generate the json schema from rust types. In order to make a service serializable, the request and response must implement `JsonSchema` trait, which can be done by adding `#[derive(JsonSchema)]` to a struct.

### Registering services with requests or responses that cannot be serialized

The requests and responses of a service are very flexible, any type that is `Send + Sync` can be used as a request, sometimes that may include types that cannot be serialized. In order to register such services, we need to treat these request and responses differently.

A extension trait is used to add several methods to mark a service with either the request or response as opaque.

e.g.
```rs
app.register_service(unserializable_service.into_opaque());
```

Opaque request and responses will not have any definitions in the service registry, they are pretty much a blackbox to external parties. As a result, they cannot be transformed and they are only compatible with itself.

### Registering different variants of services

Most services are expected to be built from bevy systems, which are functions satisfying some constaints. The catch is that a system may be convertible into multiple types of services (e.g. a system with no args and no return type can be either a blocking service or async service).

Because of this, it is important to make a distinction that it is the *service* (actually service builder) that is being registered, not *system*. If we want the system to be usable as both a blocking and async service, it needs to be registered twice.

```rs
app.register_service(sys.into_blocking_service());
app.register_service(sys.into_async_service());
```

The 2 services will have different service ids and be considered as different service.

### Dealing with version skews

The service registry does not keep track of the version of a registered service. A serialized workflow may contain reference services with different signature than the current version of the app, in these cases, there are several ways we can deal with this.

1. The registry contains json schema definitions of all request and response types, we can use that to validate that the workflow inputs and outputs matches.
1. For opaque request and responses, it is assumed that they can only be passed directly from one node to another, a serialized workflow shouldn't be able to do anything with them.
1. In cases that there are behavior differences, the workflow should panic and fail.

A version field manually set by the service author may be added, but that will only be for semantic purposes only, we cannot guarantee that 2 services with the same service id and version have the same signature.

## Alternatives

### Use procedural macros to register services

```rs
#[derive(Service)]
fn foo(f: &Foo) {}
```

The main issue with this is that you *cannot* store state in a procedural macro, at least not without very ugly hacks like writing to filesystem, there are also other problems like incremental compilation making things incredibly complex.

Even if we can store state, there are still other issues, for example, procedural macros work on tokens, not AST and it does not have information on things outside the macro. i.e. We can see that `foo` take an arg of type `&Foo`, but we do not have access to the AST of `Foo` so we can't do reflection directly, reflection must be done at the definition of `Foo` so we can't check from the macro if `Foo` does it. Also the error from the compiler when these preconditions are not met is likely to be very confusing.
