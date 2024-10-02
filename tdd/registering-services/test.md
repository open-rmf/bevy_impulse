## Registering Services

In order to run a serialized workflow, we need to map the services defined in the workflow to the service code loaded in the app, for that we need a service registry. The service registry need to know the inputs, outputs and params of a service in order for us to verify the workflow.

It is very hard to write a "service manifest" correctly so ideally we want to generate it automatically. We will use extension traits to add several functions to `bevy_app::App` to register and retrieve these manifests.

The usage will be like this:

```rs
fn hello() {}

fn main() {
    let mut app = App::new();
    app.register_service(hello);
}
```

### Creating the Registry

The input to is a bevy system, which is a rust function, in order to store service registrations, we need a hashable key, for that we assign a unique identifier to each service.

```rs
pub struct ServiceRegistry {
    registrations: HashMap<&'static, ServiceRegistration>,
}
```

### Getting the Identifer

Every function has a unique *function item* type, we can use `std::any::type_name` to get a unique identifier. Note that we need the *function item* type, not the *function pointer* type, which are the same for 2 function with the same signature. The catch here is that there is no syntax for function item types! https://doc.rust-lang.org/reference/types/function-item.html.

```rs
fn foo() {}
fn bar() {}

// std::any::type_name::<foo>() // this doesn't compile! `foo` is not a type.
```

In order to get the function item type, we need to "cheat" abit by inferring it

```rs
fn get_type_name<F>(f: F) -> &str {
    std::any::type_name::<F>()
}

fn foo() {}
fn bar() {}

fn main() {
    println!("{}", get_type_name(foo));
    println!("{}", get_type_name(bar));

    // or just
    println!("{}", std::any::type_name_of_val(foo));
}
```

### Getting the Request, Response and Params

The request and response types are represented with JSON schema. JSON schema is chosen as it is well supported in many languages (most importantly javascript), this allows us to write more dynamic frontends that can do complex validations.

`schemars` is used to generate the json schema from rust types. In order to make a service serializable, the request and response must implement `JsonSchema` trait, which can be done by adding `#[derive(JsonSchema)]` to a struct.

Sometimes service requests may be wrapped, for example, blocking services have their requests wrapped in `BlockingService<...>`, we can't implement `JsonSchema` on `BlockingService` as that will return the schema for the wrapper. To workaround that, a `SerializableServiceRequest` trait is introduced, wrappers like `BlockingService` implements that by forwarding the schema generation to the wrapped requests.

### Requests and responses that cannot be serialized

The requests and responses of a service are very flexible, any type that is `Send + Sync` can be used as a request, sometimes, that may include types that cannot be serialized. In order to register such services, we need to mark these request and responses.

A `OpaqueServiceExt` extension trait is used to add several methods to any type that is also `IntoServiceBuilder`, you will be able to use `into_opaque`, `into_opaque_request` and `into_opaque_response` to mark either the request or response as unserializable.

```rs
app.register_service(unserializable_service.into_opaque());
```

## Alternatives

### Use procedural macros to register services

```rs
#[derive(Service)]
fn foo(f: &Foo) {}
```

The main issue with this is that you *cannot* store state in a procedural macro, at least not without very ugly hacks like writing to filesystem, there are also other problems like incremental compilation potentially causing things to go out of sync.

Even if we can store state, there are still other issues, like procedural macros work on *tokens*, not AST and it does not have information on things outside the macro. i.e. We can see that `foo` take an arg of type `&Foo`, but we do not have access to the AST of `Foo` so we can't do reflection directly, reflection must be done at the definition of `Foo` so we can't check if `Foo` does it, resulting in very confusing compiler errors if it does not.
