## Registering Services

In order to run a serialized workflow, we need to map the services defined in the workflow to the service code loaded in the app, for that we need a service registry. The service registry need to know the inputs, outputs and params of a service in order for us to verify the workflow.

It is very hard to write a "service manifest" correctly so ideally we want to generate it automatically. We will use extension traits to add several functions to `bevy_app::App` to register and retrieve these manifests.

The usage will be like this:

```rs
fn hello() {}

fn main() {
    let mut app = App::new();
    app.register_blocking_service(hello);
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

WIP: tldr bevy_reflect

## Alternatives

### Use procedural macros to register services

```rs
#[derive(service)]
fn foo(f: &Foo) {}
```

The main issue with this is that you *cannot* store state in a procedural macro, at least not without very ugly hacks like writing to filesystem, there are also other problems like incremental compilation potentially causing things to go out of sync.

Even if we can store state, there are still other issues, like procedural macros work on *tokens*, not AST and it does not have information on things outside the macro. i.e. We can see that `foo` take an arg of type `&Foo`, but we do not have access to the AST of `Foo` so we can't do reflection directly, reflection must be done at the definition of `Foo` so we can't check if `Foo` does it, resulting in very confusing compiler errors if it does not.
