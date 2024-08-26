# Reactive Programming for Bevy

This library provides sophisticated [reactive programming](https://en.wikipedia.org/wiki/Reactive_programming) for the [bevy](https://bevyengine.org/) ECS. In addition to supporting one-shot chains of async operations, it can support reusable workflows with parallel branches, synchronization, races, and cycles. These workflows can be hierarchical, so workflows can be built out of other workflows.

![sense-think-act workflow](assets/figures/sense-think-act_workflow.svg)

# Why use bevy impulse?

There are several different categories of problems that bevy impulse sets out to solve. If any one of these use-cases is relevant to you, it's worth considering bevy impulse as a solution:

* Coordinating **async activities** (e.g. filesystem i/o, network i/o, or long-running calculations) with regular bevy systems
* Calling **one-shot system** on an ad-hoc basis, where the systems require an input value and produce an output value
* Defining a **procedure** to be followed by your application or by an agent or pipeline within your application
* Designing a complex **state machine** that gradually switches between different modes or behaviors while interacting with the world
* Managing many **parallel threads** of activities that need to be synchronized

# Helpful Links

 * [Introduction to workflows](https://docs.google.com/presentation/d/1_vJTyFKOB1T0ylCbp1jG72tn8AXYQOKgTGh9En9si-w/edit?usp=sharing)
 * [Bevy Engine](https://bevyengine.org/)
 * [Bevy Cheat Book](https://bevy-cheatbook.github.io/)
 * [Rust Book](https://doc.rust-lang.org/stable/book/)

# Experimenting

### Install Rust

Follow [official guidelines](https://www.rust-lang.org/tools/install) to install the Rust language.

### Get source code

```
$ git clone https://github.com/open-rmf/bevy_impulse
```

### Build

To build the library simply go to the root directory of the repo and run

```
$ cargo build
```

### Test

The library's tests can be run with

```
$ cargo test
```

### View Documentation

Like most Rust projects, the library documentation is written into the source code and can be built and viewed with

```
$ cargo doc --open
```

After the first release of the library, the documentation will be hosted on docs.rs. We will update this README with a link to that documentation once it is ready.
