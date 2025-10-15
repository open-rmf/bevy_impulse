[![style](https://github.com/open-rmf/crossflow/actions/workflows/style.yaml/badge.svg)](https://github.com/open-rmf/crossflow/actions/workflows/style.yaml)
[![ci_linux](https://github.com/open-rmf/crossflow/actions/workflows/ci_linux.yaml/badge.svg)](https://github.com/open-rmf/crossflow/actions/workflows/ci_linux.yaml)
[![ci_windows](https://github.com/open-rmf/crossflow/actions/workflows/ci_windows.yaml/badge.svg)](https://github.com/open-rmf/crossflow/actions/workflows/ci_windows.yaml)
[![ci_web](https://github.com/open-rmf/crossflow/actions/workflows/ci_web.yaml/badge.svg)](https://github.com/open-rmf/crossflow/actions/workflows/ci_web.yaml)
[![Crates.io Version](https://img.shields.io/crates/v/crossflow)](https://crates.io/crates/crossflow)

> [!IMPORTANT]
> For the ROS 2 integration feature, check out the [`ros2` branch](https://github.com/open-rmf/crossflow/tree/ros2).
>
> That feature is kept separate for now because it requires additional non-Rust setup. It will be merged into `main` after dynamic message introspection is finished.

# Reactive Programming for Bevy

This library provides sophisticated [reactive programming](https://en.wikipedia.org/wiki/Reactive_programming) for the [bevy](https://bevyengine.org/) ECS. In addition to supporting one-shot chains of async operations, it can support reusable workflows with parallel branches, synchronization, races, and cycles. These workflows can be hierarchical, so a workflow can be used as a building block by other workflows.

![sense-think-act workflow](assets/figures/sense-think-act_workflow.svg)

# Why use crossflow?

There are several different categories of problems that crossflow sets out to solve. If any one of these use-cases is relevant to you, it's worth considering crossflow as a solution:

* Coordinating **async activities** (e.g. filesystem i/o, network i/o, or long-running calculations) with regular bevy systems
* Calling **one-shot systems** on an ad hoc basis, where the systems require an input value and produce an output value that you need to use
* Defining a **procedure** to be followed by your application or by an agent or pipeline within your application
* Designing a complex **state machine** that gradually switches between different modes or behaviors while interacting with the world
* Managing many **parallel threads** of activities that need to be synchronized or raced against each other

# Helpful Links

 * [Introduction to workflows](https://docs.google.com/presentation/d/1_vJTyFKOB1T0ylCbp1jG72tn8AXYQOKgTGh9En9si-w/edit?usp=sharing)
 * [JSON Diagram Format](https://docs.google.com/presentation/d/1ShGRrbXtZYzaHTS-bPCU0nSmY-716OiFiB1VjGGTCfw/edit?usp=sharing)
 * [Crossflow Docs](https://docs.rs/crossflow/latest/crossflow/)
 * [Bevy Engine](https://bevyengine.org/)
 * [Bevy Cheat Book](https://bevy-cheatbook.github.io/)
 * [Rust Book](https://doc.rust-lang.org/stable/book/)
 * [Install Rust](https://www.rust-lang.org/tools/install)

# Compatibility

Crossflow may be supported across several releases of Bevy, although we only have one for the time being:

| bevy | crossflow |
|------|--------------|
|0.16  | 0.0.1        |

The `main` branch currently targets bevy version 0.16 (crossflow 0.0.x). We
will try to keep `main` up to date with the latest release of bevy, but you can
expect a few months of delay.

# Dependencies

This is a Rust project that often uses the latest language features. We recommend
installing `rustup` and `cargo` using the installation instructions from the Rust
website: https://www.rust-lang.org/tools/install

## Ubuntu Dependencies

For Ubuntu specifically you can run these commands to get the dependencies you need:

* To install `rustup` and `cargo`
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

* Make sure you have basic compilation tools installed
```bash
sudo apt-get install build-essential
```

# Build

Once dependencies are installed you can run the tests:

```bash
cargo test
```

You can find some illustrative examples for building workflows out of diagrams:
* [Calculator](examples/diagram/calculator)
* [Door manager that uses zenoh and protobuf](examples/zenoh-examples)
* [Mock navigation system using ROS](hhttps://github.com/open-rmf/crossflow/tree/ros2/examples/ros2)

To use `crossflow` in your own Rust project, you can run

```bash
cargo add crossflow
```
