[![style](https://github.com/open-rmf/bevy_impulse/actions/workflows/style.yaml/badge.svg)](https://github.com/open-rmf/bevy_impulse/actions/workflows/style.yaml)
[![ci_linux](https://github.com/open-rmf/bevy_impulse/actions/workflows/ci_linux.yaml/badge.svg)](https://github.com/open-rmf/bevy_impulse/actions/workflows/ci_linux.yaml)
[![ci_windows](https://github.com/open-rmf/bevy_impulse/actions/workflows/ci_windows.yaml/badge.svg)](https://github.com/open-rmf/bevy_impulse/actions/workflows/ci_windows.yaml)
[![ci_web](https://github.com/open-rmf/bevy_impulse/actions/workflows/ci_web.yaml/badge.svg)](https://github.com/open-rmf/bevy_impulse/actions/workflows/ci_web.yaml)
[![Crates.io Version](https://img.shields.io/crates/v/bevy_impulse)](https://crates.io/crates/bevy_impulse)


> [!IMPORTANT]
> You are on a branch with experimental support for ROS 2 via [`rclrs`](https://github.com/ros2-rust/ros2_rust).
> This will require more steps to set up than usual, so installation instructions for the necessary packages are below:

1. Install ROS Jazzy according to the [normal installation instructions](https://docs.ros.org/en/jazzy/Installation.html).

2. Install Rust according to the [normal installation instructions](https://www.rust-lang.org/tools/install).

3. Run these commands to set up the workspace with the message bindings:

```bash
sudo apt install -y git libclang-dev python3-pip python3-vcstool # libclang-dev is required by bindgen
```

```bash
pip install git+https://github.com/colcon/colcon-cargo.git --break-system-packages
```

```bash
pip install git+https://github.com/colcon/colcon-ros-cargo.git --break-system-packages
```

Create a workspace with the necessary repos:

```bash
mkdir -p workspace/src && cd workspace
```

```bash
git clone https://github.com/open-rmf/bevy_impulse src/bevy_impulse -b ros2
```

```bash
vcs import src < src/bevy_impulse/ros2-feature.repos
```

Source the ROS distro and build the workspace:

```bash
source /opt/ros/jazzy/setup.bash
```

```bash
colcon build
```

4. After `colcon build` has finished, you should see a `.cargo/config.toml` file inside your workspace, with `[patch.crates-io.___]` sections pointing to the generated message bindings. Now you should source the workspace using

```bash
source install/setup.bash
```

Now you can run the [ROS 2 example](examples/ros2/README.md). You can also create your own crate in this colcon workspace and link as shown in the example.

# Reactive Programming for Bevy

This library provides sophisticated [reactive programming](https://en.wikipedia.org/wiki/Reactive_programming) for the [bevy](https://bevyengine.org/) ECS. In addition to supporting one-shot chains of async operations, it can support reusable workflows with parallel branches, synchronization, races, and cycles. These workflows can be hierarchical, so a workflow can be used as a building block by other workflows.

![sense-think-act workflow](assets/figures/sense-think-act_workflow.svg)

# Why use bevy impulse?

There are several different categories of problems that bevy impulse sets out to solve. If any one of these use-cases is relevant to you, it's worth considering bevy impulse as a solution:

* Coordinating **async activities** (e.g. filesystem i/o, network i/o, or long-running calculations) with regular bevy systems
* Calling **one-shot systems** on an ad hoc basis, where the systems require an input value and produce an output value that you need to use
* Defining a **procedure** to be followed by your application or by an agent or pipeline within your application
* Designing a complex **state machine** that gradually switches between different modes or behaviors while interacting with the world
* Managing many **parallel threads** of activities that need to be synchronized or raced against each other

# Helpful Links

 * [Introduction to workflows](https://docs.google.com/presentation/d/1_vJTyFKOB1T0ylCbp1jG72tn8AXYQOKgTGh9En9si-w/edit?usp=sharing)
 * [JSON Diagram Format](https://docs.google.com/presentation/d/1ShGRrbXtZYzaHTS-bPCU0nSmY-716OiFiB1VjGGTCfw/edit?usp=sharing)
 * [Bevy Impulse Docs](https://docs.rs/bevy_impulse/latest/bevy_impulse/)
 * [Bevy Engine](https://bevyengine.org/)
 * [Bevy Cheat Book](https://bevy-cheatbook.github.io/)
 * [Rust Book](https://doc.rust-lang.org/stable/book/)
 * [Install Rust](https://www.rust-lang.org/tools/install)

# Compatibility

Bevy Impulse is supported across several releases of Bevy:

| bevy | bevy_impulse |
|------|--------------|
|0.14  | 0.2          |
|0.13  | 0.1          |
|0.12  | 0.0.x        |

The `main` branch currently targets bevy version 0.12 (bevy impulse 0.0.x)
so that new developments are still compatible for users of bevy 0.12. New features
will be forward-ported as soon as possible. `main` will move forward to newer
versions of bevy when we judge that enough of the ecosystem has finished migrating
forward that there is no longer value in supporting old versions. In the future
we may come up with a more concrete policy for this, and we are open to input on
the matter.

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
* [Mock navigation system using ROS](https://github.com/open-rmf/ros2-impulse-examples)

To use `bevy_impulse` in your own Rust project, you can run

```bash
cargo add bevy_impulse
```
