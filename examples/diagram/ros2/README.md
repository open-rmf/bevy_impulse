## Building workflows with ROS nodes

The examples in this directory showcase how to incorporate ROS primitives into a workflow.

We use rclrs for this demo, and currently the build pipeline to use ROS messages for rclrs takes some extra steps since Rust is not (yet) a core language for ROS.

To run this demo, follow these steps:

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

```bash
mkdir -p workspace/src && cd workspace
```

```bash
git clone https://github.com/open-rmf/bevy_impulse src/bevy_impulse
```

```bash
vcs import src < src/bevy_impulse/examples/ros2/example-messages.repos
```

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

Then you can go into the `bevy_impulse` directory and run the example with

```bash
cd src/bevy_impulse
```

```bash
cargo run -p ros2-workflow-examples --bin nav-example
```
