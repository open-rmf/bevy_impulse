# Calculator example

This is an example that shows how to build a `crossflow` workflow from a diagram
that expresses some calculator operations. This is not a practical use case of
workflows; it is only meant to be illustrative of how to use the tools.

For a quick run this example, open a terminal focused on this folder and run:

```bash
cargo run -- run diagrams/multiply_by_3.json 10
```

You should see `30.0` printed out by the program, because `multiply_by_3.json` is a
very simple workflow that just multiples your input by 3.

You can replace `10` with a different number or you can write a different workflow
diagram to perform a different set of operations on the input value.

## Diagram Editor

To use the diagram editor to create a new calculator workflow, run

```bash
cargo run -- serve
```

Then open http://localhost:3000 to run the diagram editor app from your web browser.
