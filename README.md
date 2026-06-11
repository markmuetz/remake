# remake 

Remake is a smart Python build tool, similar to `make`. It makes it easy to build pipelines with complex dependencies, and deploy these to high-performance computing systems. It is particularly suited to use for scientific workflows, due to its ability to reliably recreate any set of output files, based on running only those tasks that are necessary.

## Status

remake3 is a clean-break redesign, currently being implemented. See
[`design_docs/remake3_design.md`](design_docs/remake3_design.md) for the
design and [`examples/`](examples) for the intended API.

## Installation

```bash
pip install -e .
```
