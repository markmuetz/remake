# remake: instructions for Claude

## Keep it simple

Simple is better than complex. Don't overengineer: prefer the smallest
design that fixes the user-visible problem, and the fewest new files,
records, states and code paths. Reuse what remake already has. When a
design needs grace periods, corroboration rules or a state machine to be
safe, step back and look for a simpler shape: if MM couldn't debug it,
it's too complex. Example: SLURM reconciliation (0.11) was cut from a
~1500-line sacct classifier to marking submitted tasks PENDING plus
read-only reporting (`design_docs/designs/slurm_reconcile/`).

Read and follow [The Zen of Python (PEP 20)](https://peps.python.org/pep-0020/)
(`python -c "import this"`), especially: "If the implementation is hard to
explain, it's a bad idea."

## How to staff implementation work

- **Opus 5.5** (`claude-opus-5-5`) is the orchestrator and main implementor:
  it plans, writes the core changes, integrates, and runs the reviews.
- **Fable 5.2** (`claude-fable-5-2`) is the advisor: a second opinion on
  designs, plans and diffs before they are committed, given the code rather
  than Opus's conclusions.
- **Sonnet 5.5** (`claude-sonnet-5-5`) subagents take the simpler, well-scoped
  tasks: examples, docs, tests for a settled design, mechanical renames.
  Brief them with the files and the decision, and review what they return.

## Every new feature has an example

A new user-facing feature (DSL, token, config, CLI command or flag) ships with
an example in `examples/`, in the same change:

- a new `exN_<topic>.py` (or a directory, if it needs its own files such as a
  `remake.toml`), or an extension of an existing example where it fits;
- a row in `examples/README.md`;
- coverage in `tests/integration/test_examples.py` (loads and plans; runs end
  to end if it needs only the stdlib).

## Releases

See `design_docs/releasing.md`. Each release's scope and decisions live in
`design_docs/releases/vX.Y.Z.md`.
