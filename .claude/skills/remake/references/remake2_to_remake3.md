# Migrating remake2 remakefiles to remake3

Translate by reading and rewriting — there is deliberately no automated
tool. Read the whole remake2 file first; the long tail (helper methods,
implicit globals, class inheritance tricks) needs judgement, not regex.

## Recognising the source dialect

Two remake2-era styles exist:

```python
# Style A (remake ~0.7/0.8 era)
class Rule1(Rule):
    rule_matrix = {'a': [...], 'b': [...]}
    rule_inputs = {'in': 'data/{a}/{b}.in'}      # or a function
    rule_outputs = {'out': 'data/{a}/{b}.out'}
    def rule_run(inputs, outputs, a, b):          # note: no self
        ...

# Style B (older TaskRule era)
class Basic1(TaskRule):
    rule_inputs = {'in': 'data/in1.txt'}
    rule_outputs = {'out': 'data/out1.txt'}
    var_matrix = {'a': [...]}                     # name varies
    def rule_run(self):                           # self-based
        self.inputs, self.outputs, self.a ...
```

Both also relied on a module-level `rmk = Remake()` (or `ex1 = Remake()`)
with rules registered implicitly at class-definition time.

## The translation, item by item

| remake2 | remake3 |
|---|---|
| `class R(Rule/TaskRule):` | `@rule(...)` on a plain function |
| `rule_matrix` / `var_matrix` | `matrix=` |
| `rule_inputs` / `rule_outputs` class attrs | `inputs=` / `outputs=` decorator args (same dict/callable forms) |
| `def rule_run(inputs, outputs, a, b)` | the decorated function; same signature contract |
| `def rule_run(self)` + `self.inputs`/`self.a` | rewrite to explicit params: `def fn(inputs, outputs, a)` |
| implicit registration at class definition | `rmk.rules_from_current_module()` as the LAST line |
| dependency inferred from matching path strings | explicit `depends_on=[upstream_rule]` on every consumer |
| `Rule2.rule_inputs = Rule1.rule_outputs` | `inputs=rule1.outputs` (decorated fn carries `.outputs`/`.matrix`) |
| free use of module globals in `rule_run` | declare via `uses={'NAME': NAME, ...}` (see below) |
| `self.inputs` values were `Path` objects | inputs are plain strings, outputs path-like tokens — `Path(...)` wrap if Path methods needed |
| atomic tmp-file output writes | gone — rules write outputs directly (parent dirs pre-created) |
| pyquerylist filtering of `rmk.tasks` | `-Q 'python expr over kwargs'` on the CLI |
| `remake run-tasks file --tasks <keys>` | `remake run-task file <key-prefix>` |
| per-task SLURM jobs, job-name keyed | per-rule array jobs; `config={'slurm': {...}}` per rule unchanged in spirit |
| `partition='short-serial'/'long-serial'` (JASMIN) | `partition='standard', qos='standard'` (SLURM 25.11 renamed them) |
| `remake archive` | gone |
| `check_outputs_exist` config | `check_outputs='never'/'fallback'/'always'` Remake arg |

## Scope: the step that needs real judgement

remake2 `rule_run` bodies freely referenced module globals (constants,
helper functions, other classes). remake3 rejects undeclared free
variables at import (`ScopeWarning`/`ScopeError`) because untracked
globals make rerun decisions wrong.

For each free name in a rule body decide:
- imported module / stdlib → leave it, exempt;
- constant or helper function → add to `uses={...}` (it then correctly
  participates in rerun hashing);
- mutable module state / config object → restructure: pass through the
  matrix, or freeze into `uses`.

`uses` tracking is one level deep — a helper calling another helper
needs both declared.

## Metadata: no DB migration

remake3 starts a fresh `.remake/` (new schema, new task keys). Existing
on-disk outputs are NOT lost work: the default `check_outputs='fallback'`
recognises complete outputs for tasks with no DB record, so the first
plan after migration should show ~0 tasks to run for an up-to-date
pipeline. That is also the migration acceptance test.

## Workflow

1. Read the remake2 file end-to-end; list rules, globals each `rule_run`
   touches, and the implicit input/output chains.
2. Translate per the table; wire `depends_on` explicitly (follow the old
   path-string chains).
3. `remake run newfile.py -n` — must import cleanly (signature/scope
   errors surface here) and the plan must make sense: for a pipeline
   whose outputs exist, expect ~0 runnable; "everything runnable" means
   outputs aren't where the new paths point.
4. Run one small slice: `remake run newfile.py -Q '<one task's kwargs>'`.
5. Full run.

Keep the remake2 file until step 5 has been verified.
