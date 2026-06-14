# Migrating from remake2

remake3 is a clean-break redesign. Pipelines migrate by **reading and
rewriting** — there is deliberately no automated converter, because the long
tail (helper methods, implicit globals, class-inheritance tricks) needs
judgement, not regex.

The biggest shift: rules are **decorated functions**, not classes, and rule
**code/scope is tracked explicitly** so rerun decisions stay correct.

## At a glance

| remake2 | remake3 |
|---|---|
| `class R(Rule/TaskRule):` | `@rule(...)` on a plain function |
| `rule_matrix` / `var_matrix` | `matrix=` |
| `rule_inputs` / `rule_outputs` class attrs | `inputs=` / `outputs=` decorator args |
| `def rule_run(inputs, outputs, a, b)` | the decorated function; same signature |
| `def rule_run(self)` + `self.inputs` | explicit params: `def fn(inputs, outputs, a)` |
| implicit registration at class definition | `rmk.rules_from_current_module()` as the **last** line |
| dependency inferred from matching paths | explicit `depends_on=[upstream_rule]` |
| `Rule2.rule_inputs = Rule1.rule_outputs` | `inputs=rule1.outputs` |
| free use of module globals in a rule body | declare via `uses={'NAME': NAME, ...}` |
| atomic tmp-file output writes | gone — rules write outputs directly |
| pyquerylist filtering of `rmk.tasks` | `-Q 'python expr over kwargs'` on the CLI |
| `check_outputs_exist` config | `check_outputs='never'/'fallback'/'always'` |
| `remake archive` | removed |

## The part that needs real judgement: scope

remake2 rule bodies freely referenced module globals. remake3 rejects
**undeclared free variables** at import (`ScopeWarning`/`ScopeError`), because
untracked globals make rerun decisions wrong. For each free name in a rule
body:

- imported module / stdlib → leave it (exempt);
- constant or helper function → add to `uses={...}` so it participates in
  rerun hashing;
- mutable module state / config object → restructure: pass it through the
  matrix, or freeze it into `uses`.

`uses` tracking is **one level deep** — a helper that calls another helper
needs both declared.

## Adopting an existing output tree

You usually don't want to recompute a large pipeline just to start tracking it.
Mark existing outputs as succeeded (verifying they exist first):

```bash
remake set-state pipeline.py -Q True --success --check-outputs
```

## Full reference

This page is the orientation. The complete, worked migration notes — including
the two remake2 dialects, JASMIN SLURM partition/QoS renames, and a real
1885-line case study — live in
[`remake2_to_remake3.md`](https://github.com/markmuetz/remake/blob/remake3/.claude/skills/remake/references/remake2_to_remake3.md),
which also backs the `remake` Claude skill.
