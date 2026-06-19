# Road to the 0.8.0 release

What stands between the current state (0.8.0a0 alpha on PyPI, all
infrastructure shipped) and a full `0.8.0` release, plus the one-time
branch migration that folds the `remake3` working branch into `main`.

State at time of writing (2026-06-15): engine, CLI, executors, docs site
(GitHub Pages), CI (pytest 3.10–3.14 + coverage), the Trusted-Publishing
release workflow, and the `0.8.0a0` alpha are all done — see
[remake3_implementation_plan.md](remake3_implementation_plan.md). The
remaining work is validation, polish, the branch migration, and the
version bump.

## A. Blocking items (must do before tagging 0.8.0)

1. **Dogfood on the JASMIN remake2→remake3 migration.** The real
   pipeline migration is the de-facto beta: it exercises the migration
   path, the skill, and SLURM at real scale on actual data. 0.8.0 should
   not ship until at least one substantial real pipeline runs end-to-end
   on JASMIN under remake3. This is the last open item in the
   implementation plan ("validate the skill in anger on JASMIN").

   **DONE — equivalence test PASSED 2026-06-17/18.** The `mcs_prime`
   paper pipeline (6 production remakefiles: `download_gpm_imerg.py`,
   `download_era5.py`, `ASoP_analysis.py`, `N216_ens_analysis.py` +
   `proj_config.py`/`utils.py`; 24 rules total, the largest file 19
   rules / 18 edges) was hand-translated per the skill, run end-to-end on
   JASMIN via the SLURM executor into a parallel output tree, and diffed
   against the existing remake2 outputs. **Every paper figure
   (fig01–fig04, the supplementary, and the ASoP fig02/fig03 set) is
   visually identical to the remake2 reference — confirmed by
   overlay-switching, not just side-by-side.** No scientifically
   meaningful difference. This is the real beta: it shook out the SLURM
   JSON-round-trip kwargs bug (§Correctness in `todos.md`, now fixed by a
   plan-time guard), the `io_hash` gap (output-path redirection leaving
   orphans), the exit-127 PATH issue, and several skill-reference
   refinements (see `references/remake2_to_remake3.md`). The migration is
   documented downstream at
   `mcs_prime_stoch_trigger/docs/remake2_to_remake3_migration_plan.md`.

2. **Docs-vs-code accuracy pass.** The docs were written mid-flight and
   have already drifted:
   - `why` now explains multiple tasks (query matches / runnable set), and
     `info` gained `--reasons` + `-F` dedup — neither is in
     `docs/guide/debugging.md`.
   - The tuple-key and dynamic matrix forms are documented; re-verify
     every CLI flag and example against the shipped behaviour.
   - Action: read each docs page against the current CLI/API; `mkdocs
     build --strict` already guards links, not correctness.

3. **Branch migration into `main`** (see §D). Do this once the above are
   stable, so `main` becomes the release branch and the version bump /
   tag happen there.

4. **Version bump `0.8.0a0` → `0.8.0`** in `src/remake/version.py`, as the
   final commit before tagging. The release workflow's build job asserts
   the `v*` tag matches the package version, so tag `v0.8.0`.

## B. Should-do (strongly wanted, not strictly blocking)

- **CLI UX nit found on JASMIN:** a `why -Q <query>` matching >1 task
  raises `RemakeError` as an uncaught traceback instead of a clean
  message. Fix as part of a small CLI-polish pass (logged in the plan's
  "What's left").
- ~~**CHANGELOG / release notes** for 0.8.0~~ **Done 2026-06-19.**
  `CHANGELOG.md` at the repo root (Keep a Changelog format): a clean-break
  "remake3 is a ground-up rewrite from the remake2 0.6.x line" summary
  (Added/Changed/Fixed) plus a *Migrating from remake2* note pointing at
  the skill's `references/remake2_to_remake3.md`. Surfaced on the docs site
  as `docs/changelog.md` via a `pymdownx.snippets` include (no duplication)
  and added to the nav.
- **README as the PyPI long description** — confirm it renders well on
  the project page and points at the docs site and the alpha install
  (`pip install --pre remake`).
- **Coverage corners** — current ~89%; the term-missing report flags
  `util/config`, `util/util`, and the dask/multiproc executor branches.
  Worth a targeted top-up, not a chase to 100%.

## C. Nice-to-have (can slip to 0.8.x / later)

- Beta/RC pre-releases (`0.8.0b0`, `0.8.0rc0`) if the JASMIN dogfood
  surfaces enough to warrant a staged ramp rather than alpha → final.
- Backlog in [todos.md](todos.md) (benchmark-in-CI as a load-bearing job,
  batched completion transactions, eval-query parser, uses-shadowing
  warning, Hypothesis tests, zarr v3 tokens, per-task log file-count
  budget) and [discussion.md](discussion.md) ideas. None block 0.8.0.
- The Node 20 action deprecation is self-resolving (GitHub forces Node 24
  from 2026-06-16); no action needed.

## D. Branch migration: fold `remake3` into `main`

### Current topology (2026-06-15)

- `main` — GitHub's default branch, but stale remake2-era code. 144
  commits behind `remake3`; carries exactly **one** commit not in
  `remake3`: `e8b7c75` "Add queue to slurm stdout output", which edits
  `remake/executor/slurm_executor.py` + `remake/task.py` — the *old*
  `remake/` layout, fully rewritten under `src/remake/` in remake3.
  Irrelevant to remake3; safe to discard.
- `remake2` — the old stable line; carries one commit not in remake3:
  `cdb2e98` "Handle all errors in code comparer." On the old layout, but
  the *intent* (robust error handling in the code comparer) may matter —
  **before migrating, confirm the equivalent is present in remake3's
  `src/remake/util/code_compare.py`**; port if not.
- `remake3` — the real trunk: all current code, docs, CI, the `0.8.0a0`
  tag history. This is what `main` should become.

remake2 is preserved as a branch (and the old code lives in git history),
so nothing is lost by making `main` track remake3's content.

### Recommended approach: fast-forward `main` to `remake3`

A merge is the wrong tool — `main`'s only unique commit is irrelevant
old-layout code, and a merge would drag the deleted `remake/` tree into
conflicts. Instead, move `main` to remake3's tip:

```bash
# after §A is done and the version is bumped on remake3
git checkout main
git reset --hard origin/remake3      # main now == remake3 content
git push --force-with-lease origin main
```

This discards `e8b7c75` (intended). `--force-with-lease` guards against
clobbering an unseen remote update. If branch protection on `main`
forbids force-push, temporarily lift it or do the equivalent via a PR
that brings remake3's tree wholesale.

### Reference updates that must accompany the migration

The working-branch name is hard-coded in several places; switch them from
`remake3` to `main`:

- `.github/workflows/docs.yml` — `on.push.branches: [remake3]` and the
  deploy job's `if: github.ref == 'refs/heads/remake3'`.
- `.github/workflows/ci.yml` — `on.push.branches: [remake3, remake2,
  main]` (drop `remake3`; keep `main`, optionally `remake2`).
- `mkdocs.yml` — `edit_uri: edit/remake3/docs/` → `edit/main/docs/`; and
  the docs use `tree/remake3/...` / `blob/remake3/...` GitHub links in
  several pages and skill references — grep and update.
- **PyPI trusted publisher** — branch-independent (keyed on workflow file
  + environment), so no change needed; tags still trigger it.
- **GitHub environments / Pages**:
  - `github-pages` deployment-branch policy currently allows `main` +
    `remake3`; once deploying from `main`, remove the `remake3` policy.
  - `pypi` environment (required-reviewer gate) is branch-independent for
    tag-triggered runs; no change.

### Post-migration cleanup

- Confirm GitHub default branch is `main` (it already is).
- Decide the fate of `remake3`: delete it (history is in `main`) or keep a
  short while as a safety net, then delete. Update the local cached
  `origin/HEAD` (`git remote set-head origin -a`) — it currently points at
  the stale `remake2`.
- The session's git memory ("Main branch for PRs: remake2") becomes
  obsolete; PRs target `main` thereafter.

## E. Code-review findings (2026-06-19)

A full review (design docs → core → executors → CLI → examples → tests;
155 tests passing, ~86% coverage) found **no critical correctness bugs in
the engine**. The architecture matches the design and the layers are
consistent. What follows are the actionable items it surfaced, folded into
the release plan. (The deep SLURM/migration bugs were already shaken out by
the JASMIN beta — see §A.1.)

### Solid (recorded so we don't second-guess it later)

- Lazy rule-level model is cleanly implemented; the 1e6-task scaling
  claims are backed by `tests/benchmarks/`.
- Rerun propagation (element-wise vs. conservative `_same_matrix`) is
  mirrored correctly in three places — planner, `upstream_failed`
  (executors), and the SLURM `aftercorr`/`afterok` wiring.
- Sidecar result files (`metadata/sidecar.py` + `ingest_sidecars`) are a
  correct, benchmark-validated fix for the measured SQLite livelock.
- Scope analysis (`core/scope.py`) and the `io_hash`/`uses_hash`/run-code
  triad close the remake2 change-detection gaps.
- CLI introspection verbs (`why`, `info --reasons`, `info -F` dedup,
  `lint`, `rule-dag`) share a single `plan()` rather than re-planning.

### Newly-surfaced blocking items (promoted into §A's DoD)

1. **No top-level `RemakeError` handler in the CLI.** `remake_cmd`
   (`remake_cmd.py`, the `remake_cmd` entry) has no `try/except
   RemakeError`, so user-facing errors (a `-Q` query matching >1 task in
   `task-info`/`task-log`, an unknown rule, a bad query) surface as full
   tracebacks. This **subsumes** the §B `why`-multi-match nit (that
   specific case is already fixed, but the missing handler remains for the
   other single-task commands). Fix:
   ```python
   try:
       return parser.dispatch()
   except RemakeError as e:
       if getattr(args, 'debug_exception', False):
           raise
       print(f'error: {e}', file=sys.stderr)
       return 2
   ```
   Also matches the `todos.md` "Smaller debts" entry. Add a test asserting
   clean `error:` output + exit code 2 (none exists today).

2. **`-I` flag collision.** `--info`/`-I` is defined on the global parser
   (`remake_cmd.py:171`) and `--ignore-code-changes`/`-I` on the `run`
   subcommand (`remake_cmd.py:188`). argparse doesn't error (different
   parsers), but `-I` means *info-logging* before the subcommand and
   *ignore-code-changes* after it — so `remake -I run pipeline.py` silently
   does the wrong thing. Give `--ignore-code-changes` a distinct short flag
   (or drop the short form). Part of the stable CLI surface, so settle it
   before tagging.

3. **`check_outputs='fallback'` default silently swallows code changes.**
   The strongest open design concern (logged in `discussion.md`,
   2026-06-18 `theta_e_analysis` case): under the default `fallback` mode,
   `set-state --pending` + edit + `run` re-adopts the stale on-disk output
   without running the new code. This is a **default-behaviour decision**
   far cheaper to make before 1.0 than after. Decide for 0.8.0: flip the
   default to `'never'` (adoption becomes explicit, e.g. `set-state -Q True
   --success --check-outputs`), or at minimum loudly report adoptions. Pin
   the decision with a test on the `set-state --pending` re-adoption path.

### Should-do (0.8.0 or fast-follow — added to §B)

- **Bound and message-match `retry_lock_commit`** (`sqlite3_backend.py:58`).
  It catches *any* `OperationalError` (not just "database is locked" — also
  "no such table", disk-full, malformed schema) and retries forever with
  growing backoff, so a genuine error becomes a silent hang. Match on the
  lock message, re-raise others, cap attempts. The sidecar design removed
  the high-concurrency pressure, so this is robustness, not the old
  livelock. `discussion.md` already flags it as "the wrong primitive" for
  the optimistic-write idea.
- **Delete dead code** (`util/config.py` `Config` — 29% coverage, the
  repo's lowest; `util/util.py` `sysrun`/`format_path`/`Capturing`).
  Exported but unused in src/tests/examples; a remake2 carryover
  superseded by plain-dict config. The `todos.md` dead-code section sets
  the precedent ("do not leave it to mislead readers"); removing them lifts
  coverage cheaply. Otherwise wire them in.
- **Coverage corners** (already in §B): the upstream-failure-skip branches
  in `multiproc_executor.py` (77%) / `dask_executor.py` (74%), plus the new
  CLI error-path test from item 1.

### Minor / nits (no release gate)

- **`Task.key` doc drift:** `remake3_design.md` (~line 392) specifies
  `f'{rule.fn.__name__}:{kwargs!r}'`, but the implementation (`task.py:42`)
  uses `self.rule.name` + sorted-kwargs repr. The implementation is
  *better* (honours the new `name=` override; sorts kwargs for stability) —
  fix the doc, not the code.
- `load_module` appends to `sys.path` on every call without dedup
  (`loader/__init__.py`) — unbounded growth across repeated loads.
- `eval`-based query filter (`planner.py:33`) with `{'__builtins__': {}}`
  is escapable in principle; low risk for a local CLI on self-authored
  queries. `todos.md` already defers the restricted-ops parser to "CLI
  work" — fine for 0.8.x.
- `_default_nproc` can return `None` (`multiproc_executor.py:40`) if
  `os.cpu_count()` is `None`; harmless (`ProcessPoolExecutor` treats it as
  cpu_count) but `or 1` would be clearer.
- `CodeComparer` has the only `# TODO` in src (`code_compare.py:55`, a
  `RecursionError` re-raise with a stray `print`).
- `ZarrStore.is_complete()` checks `.zmetadata` (zarr v2); v3 uses
  `zarr.json` — already in `todos.md`, contained by the `zarr<3` env pin.

## Definition of done for 0.8.0

- [x] A real pipeline migrated and run end-to-end on JASMIN under remake3
      (mcs_prime equivalence PASS 2026-06-17/18 — see §A.1).
- [ ] Docs reconciled with shipped CLI/API; `mkdocs build --strict` green.
- [x] Top-level `except RemakeError` handler in the CLI (§E.1), with a test
      asserting clean `error:` output + exit 2. **Done 2026-06-19.**
- [x] `-I` flag collision resolved (§E.2) — dropped the `-I` short form from
      `--ignore-code-changes` (long form only; the symmetric `-T/-D/-I/-W`
      logging convention keeps `-I`). **Done 2026-06-19.**
- [x] `check_outputs` default decided and pinned with a test (§E.3) — flipped
      to `'never'`; `'fallback'` is now the opt-in migration mode. **Done
      2026-06-19.**
- [x] CHANGELOG / release notes written. **Done 2026-06-19** (`CHANGELOG.md`
      + docs-site `changelog.md` include).
- [ ] `main` fast-forwarded to remake3; branch refs in workflows/mkdocs
      updated; Pages branch policy trimmed.
- [ ] Version bumped to `0.8.0`; tagged `v0.8.0`; release workflow
      published to PyPI (approve the `pypi` environment gate).
- [ ] `pip install remake` (no `--pre`) resolves to `0.8.0`.
