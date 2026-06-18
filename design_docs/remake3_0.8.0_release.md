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
- **CHANGELOG / release notes** for 0.8.0 — there is none yet. A
  clean-break "what changed since 0.6.3 / from remake2" summary; the
  migration guide content already exists in the skill references.
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

## Definition of done for 0.8.0

- [ ] A real pipeline migrated and run end-to-end on JASMIN under remake3.
- [ ] Docs reconciled with shipped CLI/API; `mkdocs build --strict` green.
- [ ] `why`-multi-match (and any other dogfood-found) CLI nits fixed.
- [ ] CHANGELOG / release notes written.
- [ ] `main` fast-forwarded to remake3; branch refs in workflows/mkdocs
      updated; Pages branch policy trimmed.
- [ ] Version bumped to `0.8.0`; tagged `v0.8.0`; release workflow
      published to PyPI (approve the `pypi` environment gate).
- [ ] `pip install remake` (no `--pre`) resolves to `0.8.0`.
