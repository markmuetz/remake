# remake directories on JASMIN

Survey of `~/projects` generated 2026-07-02; `~/deploy` added and the whole
classification corrected 2026-07-03.

## Detection method

**The DB filename is NOT a reliable version signal.** Both remake3 and the later
remake2 store their metadata in `.remake/remake.db`, so "`remake.db` present ⇒
remake3" is wrong. The reliable discriminator is the **`task` table schema**:

| Version | DB file | `task` table signature |
| --- | --- | --- |
| **remake3** | `remake.db` | has `run_code_id` (+ `uses_code_id`, `io_code_id`); also `meta`/`uses_manifest` tables |
| **remake2 (late)** | `remake.db` | `code_id` + `last_run_timestamp`/`last_run_status`/`exception`; no `run_code_id` |
| **remake2 (early)** | `remake2.db` (+ `metadata_v5/`) | `code_id` + `requires_rerun` |
| **remake1** (original) | none | file-based `metadata_v4/` or `metadata_v5/` dir |
| indeterminate | none | empty, or only `slurm/`/`log/` — no metadata |

The `run_code_id` column is the single decisive signal: remake3's SQLite backend
was first written on 2026-06-11 and used `run_code_id` from its first commit, so
the column exists in every remake3 DB and never in any remake2 DB. (Do not rely on
the `meta`/`uses_manifest` tables alone — treat them as corroborating, not
decisive.) Modification times corroborate cleanly: every `run_code_id` DB is dated
after 2026-06-11; every `code_id`/`last_run_*` DB predates it.

Note: `metadata_v5/` alone does **not** mean remake2 — remake1 also uses it. Only
`remake2.db` (early) distinguishes early remake2 from remake1.

### Re-running the survey

Point this at any root (e.g. `~/projects` or `~/deploy`) to classify every
`.remake` dir by the decisive `task.run_code_id` signal:

```bash
find ~/projects -type d -name .remake 2>/dev/null | sort | while read -r d; do
  if   [ -f "$d/remake.db"  ]; then
    cols=$(sqlite3 "$d/remake.db" "SELECT group_concat(name,' ') FROM pragma_table_info('task');" 2>/dev/null)
    case "$cols" in
      *run_code_id*)      v="remake3" ;;
      *last_run_status*)  v="remake2 (late)" ;;
      *)                  v="remake.db, unknown schema" ;;
    esac
  elif [ -f "$d/remake2.db" ]; then v="remake2 (early)"
  elif ls "$d"/metadata_v[45] >/dev/null 2>&1; then v="remake1"
  else v="indeterminate ($(ls -A "$d" | tr '\n' ',' | sed 's/,$//'))"
  fi
  printf '%-12s %s\n' "$v" "$d"
done
```

---

## `~/projects` — 38 `.remake` directories

### Counts

- remake3: 6
- remake2 (late): 5
- remake2 (early): 4
- remake1: 16
- indeterminate: 7

### remake3 (6) — `remake.db`, `task.run_code_id`

```
hk26/hk26_tracking/.remake
mcs_prime/ctrl/remakefiles/.remake
mcs_prime_stoch_trigger/ctrl/remakefiles/.remake
remake3/examples/.remake
wescon-tools/ctrl/remakefiles/.remake
wescon-tools_remake3/ctrl/remakefiles3/.remake
```

### remake2 late (5) — `remake.db`, `task.code_id` + `last_run_*`

Previously misfiled as remake3 because they use the `remake.db` filename.

```
upflo/ctrl/notebooks/dev/.remake
upflo_wp1/.remake
upflo_wp1/remakefiles/.remake
wescon-tools_remake3/.remake
wescon-tools_remake3/ctrl/remakefiles/.remake
```

### remake2 early (4) — `remake2.db` + `metadata_v5/`

```
remake2/.remake
remake2/examples/.remake
remake2/examples/mcs_prime_remakefiles/.remake
mcs_prime_stoch_trigger/ctrl/remakefiles/stoch_trig_remakefiles/.remake
```

### remake1 / original (16) — file-based metadata, no DB

```
cosmic/ctrl/cmorph/.remake                                          (metadata_v4)
cosmic/ctrl/gpm_imerg/.remake                                       (metadata_v5)
cosmic/ctrl/WP2_analysis/basin_scale/.remake                        (metadata_v4)
cosmic/ctrl/WP2_analysis/orog_precip/.remake                        (metadata_v4)
mcs_prime/ctrl/mcs_env_cond_reviews_remakefiles/.remake             (metadata_v5)
mcs_prime/ctrl/notebooks/dev/.remake                                (metadata_v5)
mcs_prime/ctrl/notebooks/MCS_env_cond_reviews/.remake              (metadata_v5)
mcs_prime/ctrl/remakefiles/dev/.remake                              (metadata_v5)
mcs_prime/ctrl/remakefiles/dev/for_gavin/.remake                    (metadata_v5)
mcs_prime/ctrl/remakefiles/dev/for_xinli/.remake                    (metadata_v5)
mcs_prime_stoch_trigger/ctrl/remakefiles/stoch_trig_remakefiles/dev/.remake (metadata_v5)
mid_lat_EASM/.remake                                                (metadata_v5)
rwp_cat/scripts/.remake                                             (metadata_v5)
usm/.remake                                                         (metadata_v5)
wescon_conv_init/.remake                                            (metadata_v5)
wescon_conv_init/data/nimrod/2012/0601/.remake                      (metadata_v5)
```

### Indeterminate (7) — empty or only `slurm/`/`log/`, no metadata

```
mcs_prime/ctrl/notebooks/.remake                (empty)
mcs_prime/ctrl/notebooks/experimental/.remake   (empty)
mcs_prime/ctrl/scripts/dev/.remake              (slurm/ only)
mcs_prime_stoch_trigger/.remake                 (log/ only)
remake_tests/lots_of_tasks/.remake              (empty)
upflo/.remake                                   (log/ only)
usm/scripts/.remake                             (empty)
```

Paths are relative to `/home/users/mmuetz/projects/`.

---

## `~/deploy` — 7 `.remake` directories

### Counts

- remake3: 0
- remake2 (late): 6
- remake1: 0
- indeterminate: 1

### remake2 late (6) — `remake.db`, `task.code_id` + `last_run_*`

None of these are remake3: they all predate remake3's SQLite backend (2026-06-11)
and their `task` table has `code_id` rather than `run_code_id`.

```
mcs_prime_stoch_trigger/ctrl/remakefiles/.remake
remake/remake/examples/.remake
um_to_healpix/output_testing/.remake
upflo_wp1/remakefiles/.remake
wescon-tools/ctrl/remakefiles/.remake
wescon-tools/.remake
```

### Indeterminate (1) — only `log/`, no metadata

```
PythonProjectRemoteDebug/.remake                (log/ only)
```

Paths are relative to `/home/users/mmuetz/deploy/`.
