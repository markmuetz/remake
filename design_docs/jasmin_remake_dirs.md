# remake directories under `~/projects` (JASMIN)

Survey generated 2026-07-02. Found 38 `.remake` directories under `/home/users/mmuetz/projects`.

## Detection method

The version is identified by the metadata store inside each `.remake` dir. Fastest
discriminator is the DB filename; the sqlite schema confirms it:

| Version | Signature | `task` table distinctive columns |
| --- | --- | --- |
| **remake3** | `remake.db` present | `uses_hash`, `last_run_status`, `last_run_timestamp`, `run_code_id`, `exception` |
| **remake2** | `remake2.db` present (+ `metadata_v5/`) | `code_id`, `requires_rerun` |
| **remake1** (original) | no sqlite DB; file-based `metadata_v4/` or `metadata_v5/` dir | n/a (file-based tracker) |
| indeterminate | empty, or only `slurm/`/`log/` subdirs — no metadata | n/a |

Note: `metadata_v5/` alone does **not** mean remake2 — remake1 also uses it. The
presence of `remake2.db` is what distinguishes remake2 from remake1.

## Counts

- remake3: 11
- remake2: 4
- remake1: 16
- indeterminate: 7

---

## remake3 (11) — `remake.db`

```
hk26/hk26_tracking/.remake
mcs_prime/ctrl/remakefiles/.remake
mcs_prime_stoch_trigger/ctrl/remakefiles/.remake
remake3/examples/.remake
upflo/ctrl/notebooks/dev/.remake
upflo_wp1/.remake
upflo_wp1/remakefiles/.remake
wescon-tools/ctrl/remakefiles/.remake
wescon-tools_remake3/.remake
wescon-tools_remake3/ctrl/remakefiles/.remake
wescon-tools_remake3/ctrl/remakefiles3/.remake
```

## remake2 (4) — `remake2.db` + `metadata_v5/`

```
remake2/.remake
remake2/examples/.remake
remake2/examples/mcs_prime_remakefiles/.remake
mcs_prime_stoch_trigger/ctrl/remakefiles/stoch_trig_remakefiles/.remake
```

## remake1 / original (16) — file-based metadata, no DB

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

## Indeterminate (7) — empty or only `slurm/`/`log/`, no metadata

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
