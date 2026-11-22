# Configuration

remake reads configuration from several layers, merged from lowest to highest
precedence:

| # | Layer | Where |
|---|---|---|
| 1 | built-in defaults | in remake (today: the SLURM defaults of JASMIN LOTUS2) |
| 2 | the selected **profile** | shipped with remake, or `[profiles.NAME]` in a config file |
| 3 | **user file** | `$XDG_CONFIG_HOME/remake/config.toml` (default `~/.config/remake/config.toml`) |
| 4 | **project file** | `remake.toml` next to the remakefile (commit it) |
| 5 | `Remake(config=...)` | in the remakefile |
| 6 | `@rule(config=...)` | per rule: `slurm` options (merged by the SLURM executor) and `checksum_outputs`; other executors ignore it |

The format is TOML. **Dicts merge recursively; lists and scalars replace.**
There is no way to delete an inherited key; override it instead.

**Unknown keys are errors**, naming the file and the key. (`Remake(config=)`
only warns, so programmatic callers can keep their own keys there.)

## Keys

```toml
external_inputs_hint = "Run make_data.py first."   # project file only
checksum_outputs = false                            # project file only

[slurm]            # any sbatch long option: partition, qos, time, mem, account, ...
mem = "8G"

[multiproc]
nproc = 8

[dask]
scheduler = "tcp://host:8786"
nproc = 16

[resources]
capture = true     # RSS sampling
rss_interval = 0.1

[history]          # project file only
keep = 10          # executions kept per task in the run history (default 10)
```

The user file is per-user machine/account settings, so it accepts only
`slurm`, `multiproc`, `dask`, `resources`, `profile` and `profiles`.
Pipeline settings (`external_inputs_hint`, `checksum_outputs`) belong in the
project file or the remakefile.

## Profiles

A profile is a named config table, applied *below* the user and project files
(a defaults-only layer). Define your own in either file:

```toml
profile = "hpc"          # select it by default (see below)

[profiles.hpc.slurm]
partition = "long"
```

Select a profile, highest priority first:

1. `remake --profile NAME run pipeline.py`
2. the `REMAKE_PROFILE` environment variable
3. `profile = "NAME"` in the project file
4. `profile = "NAME"` in the user file

An unknown name is an error listing the available profiles. A user or project
`[profiles.NAME]` with the same name as a shipped profile is merged over it.

### The `jasmin` profile

Ships with remake: JASMIN LOTUS2 defaults (`partition`, `qos`, `time`, `mem`).
It does **not** set `account`, which is per-user. Put yours in the user file:

```toml
# ~/.config/remake/config.toml
profile = "jasmin"

[slurm]
account = "my_group_workspace"
```

With the `jasmin` profile active, the SLURM executor refuses to submit if no
`slurm.account` is set anywhere, and tells you where to put it.

The *contents* of shipped profiles are outside remake's compatibility
guarantee: they track the cluster and can change in any release.

### Built-in SLURM defaults (to be removed at 1.0)

For now the built-in layer carries the JASMIN LOTUS2 SLURM values, so existing
pipelines keep working. If you run the SLURM executor with no profile and no
`partition` set anywhere, remake logs a warning; use `--profile jasmin` or set
the slurm options explicitly. The built-in defaults will be removed at 1.0.

## Inspecting the result

```bash
remake config pipeline.py          # effective config, per-key source layer
remake --profile jasmin config pipeline.py --json
```

It prints the selected profile, the files consulted (found or not), and each
key with the layer that set it. Each run also records its effective config
in the metadata database and logs the executor, `nproc` and profile at start.

## Config never causes reruns

Configuration, including profile contents and per-rule config, is never part
of change detection: editing any config file, or a rule's `config=`, does not
rerun tasks.
