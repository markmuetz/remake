# Installation

remake requires **Python 3.10+**.

## From PyPI

```bash
pip install --pre remake
```

!!! note
    remake3 is on PyPI as a pre-release (`0.8.0a0`), so the `--pre` flag is
    required — a plain `pip install remake` currently resolves to the old
    remake2 (`0.6.x`) line.

## From source

```bash
git clone https://github.com/markmuetz/remake
cd remake
pip install -e .
```

### With uv

The project is developed with [uv](https://docs.astral.sh/uv/):

```bash
uv sync            # create the managed .venv with dev dependencies
uv run remake version
```

## Optional dependencies

remake's core has a light footprint (`loguru`, `networkx`). Extra features
pull in extras:

| Extra | Installs | For |
|---|---|---|
| `debug` | `ipdb` | `remake run -X` post-mortem debugging |
| `s3` | `boto3` | S3-backed output tokens |
| `dask` | `distributed` | the (experimental) dask executor |

```bash
pip install "remake[debug]"
```

Scientific pipelines that read/write NetCDF or Zarr will also want
`xarray`, `netCDF4`/`h5netcdf` and `zarr` in your environment (these are not
remake dependencies — they belong to your pipeline).

## Verify

```bash
remake version
```
