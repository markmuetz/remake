# API reference

The public API is exported from the top-level `remake` package. Most pipelines
only need `Remake` and `rule`.

## Core

::: remake.Remake

::: remake.rule

::: remake.Rule

::: remake.Task

::: remake.deferrable

::: remake.Defer

## Errors

::: remake.RemakeError

::: remake.ScopeError

::: remake.ScopeWarning

::: remake.SignatureError

## Executors

::: remake.Executor

::: remake.SingleprocExecutor

::: remake.MultiprocExecutor

::: remake.SlurmExecutor

::: remake.DaskExecutor

## Output tokens

::: remake.OutputToken

::: remake.FileToken

::: remake.PathToken

::: remake.ZarrStore

::: remake.S3Object

## Loading & metadata

::: remake.load_remake

::: remake.MetadataManager

::: remake.Sqlite3Backend

::: remake.TaskRecord
