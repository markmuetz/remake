class RemakeError(Exception):
    pass


class RemakeLoadError(RemakeError):
    pass


class SignatureError(RemakeError):
    """Rule function signature does not match its declarations."""

    pass


class ScopeError(RemakeError):
    """Rule function uses undeclared names from outer scope (strict mode)."""

    pass


class RuleGraphError(RemakeError, ValueError):
    """Bad rule dependencies (unknown depends_on name, a cycle). Also a
    ValueError, which is what these were raised as before 0.8.4."""

    pass


class TaskExit(Exception):
    """A task's code raised SystemExit (e.g. a CLI `main()` calling
    `sys.exit`). run_task records the task as failed and raises this in its
    place, so executors treat it as an ordinary task failure instead of the
    whole run exiting silently. Deliberately not a RemakeError: it is a task
    failure, not a usage error."""

    pass


class Defer(Exception):
    """Raised by a `@deferrable` matrix callable to signal that the rule
    cannot be expanded this wave — its task list derives from an upstream
    output that does not yet exist. A control-flow signal, not an error
    (hence not a RemakeError): the planner defers the rule and the replan
    loop / SLURM continuation job retries it once the upstream completes.

    The planner additionally defers a `@deferrable` rule when an upstream is
    *rerunning* this wave (its on-disk output is stale), so the matrix never
    expands from an about-to-be-overwritten output.

    Accepts path strings as context, surfaced to the user to show what is
    blocking resolution.
    """

    def __init__(self, *paths):
        self.paths = [str(p) for p in paths]
        super().__init__(', '.join(self.paths) if self.paths else 'deferred')
