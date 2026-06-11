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


class MatrixNotReady(RemakeError):
    """Raised by a matrix callable whose required upstream outputs do not yet
    exist. Treated as a deferral signal by the planner, not an error.

    Accepts path strings as context, surfaced to the user to show what is
    blocking resolution.
    """

    def __init__(self, *paths):
        self.paths = [str(p) for p in paths]
        super().__init__(', '.join(self.paths) if self.paths else 'matrix not ready')
