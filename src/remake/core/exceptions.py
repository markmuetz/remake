class RemakeError(Exception):
    pass


class RemakeLoadError(RemakeError):
    pass


class RemakeOutputNotCreated(RemakeError):
    pass
