from enum import Flag, auto


class RemakeOn(Flag):
    NOT_NEEDED = 0

    MISSING_INPUT = auto()
    MISSING_OUTPUT = auto()
    OLDER_OUTPUT = auto()

    NO_TASK_METADATA = auto()
    INPUTS_CHANGED = auto()
    TASK_BYTECODE_CHANGED = auto()
    TASK_SOURCE_CHANGED = auto()
    # DEPENDS_BYTECODE_CHANGED = auto()
    DEPENDS_SOURCE_CHANGED = auto()

    ANY_METADATA_CHANGE = (NO_TASK_METADATA | INPUTS_CHANGED | TASK_BYTECODE_CHANGED | TASK_SOURCE_CHANGED |
                           DEPENDS_SOURCE_CHANGED)
