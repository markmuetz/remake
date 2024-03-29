from enum import Flag, auto


class RemakeOn(Flag):
    NOT_NEEDED = 0

    MISSING_INPUT = auto()
    MISSING_OUTPUT = auto()
    OLDER_OUTPUT = auto()
    ANY_FILE_CHANGE = MISSING_INPUT | MISSING_OUTPUT | OLDER_OUTPUT

    NO_TASK_METADATA = auto()
    INPUTS_CHANGED = auto()
    TASK_BYTECODE_CHANGED = auto()
    TASK_SOURCE_CHANGED = auto()
    # DEPENDS_BYTECODE_CHANGED = auto()
    DEPENDS_SOURCE_CHANGED = auto()

    ANY_METADATA_CHANGE = (NO_TASK_METADATA | INPUTS_CHANGED | TASK_BYTECODE_CHANGED | TASK_SOURCE_CHANGED |
                           DEPENDS_SOURCE_CHANGED)
    ANY_STANDARD_CHANGE = ANY_METADATA_CHANGE | MISSING_OUTPUT | OLDER_OUTPUT

    # TODO: Add these in and get them working.
    # BYTECODE_METADATA_CHANGE = (NO_TASK_METADATA | INPUTS_CHANGED | TASK_BYTECODE_CHANGED | DEPENDS_SOURCE_CHANGED)
    # BYTECODE_STANDARD_CHANGE = BYTECODE_METADATA_CHANGE | MISSING_OUTPUT
