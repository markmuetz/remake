import functools
from remake.flags import RemakeOn


def remake_required(*, remake_on=RemakeOn.ANY_METADATA_CHANGE, depends_on=tuple()):
    def wrapped(func):
        @functools.wraps(func)
        def wrapped_inner(*args, **kwargs):
            return func(*args, **kwargs)

        wrapped_inner.is_remake_wrapped = True
        wrapped_inner.remake_func = func
        wrapped_inner.remake_on = remake_on
        wrapped_inner.depends_on = depends_on

        return wrapped_inner

    return wrapped
