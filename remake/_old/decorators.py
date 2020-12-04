import functools
from remake.flags import RemakeOn


def remake_task_control(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        return func(*args, **kwargs)

    wrapped.is_remake_task_control = True
    return wrapped


def remake_required(*, remake_on=RemakeOn.ANY_METADATA_CHANGE, depends_on=tuple()):
    """Decorator to add extra information to a function

    Note, if neither of the keyword arguments is given, this will raise an Exception.
    See e.g. https://stackoverflow.com/questions/653368 for why."""
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
