import functools


def remake_required(*, on_func_change=True, depends_on=tuple()):
    def wrapped(func):
        @functools.wraps(func)
        def wrapped_inner(*args, **kwargs):
            return func(*args, **kwargs)

        wrapped_inner.is_remake_wrapped = True
        wrapped_inner.remake_func = func
        wrapped_inner.remake_on_func_change = on_func_change
        wrapped_inner.depends_on = depends_on

        return wrapped_inner

    return wrapped
