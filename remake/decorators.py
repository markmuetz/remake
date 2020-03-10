import functools


def remake_required(*, on_func_change=True, depends_on=tuple()):
    def wrapped(func):
        @functools.wraps(func)
        def wrapped_f(*args, **kwargs):
            return func(*args, **kwargs)

        wrapped_f.is_remake_wrapped = True
        wrapped_f.remake_func = func
        wrapped_f.remake_on_func_change = on_func_change
        wrapped_f.depends_on = depends_on

        return wrapped_f

    return wrapped
