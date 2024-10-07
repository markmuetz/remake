from functools import wraps


class RemakeFunc:
    """Class that allows functions to be called, such that they only execute if they or their args have changed.

    >>> remake_func = RemakeFunc()
    >>> def f1(): return 22
    >>> def f2(val): return val**2
    >>> val1 = remake_func(f1)
    >>> val2 = remake_func(f2, val1)
    >>> print(val2)
    484
    """
    def __init__(self):
        self.func_uniq_sign_res = {}

    def __call__(self, func, *args, **kwargs):
        func_uniq_sign = (func.__code__.co_code, func.__code__.co_consts, f'{repr(args)} - {repr(kwargs)}')
        func_uniq_sign_res = self.func_uniq_sign_res.get(func.__name__, None)
        if func_uniq_sign_res and func_uniq_sign_res[0] == func_uniq_sign:
            return func_uniq_sign_res[1]
        self.func_uniq_sign_res[func.__name__] = (func_uniq_sign, func(*args, **kwargs))
        return self.func_uniq_sign_res[func.__name__][1]

    def __repr__(self):
        return f'RemakeFunc({list(self.func_uniq_sign_res.keys())})'

    def __str__(self):
        return repr(self)


class RemakeDecorator:
    """Decorator class that allows functions to be called.

    >>>

    """
    def __init__(self):
        self.remake_func = RemakeFunc()

    def __call__(self, func):
        @wraps(func)
        def inner_func(*args, **kwargs):
            return self.remake_func(func, *args, **kwargs)
        return inner_func

    def __repr__(self):
        return f'RemakeDec({repr(self.remake_func)})'

    def __str__(self):
        return repr(self)


# REALLY experimental. Could be used to build up a task DAG.
class DeferredExec:
    def __init__(self, remake_func, func, args, kwargs):
        self.remake_func = remake_func
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def execute(self):
        args = []
        for arg in self.args:
            if isinstance(arg, DeferredExec):
                args.append(arg.execute())
            else:
                args.append(arg)
        kwargs = {}
        for k, v in self.kwargs.items():
            if isinstance(v, DeferredExec):
                kwargs[k] = v.execute()
            else:
                kwargs[k] = v
        return self.remake_func(self.func, *args, **kwargs)


class DeferredRemakeDec:
    def __init__(self):
        self.remake_func = RemakeFunc()

    def __call__(self, func):
        @wraps(func)
        def inner_func(*args, **kwargs):
            return DeferredExec(self.remake_func, func, args, kwargs)
        return inner_func

    def __repr__(self):
        return f'RemakeDec({repr(self.remake_func)})'

    def __str__(self):
        return repr(self)
