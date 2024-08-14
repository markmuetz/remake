import functools
import types
import typing

class RemakeReducedScope(Exception):
    pass


def _call_with_reduced_scope(method, scoped_vars, *args, **kwargs):
    # TODO: problem - this needs to know the class to which the method is attached.
    # so that it can attach scoped_vars to class dependencies.
    _g = globals().copy()
    temp_globals = {}
    remaining_scoped_vars = []
    for v in scoped_vars:
        if isinstance(v, typing.Hashable) and v in _g:
            temp_globals[v] = _g[v]
        else:
            remaining_scoped_vars.append(v)

    used_scoped_vars = []
    # Pass through all modules.
    for k, v in globals().items():
        if isinstance(v, types.ModuleType):
            temp_globals[k] = v
        for scoped_var in remaining_scoped_vars:
            if v is scoped_var:
                temp_globals[k] = v
                used_scoped_vars.append(v)

    for v in used_scoped_vars:
        remaining_scoped_vars.remove(v)

    if remaining_scoped_vars:
        raise

    try:
        globals().clear()
        globals().update(temp_globals)
        return staticmethod(method)(*args, **kwargs)
    except NameError as ne:
        globals().update(_g)
        if ne.name in globals():
            msg = (
                f'{ne.name} is in outer scope but has been restricted\n'
                f'Use @rule_dec([\'{ne.name}\']) to pass through'
            )
        raise RemakeReducedScope(msg)
    finally:
        globals().update(_g)

def rule_dec(method_or_scoped_vars=None):
    if callable(method_or_scoped_vars):
        method = method_or_scoped_vars
        scoped_vars = []
        method.is_rule_dec = True

        @functools.wraps(method)
        def inner(*args, **kwargs):
            return _call_with_reduced_scope(method, scoped_vars, *args, **kwargs)
        return inner
    if method_or_scoped_vars is None:
        scoped_vars = []
    else:
        scoped_vars = method_or_scoped_vars

    def outer(method):
        method.is_rule_dec = True

        @functools.wraps(method)
        def inner(*args, **kwargs):
            return _call_with_reduced_scope(method, scoped_vars, *args, **kwargs)
        return inner
    return outer



