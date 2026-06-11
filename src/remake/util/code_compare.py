import ast
import textwrap
from itertools import zip_longest
from typing import Union

from loguru import logger


def dedent(s):
    return textwrap.dedent(s)


# https://stackoverflow.com/a/66733795/54557
def _compare_ast(
    node1: Union[ast.expr, list[ast.expr]], node2: Union[ast.expr, list[ast.expr]]
) -> bool:
    if type(node1) is not type(node2):
        return False

    if isinstance(node1, ast.AST):
        for k, v in vars(node1).items():
            if k in {"lineno", "end_lineno", "col_offset", "end_col_offset", "ctx"}:
                continue
            if not _compare_ast(v, getattr(node2, k)):
                return False
        return True

    elif isinstance(node1, list) and isinstance(node2, list):
        return all(_compare_ast(n1, n2) for n1, n2 in zip_longest(node1, node2))
    else:
        return node1 == node2


class CodeComparer:
    def __init__(self):
        self.compare_cache = {}

    def __call__(self, code1, code2):
        logger.trace('code1:\n' + code1)
        logger.trace('code2:\n' + code2)
        if code1 == code2:
            logger.trace('code1 == code2')
            return True
        key = tuple(sorted([code1, code2]))
        if key in self.compare_cache:
            logger.trace('already compared')
            return self.compare_cache[key]
        try:
            res = _compare_ast(ast.parse(dedent(code1)), ast.parse(dedent(code2)))
        except SyntaxError:
            # Unparseable source (e.g. bytecode-digest fallbacks mixed with
            # real source): strings differ, so treat as changed.
            res = False
        except RecursionError as re:
            # TODO: investigate how this happens.
            # Seems to be when I invoke the remake cmd from within Ipython.
            print('code1:')
            print(code1)
            print('code2:')
            print(code2)
            # Why not just raise? (to raise first error)
            # If you do this, the debugger will be stuck down the
            # recursion loop, and you cannot inspect vars.
            raise re
        self.compare_cache[key] = res
        return res
