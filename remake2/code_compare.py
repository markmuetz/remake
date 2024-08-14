import ast
from itertools import zip_longest
from typing import Union


def dedent(s):
    lines = s.split('\n')
    min_leading_space = min([len(l) - len(l.lstrip()) for l in lines])
    return '\n'.join(l[4:] for l in lines)


# https://stackoverflow.com/a/66733795/54557
def compare_ast(node1: Union[ast.expr, list[ast.expr]], node2: Union[ast.expr, list[ast.expr]]) -> bool:
    if type(node1) is not type(node2):
        return False

    if isinstance(node1, ast.AST):
        for k, v in vars(node1).items():
            if k in {"lineno", "end_lineno", "col_offset", "end_col_offset", "ctx"}:
                continue
            if not compare_ast(v, getattr(node2, k)):
                return False
        return True

    elif isinstance(node1, list) and isinstance(node2, list):
        return all(compare_ast(n1, n2) for n1, n2 in zip_longest(node1, node2))
    else:
        return node1 == node2



class CodeComparer:
    def __init__(self):
        self.compare_cache = {}

    def __call__(self, code1, code2):
        if code1 == code2:
            return True
        key = tuple(sorted([code1, code2]))
        if key in self.compare_cache:
            return self.compare_cache[key]
        res = compare_ast(ast.parse(code1), ast.parse(code2))
        self.compare_cache[key] = res
        return res



