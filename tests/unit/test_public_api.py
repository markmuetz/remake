"""The declared public API (`remake.__all__`) agrees with docs/api/."""
import re
from pathlib import Path


import remake

API_DOCS = Path(__file__).parents[2] / 'docs' / 'api'


def documented_names():
    """Return the set of names given as `::: remake.X` directives in docs/api/."""
    names = set()
    for md in API_DOCS.glob('*.md'):
        names.update(re.findall(r'^::: remake\.(\w+)\s*$', md.read_text(), re.M))
    return names


def test_all_names_importable():
    """Every name in `__all__` is an attribute of `remake`."""
    missing = [n for n in remake.__all__ if not hasattr(remake, n)]
    assert not missing
    assert len(set(remake.__all__)) == len(remake.__all__)


def test_all_matches_documented_api():
    """`__all__` equals the names documented under docs/api/."""
    documented = documented_names()
    assert documented
    exported = set(remake.__all__)
    assert exported == documented, (
        f'exported but undocumented: {sorted(exported - documented)}; '
        f'documented but not exported: {sorted(documented - exported)}'
    )


def test_documented_names_are_exported():
    """Everything documented is in `__all__` (the reverse may lag)."""
    assert documented_names() <= set(remake.__all__)
