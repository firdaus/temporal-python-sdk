from typing import Dict

from temporal.converter import get_fn_args_type_hints


def test_get_fn_args_type_hints_no_annotations():
    def hello(a, b):
        pass
    hints = get_fn_args_type_hints(hello)
    assert len(hints) == 2
    assert hints[0] is None
    assert hints[1] is None


def test_get_fn_args_type_hints_no_arguments():
    def hello():
        pass
    hints = get_fn_args_type_hints(hello)
    assert len(hints) == 0


def test_get_fn_type_with_annotations():
    def hello(a: str, b: Dict):
        pass
    hints = get_fn_args_type_hints(hello)
    assert len(hints) == 2
    assert hints[0] is str
    assert hints[1] is Dict


def test_get_fn_type_method():
    class Person:
        def hello(self, a: str, b: Dict):
            pass
    hints = get_fn_args_type_hints(Person().hello)
    assert len(hints) == 2
    assert hints[0] is str
    assert hints[1] is Dict

