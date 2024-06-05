import pytest

import intake


def test_func_ser():
    class A:
        def get(self):
            def inner():
                return self.x

            return inner

    func = A().get()
    reader = intake.readers.convert.GenericFunc(data=func)
    cat = intake.Catalog()
    with pytest.raises(RuntimeError):
        cat.add_entry(reader)
