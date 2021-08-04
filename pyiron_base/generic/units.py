import functools


class UnitsDecorator:

    def __init__(self):
        self._units = None
        self._label = None

    def __call__(self, label, units=None):
        if units is not None:
            self._units = units
        self._label = label
        return self.__decorate_to_pyiron

    def __decorate_to_pyiron(self, function):
        @functools.wraps(function)
        def dec(*args, **kwargs):
            return function(*args, **kwargs)
        return dec

# def decorator(func):
# @wraps(func)
# def f(self, *args, **kwargs):
#     units = unit_getter(self)
#     return func(self, *args, **kwargs)

# Quick sketch, unit_getter is something you have to define via a class decorator like you had
