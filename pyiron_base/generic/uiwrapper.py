import inspect

def user_facing(method):
    method.user_facing = True
    return method

class UIWrapper:

    __slots__ = ["_delegate", "_public"]

    def __init__(self, delegate, public=None):
        self._delegate = delegate
        self._public   = getattr(delegate, "_user_facing", [])
        if public is not None:
            self._public += public
        members = inspect.getmembers(self._delegate)
        for name, attr in members:
            if inspect.ismethod(attr) and hasattr(attr, "user_facing"):
                self._public.append(name)
        # if no user_facing are found, give full access to the wrapped instance
        if len(self._public) == 0:
            self._public = list(members.keys())

    def __getattr__(self, name):
        if name in self._public:
            return getattr(self._delegate, name)
        else:
            raise AttributeError("'{}' object has no attribute '{}'".format(self._delegate.__class__, name))

    def __setattr__(self, name, value):
        if name in self.__slots__:
            object.__setattr__(self, name, value)
        elif name in self._public:
            return setattr(self._delegate, name, value)
        else:
            raise AttributeError("'{}' object has no attribute '{}'".format(self._delegate.__class__, name))

    def __dir__(self):
        return self._public

    @property
    def wrapped(self):
        return self._delegate
