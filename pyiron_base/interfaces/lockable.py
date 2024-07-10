"""
A small mixin to lock attribute and method access at runtime.

Sometimes we wish to restrict users of pyiron from changing certain things past certain stages of object lifetime, e.g.
the input of jobs should only be changed before it is run, but still need to be able to change them internally.  This
can be implemented with :class:`~Lockable` and the decorator :func:`~sentinel`.  It should be thought of as a well
defined escape hatch that is rarely necessary.  Users should never be expected to unlock an object ever again after it
has been locked by them or pyiron.

The context manager functionality is implemented in a separate class rather than directly on Lockable to conserve dunder
name space real estate and let subclasses be context managers on their own.

Through out the code inside methods of `Lockable` will use `object.__setattr__`
and `object.__getattribute__` to avoid any overloading attribute access that
sibling classes may bring in.
"""

import warnings
from contextlib import nullcontext
from functools import wraps
from typing import Literal, Optional

from pyiron_base.interfaces.has_groups import HasGroups


class Locked(Exception):
    pass


class LockedWarning(UserWarning):
    pass


def sentinel(meth):
    """
    Wrap a method to fail if `read_only` is `True` on the owning object.

    Use together with :class:`Lockable`.

    Args:
        meth (method): method to call if `read_only` is `False`.

    Returns:
        wrapped method
    """

    def dispatch_or_error(self, *args, **kwargs):
        try:
            method = object.__getattribute__(self, "_lock_method")
        except AttributeError:
            method = None
        if method not in ("error", "warning"):
            method = "error"
        if self.read_only and method == "error":
            raise Locked(
                "Object is currently locked!  Use unlocked() if you know what you are doing."
            )
        elif self.read_only and method == "warning":
            warnings.warn(
                f"{meth.__name__} called on {type(self)}, but object is locked!",
                category=LockedWarning,
            )
        return meth(self, *args, **kwargs)

    # if sentinel is applied to __setattr__ we must ensure that `read_only`
    # stays available, otherwise we can't unlock again later
    if meth.__name__ == "__setattr__":

        @wraps(meth)
        def f(self, *args, **kwargs):
            if len(args) > 0:
                target = args[0]
            else:
                target = kwargs["name"]
            if target in ("read_only", "_read_only"):
                return meth(self, *args, **kwargs)
            return dispatch_or_error(self, *args, **kwargs)

    else:
        f = wraps(meth)(dispatch_or_error)
    return f


class _UnlockContext:
    """
    Context manager that unlocks and relocks a :class:`Lockable`.

    This is an implementation detail of :class:`Lockable`.
    """

    __slots__ = ("owner",)

    def __init__(self, owner):
        self.owner = owner

    def __enter__(self):
        self.owner.read_only = False
        return self.owner

    def __exit__(self, *_):
        self.owner.lock()
        return False  # never handle exceptions


def _iterate_lockable_subs(lockable_groups):
    """
    Yield sub nodes and groups that are lockable.  Recurse into groups.

    If the given object is not a :class:`HasGroups` yield nothing.
    """
    if not isinstance(lockable_groups, HasGroups):
        return

    subs = lockable_groups.list_all()
    for n in subs["nodes"]:
        node = lockable_groups[n]
        if isinstance(node, Lockable):
            yield node

    for g in subs["groups"]:
        group = lockable_groups[g]
        yield group
        yield from _iterate_lockable_subs(g)


class Lockable:
    """
    A small mixin to lock attribute and method access at runtime.

    The mixin maintains an :attr:`~.read_only` and offers a context manager to temporarily unset it.  It does **not**
    restrict access to any attributes or methods on its own.  Instead sub classes are expected to mark methods they wish
    protected with :func:`.sentinel`.  Wrapped methods will then raise :exc:`.Locked` if :attr:`~.read_only` is set.

    If the subclass also implements :class:`.HasGroups`, locking it will iterate over all nodes and (recursively)
    groups and lock them if possible and vice-versa for unlocking.

    Once an object has been locked it should generally not be expected to be (permanently) unlocked again, especially
    not explicitely by the user.

    Subclasses need to initialize this class by calling the inherited `__init__`, if explicitely overriding it.  When
    not explicitely overriding it (as in the examples below), take care that either the other super classes call
    `super().__init__` or place this class before them in the inheritance order.  Also be sure to initialize it before
    using methods and properties decorated with :func:`.sentinel`.

    Subclasses may override :meth:`_on_lock` and :meth:`_on_unlock` if they wish to customize locking/unlocking
    behaviour, provided that they call `super()` in their overloads.

    Let's start with a simple example; a list that can be locked

    >>> class LockList(Lockable, list):
    ...   __setitem__ = sentinel(list.__setitem__)
    ...   clear = sentinel(list.clear)
    >>> l = LockList([1,2,3])
    >>> l
    [1, 2, 3]

    Deriving adds the read only flag

    >>> l.read_only
    False

    While it is not set, we may mutate the object

    >>> l[2] = 4
    >>> l[2]
    4

    Once it is locked, the wrapped methods will raise errors

    >>> l.lock()
    >>> l.read_only
    True
    >>> l[1]
    2
    >>> l[1] = 4
    Traceback (most recent call last):
        ...
    lockable.Locked: Object is currently locked!  Use unlocked() if you know what you are doing.

    You can lock an object multiple times to no effect

    >>> l.lock()

    From now on every modification should be done with the :meth:`~.unlocked()` context manager.  It returns the
    unlocked object itself.

    >>> with l.unlocked():
    ...   l[1] = 4
    >>> l[1]
    4
    >>> with l.unlocked() as lopen:
    ...   print(l is lopen)
    ...   l[1] = 4
    True

    :func:`~.sentinel` can be used for methods, item and attribute access.

    >>> l.clear()
    Traceback (most recent call last):
        ...
    lockable.Locked: Object is currently locked!  Use unlocked() if you know what you are doing.
    >>> with l.unlocked():
    ...   l.clear()
    >>> l
    []

    When used together with :class:`.HasGroups`, objects will be locked recursively.

    >>> class LockGroupDict(Lockable, dict, HasGroups):
    ...   __setitem__ = sentinel(dict.__setitem__)
    ...
    ...   def _list_groups(self):
    ...     return [k for k, v in self.items() if isinstance(v, LockGroupDict)]
    ...
    ...   def _list_nodes(self):
    ...     return [k for k, v in self.items() if not isinstance(v, LockGroupDict)]

    >>> d = LockGroupDict(a=dict(c=1, d=2), b=LockGroupDict(c=1, d=2))
    >>> d.lock()

    Since the first item is a plain dict, it can still be mutated.

    >>> type(d['a'])
    <class 'dict'>
    >>> d['a']['c'] = 23
    >>> d['a']['c']
    23

    Where as the second will be locked from now on

    >>> type(d['b'])
    <class 'lockable.LockGroupDict'>
    >>> d['b']['c'] = 23
    Traceback (most recent call last):
        ...
    lockable.Locked: Object is currently locked!  Use unlocked() if you know what you are doing.
    >>> d['b']['c']
    1

    but we can unlock it as usual

    >>> with d.unlocked():
    ...   d['b']['d'] = 23
    >>> d['b']['d']
    23

    To use this class with properties, simply decorate the setter

    >>> class MyLock(Lockable):
    ...   def __init__(self, foo):
    ...     super().__init__()
    ...     self._foo = foo
    ...   @property
    ...   def foo(self):
    ...     return self._foo
    ...   @foo.setter
    ...   @sentinel
    ...   def foo(self, value):
    ...     self._foo = value
    >>> ml = MyLock(42)
    >>> ml.foo
    42
    >>> ml.foo = 23
    >>> ml.lock()
    >>> ml.foo = 42
    Traceback (most recent call last):
        ...
    lockable.Locked: Object is currently locked!  Use unlocked() if you know what you are doing.

    It's possible to change the errors raised into a warning and allow
    modification by passing `lock_method` to :meth:`~.Lockable.__init__` or
    `method` to :meth:`~.lock`.

    >>> mw = LockList(lock_method="warning")
    >>> mw.append(0)
    >>> mw.lock()
    >>> mw[0] = 1 # will print the warning
    >>> mw[0]
    1

    >>> mw = LockList()
    >>> mw.append(0)
    >>> mw.lock(method='warning')
    >>> mw[0] = 1 # will print the warning
    >>> mw[0]
    1

    """

    def __init__(self, *args, lock_method: str = "error", **kwargs):
        object.__setattr__(self, "_read_only", False)
        object.__setattr__(self, "_lock_method", lock_method)
        super().__init__(*args, **kwargs)

    @property
    def read_only(self) -> bool:
        """
        bool: False if the object can currently be written to

        Setting this value will trigger :meth:`._on_lock` and :meth:`._on_unlock` if it changes.
        """
        return object.__getattribute__(self, "_read_only")

    @read_only.setter
    def read_only(self, value: bool):
        changed = self._read_only != value
        if changed:
            self._read_only = value
            if value:
                self._on_lock()
            else:
                self._on_unlock()

    def _on_lock(self):
        for it in _iterate_lockable_subs(self):
            it.lock()

    def _on_unlock(self):
        for it in _iterate_lockable_subs(self):
            it.read_only = False

    def lock(self, method: Optional[Literal["error", "warning"]] = None):
        """
        Set :attr:`~.read_only`.

        Objects may be safely locked multiple times without further effect.

        Args:
            method (str, either "error" or "warning"): if "error" raise an :class:`.Locked` exception if modification is
                    attempted; if "warning" raise a :class:`.LockedWarning` warning; default is "error" or the value
                    passed to the constructor.

        Raises:
            ValueError: if `method` is not an allowed value
        """
        if method not in ["error", "warning", None]:
            raise ValueError(f"Unrecognized lock method {method}!")
        if method is not None:
            object.__setattr__(self, "_lock_method", method)
        self.read_only = True

    def unlocked(self) -> _UnlockContext:
        """
        Unlock the object temporarily.

        Context manager returns this object again and relocks it after the `with` statement finished.

        .. note:: `lock()` vs. `unlocked()`

            There is a small asymmetry between these two methods.  :meth:`.lock` can only be done once (meaningfully), while :meth:`.unlocked` is a context manager and can be called multiple times.
        """
        if self.read_only:
            return _UnlockContext(self)
        else:
            return nullcontext(self)
