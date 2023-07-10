"""
A small mixin to lock attribute and method access at runtime.

Sometimes we wish to restrict users of pyiron from changing certain things past certain stages of object lifetime, e.g.
the input of jobs should only be changed before it is run, but still need to be able to change them internally.  This
can be implemented with :class:`~Lockable` and the decorator :func:`~sentinel`.  It should be thought of as a well
defined escape hatch that is rarely necessary.  Users should never be expected to unlock an object ever again after it
has been locked by them or pyiron.

The context manager functionality is implemented in a separate class rather than directly on Lockable to conserve dunder
name space real estate and let subclasses be context managers on their own.
"""

from abc import ABC
from functools import wraps

from pyiron_base.interfaces.has_groups import HasGroups

class Locked(Exception):
    pass

def sentinel(meth):
    @wraps(meth)
    def f(self, *args, **kwargs):
        if self.read_only:
            raise Locked("Object is currently locked!  Use unlocked() if you know what you are doing.")
        else:
            return meth(self, *args, **kwargs)
    return f

class _UnlockContext:
    __slots__ = ("owner",)

    def __init__(self, owner):
        self.owner = owner

    def __enter__(self):
        self.owner.read_only = False
        return self.owner

    def __exit__(self, *_):
        self.owner.lock()
        return False # never handle exceptions

def _iterate_lockable_subs(lockable_groups):
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
    `super().__init__` or place this class before them in the inheritance order.

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
    >>>    l.clear()
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
    dict
    >>> d['a']['c'] = 23
    >>> d['a']['c']
    23

    Where as the second will be locked from now on

    >>> type(d['b'])
    LockGroupDict
    >>> d['b']['c'] = 23
    Traceback (most recent call last):
        ...
    lockable.Locked: Object is currently locked!  Call unlock() if you know what you are doing.
    >>> d['b']['c']
    1

    but we can unlock it as usual

    >>> with d.unlocked() as dopen:
    ...   dopen['b']['d'] = 23
    >>> d['b']['d']
    23
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        object.__setattr__(self, "_read_only", False)

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
        self._read_only = value
        if changed:
            if value:
                self._on_lock()
            else:
                self._on_unlock()

    def _on_lock(self):
        if isinstance(self, HasGroups):
            for it in _iterate_lockable_subs(self):
                it.lock()

    def _on_unlock(self):
        for it in _iterate_lockable_subs(self):
            it.read_only = False

    def lock(self):
        """
        Set :attr:`~.read_only`.
        """
        self.read_only = True

    def unlocked(self) -> _UnlockContext:
        """
        Unlock the object temporarily.

        Context manager returns this object again.
        """
        return _UnlockContext(self)
