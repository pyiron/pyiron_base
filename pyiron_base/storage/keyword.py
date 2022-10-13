from pyiron_base.interfaces.object import HasStorage

class KeywordInput(HasStorage):
    """
    Base class for keyword based input structures, that know how write themselves to HDF5.

    Derive from this class and add a class level attribute `_defaults`, which can be any mapping from allowed keyword
    names to their default values.

    Example:

    >>> from pyiron_base.storage.datacontainer import DataContainer
    >>> class MyOptions(KeywordInput):
    ...     '''
    ...     Stores the options to my beautiful code.
    ...
    ...     Attributes:
    ...         run_fast (bool): run fast or slow?
    ...         foo_level (float): amount of foo-iness
    ...     '''
    ...
    ...     _defaults = {
    ...             'run_fast': False,
    ...             'foo_level': .7,
    ...             'suboptions': DataContainer({'bar': 23})
    ...     }
    >>> opt = MyOptions()
    >>> opt.run_fast
    False
    >>> opt['foo_level'] = .1
    >>> opt.foo_level
    0.1
    >>> opt['invalid_option']
    Traceback (most recent call last):
        ...
    KeyError: 'invalid_option'
    >>> opt.yet_another_option
    Traceback (most recent call last):
        ...
    AttributeError: yet_another_option
    >>> opt.suboptions.bar
    23
    >>> opt.suboptions.name = 'Donald'
    >>> opt.suboptions
    DataContainer({'bar': 23, 'name': 'Donald'})
    """

    _defaults = {}
    _finalized = False

    def __init__(self, *args, **kwargs):
        if "group_name" not in kwargs:
            kwargs["group_name"] = self.__class__.__name__.lower()
        super().__init__(*args, **kwargs)
        self._finalized = True
        for k, v in self._defaults.items():
            self.storage[k] = v

    def __getitem__(self, name):
        if name in self._defaults:
            return self.storage[name]
        else:
            raise KeyError(name)

    def __setitem__(self, name, value):
        if name in self._defaults:
            self.storage[name] = value
        else:
            raise KeyError(name)

    def __getattr__(self, name):
        if name in self._defaults:
            return self.storage[name]
        else:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        if not self._finalized:
            object.__setattr__(self, name, value)
        elif name in self._defaults:
            self.storage[name] = value
        else:
            raise AttributeError(name)
