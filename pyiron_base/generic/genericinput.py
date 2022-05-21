from pyiron_base.generic.object import HasStorage


class GenericInput(HasStorage):
    """
    Template for generic input class

    Example:

    >>> class MDInput(GenericInput):
    >>>     '''Input parameters for MD calculations.'''
    >>>     temperature = InputField(
    >>>         name="temperature", data_type=float, doc="Run at this temperature"
    >>>     )
    >>>     steps = InputField(
    >>>         name="steps", data_type=int, doc="How many steps to integrate"
    >>>     )
    >>>     timestep = InputField(
    >>>         name="timestep", data_type=float, default=1e-15, doc="Time step for the integration in fs"
    >>>     )

    Then doing

    >>> md = MDInput()
    >>> md?

    in an IPython shell gives you

    ```
    Type:            MDInput
    String form:     <MDInput object at 0x7fb1808ffca0>
    File:            file_location
    Docstring:
    MDInput
    -------
    Input parameters for MD calculations.

    Attributes:
    -----------
            temperature (<class 'float'>): Run at this temperature
            steps (<class 'int'>): How many steps to integrate
            timestep (<class 'float'>): Time step for the integration in fs
    Class docstring: Input parameters for MD calculations.
    ```
    """

    def __init__(self):
        super().__init__()
        self.__doc__ = self._make_doc()

    def _make_doc(self):
        cn = self.__class__.__name__
        doc = cn + "\n" + "-" * len(cn) + "\n"
        if self.__doc__ is not None:
            doc += self.__doc__ + "\n"
        doc += "\nAttributes:\n"
        doc += "-----------"
        for k, v in self.__class__.__dict__.items():
            if isinstance(v, InputField):
                doc += f"\n\t{k}"
                if v.str_type is not None:
                    doc += f" ({v.str_type})"
                doc += f": {v.__doc__}"
        return doc


class InputField:
    """
    Create input field of GenericInput (v.s.).

    Example:

    >>> class SomeClass:
    >>>     '''example class that you might want to use'''
    >>>     attribute = InputField(
    >>>         name="some attribute", data_type=float, doc="an interesting attribute"
    >>>     )
    Then doing

    >>> example = SomeClass()
    >>> example?

    in an IPython shell gives you

    ```
    Type:            SomeClass
    String form:     <SomeClass object at 0x7fb1808ffca0>
    File:            file_location
    Docstring:
    SomeClass
    -------
    example class that you might want to use

    Attributes:
    -----------
            some attribute (<class 'float'>): an interesting attribute
    ```
    """

    def __init__(
        self,
        name,
        doc,
        data_type=None,
        default=None,
        required=True,
        fget=lambda x: x,
        fset=lambda x: x,
    ):
        """
        Args:
            name (str): Name of the attribute
            doc (str): Docstring of the attribute
            data_type (None/type/list/tuple): Data type. If set, setter function
                checks the input. If the check should not take place, set `None`.
                If several types should be specified, use `list` or `tuple` to
                set all types.
            default (None/data_type): Default value
            required (bool): Whether the value is required to be defined
            fget (function): Getter function
            fset (function): Setter function
        """
        self._name = name
        self.fget = fget
        self.fset = fset
        self.__doc__ = doc
        self._type = data_type
        self._default = default
        self._required = required

    def __get__(self, instance, owner=None):
        value = instance.storage.get(self._name, default=None)
        if value is None:
            value = self._default
        if value is None and self.required:
            raise AttributeError(f'"{self._name}" not defined (yet)!')
        return self.fget(value)

    def _check_type(self, value):
        if self._type is None:
            return False
        elif isinstance(self._type, tuple) or isinstance(self._type, list):
            return all([not isinstance(value, tt) for tt in self._type])
        else:
            return not isinstance(value, self._type)

    def __set__(self, instance, value):
        if self._check_type(value):
            raise TypeError(f"{self._name} must be of type {self.str_type}!")
        instance.storage[self._name] = self.fset(value)

    @property
    def str_type(self):
        if isinstance(self._type, list) or isinstance(self._type, tuple):
            return "/".join([str(tt) for tt in self._type])
        else:
            return self._type
