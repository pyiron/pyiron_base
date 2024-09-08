# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

"""
A parent class for managing input to pyiron jobs.
"""

from __future__ import annotations

from abc import ABC, ABCMeta
from typing import TYPE_CHECKING, Optional

from traitlets import HasTraits, MetaHasTraits

from pyiron_base.interfaces.object import HasStorage

if TYPE_CHECKING:
    from pyiron_base.storage.hdfio import ProjectHDFio

__author__ = "Liam Huber"
__copyright__ = (
    "Copyright 2022, Max-Planck-Institut für Eisenforschung GmbH - "
    "Computational Materials Design (CM) Department"
)
__version__ = "0.1"
__maintainer__ = "Liam Huber"
__email__ = "liamhuber@greyhavensolutions.com"
__status__ = "development"
__date__ = "Oct 17, 2022"


class ABCTraitsMeta(MetaHasTraits, ABCMeta):
    """
    Just a bookkeeping necessity for classes that inherit from both `ABC` and `HasTraits`.
    """

    pass


class HasStoredTraits(HasTraits, HasStorage, ABC, metaclass=ABCTraitsMeta):
    """
    A base class for input to pyiron jobs, combining pyiron's `HasStorage` and `traitlets.HasTraits` for ease of access
    and validation/callbacks.

    Child classes can define traits like a normal `HasTraits` child, and these get automatically put in `storage` and
    serialized when `to/from_hdf` is called.

    Attributes:
        read_only (bool): Whether the traits (recursively in case and traits are also of this class) are allowed to be
            updated or are read-only. This attribute is itself read-only, but can be updated with the `lock()` and
            `unlock()` methods. (Default is False, allow both reading and writing of traits.)

    Note:
        If any of your traits are complex objects that `pyiron_base.DataContainer` doesn't already know how to
        serialize, you will need to make sure these objects have their own `to/from_hdf` methods, e.g. by making sure
        they inherit from `HasHDF` and defining their `_to/_from_hdf` methods accordingly.

    Note:
        If you write `__init__` in any child class, be sure to pass
        `super().__init__(*args, group_name=group_name, **kwargs)` to ensure that the group name for `HasStorage` gets
        set, at the trait values (if any are set during instantiation) get set.

    Example:
        >>> from traitlets import (
        ...     Bool,
        ...     default,
        ...     Instance,
        ...     Int,
        ...     List,
        ...     observe,
        ...     TraitError,
        ...     TraitType,
        ...     Unicode,
        ...     validate
        ... )
        >>>
        >>> from pyiron_base.interfaces.has_hdf import HasHDF
        >>> from pyiron_base.storage.has_stored_traits import HasStoredTraits
        >>>
        >>>
        >>> class Omelette(HasStoredTraits):
        ...     '''
        ...     A toy model for cooking an omelette with traitlets.
        ...     '''
        ...
        ...     # The traits
        ...     n_eggs = Int(default_value=2)
        ...     acceptable = Bool()
        ...     ingredients = List(default_value=[], trait=Unicode())
        ...
        ...     @default('acceptable')
        ...     def wait_for_a_complaint(self):
        ...         '''
        ...         Default values can be assigned using the keyword, for mutable defaults always use a separate
        ...         function decorated with the `@default` decorator.
        ...         '''
        ...         return True
        ...
        ...     @validate('n_eggs')
        ...     def _gotta_crack_some_eggs(self, proposal):
        ...         '''
        ...         Validation proposals have the keys `['trait', 'value', 'owner']`.
        ...         The returned value is assigned to the trait, so you can do coercion here, or if all is well simply
        ...         return `proposal['value']`
        ...         '''
        ...         if proposal['value'] <= 0:
        ...             raise TraitError(
        ...                 f"You gotta crack some eggs to make a omelette, but asked for {proposal['value']}."
        ...             )
        ...         return proposal['value']
        ...
        ...     @observe('ingredients')
        ...     def _picky_eater(self, change):
        ...         '''
        ...         Observation changes have the keys `['name', 'old', 'new', 'owner', 'type']`.
        ...         Observations can also be set up with a method call, like
        ...         `self.observe(_picky_eater, names=['ingredients']`.
        ...         They don't need to return anything.
        ...         '''
        ...         wont_eat = ['mushrooms', 'zucchini']
        ...         for picky in wont_eat:
        ...             if picky in change['new']:
        ...                 self.acceptable = False
        >>>
        >>>
        >>> class Beverage(HasHDF):
        ...     '''
        ...     We can store custom objects in our input classes, but since they will ultimately be passed to a
        ...     `pyiron_base.DataContainer` for serialization, they either need to be of a type that `DataContainer`
        ...     can already handle, or they'll need to have `to_hdf` and `from_hdf` methods, e.g. by inheriting from
        ...     `pyiron_base.HasHDF` or `pyiron_base.HasStorage`.
        ...     '''
        ...     _types = ['coffee', 'tea', 'orange juice', 'water']
        ...
        ...     def __init__(self, type_='coffee'):
        ...         if type_ not in self._types:
        ...             raise ValueError(f"The beverage type must be chosen from {self._types}")
        ...         self.type_ = type_
        ...
        ...     def __repr__(self):
        ...         return self.type_
        ...
        ...     def _to_hdf(self, hdf):
        ...         hdf['drink_type'] = self.type_
        ...
        ...     def _from_hdf(self, hdf, version=None):
        ...         self.type_ = hdf['drink_type']
        >>>
        >>>
        >>> class CaffeinatedTrait(TraitType):
        ...     '''
        ...     We can even make our own trait types.
        ...
        ...     In this case, our default value is mutable, so we need to be careful! Normally we would just assign the
        ...     default to `default_value` (shown bug commented out). For mutable types we instead need to define a
        ...     function with the name `make_dynamic_default`.
        ...
        ...     (This is not well documented in readthedocs for traitlets, but is easy to see in the source code for
        ...     `TraitType`)
        ...     '''
        ...     # default_value = Beverage('coffee')  # DON'T DO THIS WITH MUTABLE DEFAULTS
        ...     def make_dynamic_default(self):  # Do this instead
        ...         return Beverage('coffee')
        ...
        ...     def validate(self, obj, value):
        ...         '''
        ...         Let's just make sure it's a caffeinated beverage.
        ...
        ...         Validations should return the value if everything works fine (maybe after some coercion), and hit
        ...         `self.error(obj, value)` if something goes wrong.
        ...         '''
        ...         if not isinstance(value, Beverage):
        ...             self.error(obj, value)
        ...         elif value.type_ not in ['coffee', 'tea']:
        ...             raise TraitError(f"Expected a caffeinated beverage, but got {value.type_}")
        ...         return value
        >>>
        >>>
        >>> class HasDrink(HasStoredTraits):
        ...     '''
        ...     We can use our special trait type in `HasTraits` classes, but a lot of the time it will be overkill
        ...     thanks to the `Instance` trait type. In this case we can accomplish the same functionality with
        ...     `@default` and `@validate` decorators.
        ...     '''
        ...     drink1 = CaffeinatedTrait()
        ...     drink2 = Instance(klass=Beverage)
        ...
        ...     @default('drink2')
        ...     def _default_drink2(self):
        ...         '''
        ...         Similar to the danger with a custom `TraitType`, we can't just use
        ...         '''
        ...         return Beverage('orange juice')
        ...
        ...     @validate('drink2')
        ...     def _non_caffeinated(self, proposal):
        ...         if proposal['value'].type_ not in ['orange juice', 'water']:
        ...         raise TraitError(
        ...             f"Expected a beverage of type 'orange juice' or 'water', but got {proposal['value'].type_}"
        ...         )
        ...         return proposal['value']
        >>>
        >>>
        >>> class ComposedBreakfast(Omelette, HasDrink):
        ...     '''
        ...     We can then put our Input children together very easily in a composition pattern.
        ...     Just don't forget to call `super().__init__(*args, **kwargs)` any time you override `__init__` to make sure
        ...     initialization of the traits gets passed through the MRO appropriately.
        ...     '''
        ...     pass
        >>>
        >>>
        >>> class NestedBreakfast(Omelette):
        ...     '''
        ...     We can also nest input classes together.
        ...
        ...     Again, since our trait is an instance of something mutable, we want to use the `@default` decorator instead of the
        ...     `default_value` kwarg.
        ...     '''
        ...     drinks = Instance(klass=HasDrink)
        ...
        ...     @default('drinks')
        ...     def _drinks_default(self):
        ...         return HasDrink()

        Now let's look at a few features in action.

        We can pass trait values in at initialization:
        >>> Omelette(n_eggs=3).n_eggs == 3
        True

        We can get traits to update automatically based on the value of other traits:
        >>> omelette = Omelette()
        >>> omelette.ingredients = ['ham', 'mushrooms']
        >>> omelette.acceptable
        False

        But we need to be careful, because we can observe internal changes to mutable traits!
        >>> omelette = Omelette()
        >>> omelette.ingredients.append('zucchini')  # Unacceptable!
        >>> omelette.acceptable
        True

        We saw that we could combine different subclasses together either by composition or by nesting.
        These are both totally valid choices, and it just depends what you want your data access to look like -- deep or
        wide?

        However, when we choose the nested architecture, that means we have a mutable trait, and we need to be careful
        with those: we warned about using a mutable object as a default value, however, `make_dynamic_default` method,
        we safely get separate instances for each trait owner:
        >>> cb = ComposedBreakfast()
        >>> nb = NestedBreakfast()
        >>> cb.drink1 == nb.drinks.drink1
        False

        Similarly, when defining our trait with the `Instance` type and using the `@default` decorator:
        >>> cb.drink2 == np.drinks.drink2
        False

        We can (recursively) change the traits to read-only (read/write) mode using the `lock()` (`unlock()`) method,
        e.g. if the child class is being used as input for a job you may want to lock the input when the job is run.
        >>> nb.lock()
        >>> nb.drinks.drink2 = Beverage('tea')
        RuntimeError: HasDrink is locked, so the trait drink2 cannot be updated to tea. Call `.unlock()` first if
        you're sure you know what you're doing.

        Be a bit careful though, since as with observer callbacks mutable traits can still be mutated:
        >>> nb.drinks.drink2.type_ = 'tea'
        >>> nb.drinks.drink2
        tea

        Further reading: the tests for this class use the same examples we have here, but are more in depth.
    """

    def setup_instance(*args, **kwargs):
        """
        This is called **before** self.__init__ is called.

        Overrides `HasTraits.setup_instance`, which gets called in `HasTraits.__new__` and initializes instances of the
        traits on self. Since we override `__setattr__` to depend on the attribute `_read_only`, we need to make sure
        this is the very first attribute that gets set!
        """
        self = args[0]
        self._read_only = False
        super(HasStoredTraits, self).setup_instance(*args, **kwargs)

    @property
    def read_only(self) -> bool:
        """
        Get the read-only status of the traits.

        Returns:
            bool: True if the traits are read-only, False otherwise.
        """
        return self._read_only

    def _to_hdf(self, hdf: ProjectHDFio):
        """
        Serialize the object to HDF5 format.

        Args:
            hdf (ProjectHDFio): The HDF5 file handler.
        """
        self.storage.is_read_only = self._read_only
        for k in self.traits().keys():
            self.storage[k] = getattr(self, k)
        super()._to_hdf(hdf)

    def _from_hdf(self, hdf: ProjectHDFio, version: Optional[str] = None):
        """
        Deserialize the object from HDF5 format.

        Args:
            hdf (ProjectHDFio): The HDF5 file handler.
            version (Optional[str]): The version of the object (default is None).
        """
        super()._from_hdf(hdf, version=version)
        if len(self.storage) > 0:
            read_only = self.storage.pop("is_read_only")
            for k, v in self.storage.items():
                setattr(self, k, v)
            self._read_only = read_only

    def lock(self) -> None:
        """
        Recursively make all traits read-only.
        """
        self._read_only = True
        for sub in self.trait_values().values():
            if isinstance(sub, HasStoredTraits):
                sub.lock()

    def unlock(self) -> None:
        """
        Recursively make all traits both readable and writeable.
        """
        self._read_only = False
        for sub in self.trait_values().values():
            if isinstance(sub, HasStoredTraits):
                sub.unlock()

    def __setattr__(self, key: str, value) -> None:
        """
        Set the value of an attribute.

        Args:
            key (str): The attribute name.
            value: The attribute value.
        """
        if key == "_read_only":
            super(HasStoredTraits, self).__setattr__(key, value)
        elif self.read_only and key in self.traits().keys():
            raise RuntimeError(
                f"{self.__class__.__name__} is locked, so the trait {key} cannot be updated to {value}. Call "
                f"`.unlock()` first if you're sure you know what you're doing."
            ) from None
        super().__setattr__(key, value)
