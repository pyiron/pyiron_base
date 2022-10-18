# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.

from traitlets import (
    Bool,
    default,
    Instance,
    Int,
    List,
    observe,
    TraitError,
    TraitType,
    Unicode,
    validate
)

from pyiron_base._tests import TestWithProject
from pyiron_base.interfaces.has_hdf import HasHDF
from pyiron_base.storage.input import HasStoredTraits


class Omelette(HasStoredTraits):
    """
    A toy model for cooking an omelette with traitlets.
    """

    # The traits
    n_eggs = Int(default_value=2)
    acceptable = Bool()
    ingredients = List(default_value=[], trait=Unicode())

    @default('acceptable')
    def wait_for_a_complaint(self):
        """
        Default values can be assigned using the keyword, or more complex values can be constructed in a
        separate function and assigned using the `@default` decorator.
        """
        return True

    @validate('n_eggs')
    def _gotta_crack_some_eggs(self, proposal):
        """
        Validation proposals have the keys `['trait', 'value', 'owner']`.
        The returned value is assigned to the trait, so you can do coercion here, or if all is well simply
        return `proposal['value']`
        """
        if proposal['value'] <= 0:
            raise TraitError(
                f"You gotta crack some eggs to make a omelette, but asked for {proposal['value']}."
            )
        return proposal['value']

    @observe('ingredients')
    def _picky_eater(self, change):
        """
        Observation changes have the keys `['name', 'old', 'new', 'owner', 'type']`.
        Observations can also be set up with a method call, like
        `self.observe(_picky_eater, names=['ingredients']`.
        They don't need to return anything.
        """
        wont_eat = ['mushrooms', 'zucchini']
        for picky in wont_eat:
            if picky in change['new']:
                self.acceptable = False


class Beverage(HasHDF):
    """
    We can store custom objects in our input classes, but since they will ultimately be passed to a
    `pyiron_base.DataContainer` for serialization, they either need to be of a type that `DataContainer` can already
    handle, or they'll need to have `to_hdf` and `from_hdf` methods, e.g. by inheriting from `pyiron_base.HasHDF` or
    `pyiron_base.HasStorage`.
    """
    _types = ['coffee', 'tea', 'orange juice']

    def __init__(self, type_='coffee'):
        if type_ not in self._types:
            raise ValueError(f"The beverage type must be chosen from {self._types}")
        self.type_ = type_

    def __repr__(self):
        return self.type_

    def _to_hdf(self, hdf):
        hdf['drink_type'] = self.type_

    def _from_hdf(self, hdf, version=None):
        self.type_ = hdf['drink_type']


class CaffeinatedTrait(TraitType):
    """
    We can even make our own trait types.

    In this case, we're making a counter-example -- custom traits with mutable defaults are dangerous! If more than one
    object uses this trait, their default is the *same instance* of the mutable default.
    """
    default_value = Beverage('coffee')

    def validate(self, obj, value):
        """
        Let's just make sure it's a caffeinated beverage.

        Validations should return the value if everything works fine (maybe after some coercion), and hit
        `self.error(obj, value)` if something goes wrong.
        """
        if not isinstance(value, Beverage):
            self.error(obj, value)
        elif value.type_ not in ['coffee', 'tea']:
            raise ValueError(f"Expected a caffeinated beverage, but got {value.type_}")
        return value


class HasDrink(HasStoredTraits):
    """
    We can use our special trait type in `HasTraits` classes, but a lot of the time it will be overkill thanks
    to the `Instance` trait type.
    In this toy example, our new trait type is just a little more restrictive than `Instance`.
    """
    drink1 = CaffeinatedTrait()
    drink2 = Instance(klass=Beverage)

    @default('drink2')
    def _default_drink2(self):
        """
        Similar to the danger with a custom `TraitType`, we can't just use
        """
        return Beverage('orange juice')


class ComposedBreakfast(Omelette, HasDrink):
    """
    We can then put our Input children together very easily in a composition pattern.
    Just don't forget to call `super().__init__(*args, **kwargs)` any time you override `__init__` to make sure
    initialization of the traits gets passed through the MRO appropriately.
    """
    pass


class NestedBreakfast(Omelette):
    """
    We can also nest input classes together.

    Again, since our trait is an instance of something mutable, we want to use the `@default` decorator instead of the
    `default_value` kwarg.
    """
    drinks = Instance(klass=HasDrink)

    @default('drinks')
    def _drinks_default(self):
        return HasDrink()


class TestInput(TestWithProject):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self) -> None:
        super().setUp()
        self.hdf = self.project.create_hdf(path=self.project.path, job_name='h5_storage')
        self.omelette = Omelette()

    def tearDown(self) -> None:
        super().tearDown()
        self.hdf.remove_file()

    def test_instantiation(self):
        with self.subTest("Defaults can be assigned by keyword or decorator"):
            self.assertEqual(True, self.omelette.acceptable)
            self.assertEqual(Omelette.n_eggs.default_value, self.omelette.n_eggs)

        with self.subTest("Traits can be passed as kwargs"):
            self.assertEqual(3, Omelette(n_eggs=3).n_eggs)

    def test_validation(self):
        with self.subTest("Test standard validation"):
            with self.assertRaises(TraitError):
                self.omelette.acceptable = "This is not a Bool"

        with self.subTest("Test standard coercion"):
            self.omelette.ingredients = "ham"
            self.assertEqual(['ham'], self.omelette.ingredients, msg="Simple assignments to list are coerced")
            self.omelette.ingredients = ('cheese', 'ham')
            self.assertEqual(
                ['cheese', 'ham'],
                self.omelette.ingredients,
                msg="This coercion is a bit flexible"
            )

        with self.subTest("But *because we specified our trait type*, not just any list will do"):
            with self.assertRaises(TraitError):
                self.omelette.ingredients = [5]

        with self.subTest("Test custom validation"):
            with self.assertRaises(TraitError):
                self.omelette.n_eggs = 0

    def test_observe(self):
        with self.subTest("Observe catches modifications to the trait value"):
            self.assertEqual(True, self.omelette.acceptable)
            self.omelette.ingredients = ['ham', 'mushrooms']
            self.assertEqual(False, self.omelette.acceptable)

        with self.subTest(
                "But not at instantiation, which [is tricky](https://github.com/ipython/traitlets/issues/389)"
        ):
            omelette2 = Omelette(ingredients=['ham', 'mushrooms'])
            self.assertEqual(False, omelette2.acceptable)

        with self.subTest("And changes within mutable types are ignored"):
            another_omelette = Omelette()
            another_omelette.ingredients.append('zucchini')
            self.assertEqual(True, another_omelette.acceptable)

    def test_architectures_and_mutability(self):
        cb = ComposedBreakfast()
        nb = NestedBreakfast()

        with self.subTest("Test the counter example, where we see that mutable defaults are dangerous"):
            self.assertEqual(cb.drink1, nb.drinks.drink1)  # The same instance!
            cb.drink1.type_ = 'tea'
            self.assertEqual('tea', nb.drinks.drink1.type_)  # So of course we can mutate that instance
            nb.drinks.drink1 = Beverage('coffee')
            self.assertNotEqual(cb.drink1.type_, nb.drinks.drink2.type_)  # But we are OK if we modify the entire trait

        with self.subTest("When we use the `@default` decorator, we return safely return a new instance each time"):
            self.assertNotEqual(cb.drink2, nb.drinks.drink2)

    def test_serialization(self):
        with self.subTest("Save and load to default location"):
            self.omelette.n_eggs = 12
            self.omelette.to_hdf(self.hdf)
            loaded_omelette = Omelette()
            loaded_omelette.from_hdf(self.hdf)
            self.assertEqual(self.omelette.n_eggs, loaded_omelette.n_eggs)

        with self.subTest("Save and load from some other group name"):
            self.omelette.n_eggs = 13
            self.omelette.to_hdf(self.hdf, group_name='my_group')
            loaded_omelette = Omelette()
            loaded_omelette.from_hdf(self.hdf, group_name='my_group')
            self.assertEqual(self.omelette.n_eggs, loaded_omelette.n_eggs)

    def test_composed_serialization(self):
        breakfast = ComposedBreakfast(ingredients=['ham'], drink2=Beverage('tea'))
        breakfast.to_hdf(self.hdf)
        loaded_breakfast = ComposedBreakfast()
        loaded_breakfast.from_hdf(self.hdf)
        self.assertEqual(breakfast.ingredients, loaded_breakfast.ingredients)
        self.assertEqual(breakfast.drink2.type_, loaded_breakfast.drink2.type_)

    def test_nested_serialization(self):
        breakfast = NestedBreakfast(ingredients=['ham'])
        breakfast.drinks.drink2 = Beverage('tea')
        breakfast.to_hdf(self.hdf)
        loaded_breakfast = NestedBreakfast()
        loaded_breakfast.from_hdf(self.hdf)
        self.assertEqual(breakfast.ingredients, loaded_breakfast.ingredients)
        self.assertEqual(breakfast.drinks.drink2.type_, loaded_breakfast.drinks.drink2.type_)

    def test_locking(self):
        nb = NestedBreakfast()
        nb.drinks.drink2 = Beverage('tea')
        nb.lock()
        with self.assertRaises(RuntimeError):
            # Test lock
            nb.n_eggs = 12
        with self.assertRaises(RuntimeError):
            # Test recursion of lock
            nb.drinks.drink2 = Beverage('orange juice')
        nb.to_hdf(self.hdf)

        loaded_nb = NestedBreakfast()
        loaded_nb.from_hdf(self.hdf)
        # Make sure read_only is getting (de)serialized OK
        with self.assertRaises(RuntimeError):
            # Test lock
            loaded_nb.n_eggs = 12
        with self.assertRaises(RuntimeError):
            # Test recursion of lock
            loaded_nb.drinks.drink2 = Beverage('coffee')

        loaded_nb.unlock()
        # Test unlock
        loaded_nb.n_eggs = 12
        # Test recursion of unlock
        loaded_nb.drinks.drink2 = Beverage('coffee')  # It's a two-cup day
