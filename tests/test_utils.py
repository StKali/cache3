#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/26
# author: clarkmonkey@163.com

import pytest
from cache3.util import cached_property, lazy, LazyObject
raises = pytest.raises


class TestCachedProperty:

    def test_func_error(self):
        with raises(TypeError):
            cached_property.func(None)

    def test_cached_property(self):
        """cached_property caches its value and behaves like a property."""

        class Class:
            @cached_property
            def value(self):
                """Here is the docstring..."""
                return 1, object()

            @cached_property
            def __foo__(self):
                """Here is the docstring..."""
                return 1, object()

            def other_value(self):
                """Here is the docstring..."""
                return 1, object()

            other = cached_property(other_value)

        attrs = ["value", "other", "__foo__"]
        for attr in attrs:
            self.assertCachedPropertyWorks(attr, Class)

    def assertCachedPropertyWorks(self, attr, Class):

        def get(source):
            return getattr(source, attr)

        obj = Class()

        class SubClass(Class):
            pass

        subobj = SubClass()
        # Docstring is preserved.
        assert get(Class).__doc__ == "Here is the docstring..."
        assert get(SubClass).__doc__, "Here is the docstring..."

        # It's cached.
        assert get(obj) == get(obj)
        assert get(subobj) == get(subobj)

        # The correct value is returned.
        assert get(obj)[0], 1
        assert get(subobj)[0], 1

        # State isn't shared between instances.
        obj2 = Class()
        subobj2 = SubClass()
        assert get(obj), get(obj2)
        assert get(subobj), get(subobj2)

        # It behaves like a property when there's no instance.
        # self.assertIsInstance(get(Class), cached_property)
        # self.assertIsInstance(get(SubClass), cached_property)

        # 'other_value' doesn't become a property.
        assert callable(obj.other_value)
        assert callable(subobj.other_value)

    def test_cached_property_auto_name(self):
        """
        cached_property caches its value and behaves like a property
        on mangled methods or when the name kwarg isn't set.
        """

        class Class:
            @cached_property
            def __value(self):
                """Here is the docstring..."""
                return 1, object()

            def other_value(self):
                """Here is the docstring..."""
                return 1, object()

            other = cached_property(other_value)

        attrs = ["_Class__value", "other"]
        for attr in attrs:
            self.assertCachedPropertyWorks(attr, Class)

    def test_cached_property_reuse_different_names(self):
        """Disallow this case because the decorated function wouldn't be cached."""
        with raises(RuntimeError, match='Error calling __set_name__ on .*'):

            class ReusedCachedProperty:
                @cached_property
                def a(self):
                    pass

                b = a

    def test_cached_property_reuse_same_name(self):
        """
        Reusing a cached_property on different classes under the same name is
        allowed.
        """
        counter = 0

        @cached_property
        def _cp(_self):
            nonlocal counter
            counter += 1
            return counter

        class A:
            cp = _cp

        class B:
            cp = _cp

        a = A()
        b = B()
        assert a.cp == 1
        assert b.cp == 2
        assert a.cp == 1

    def test_cached_property_set_name_not_called(self):
        cp = cached_property(lambda s: None)

        class Foo:
            pass

        Foo.cp = cp
        msg = (
            "Cannot use cached_property instance without calling __set_name__() on it."
        )
        with raises(TypeError, match='Cannot use cached_property instance without calling .*'):
            Foo().cp

    def test_lazy_add(self):
        v4 = lazy(int)
        assert v4(4) + 1 == 5

    def test_lazy_repr(self):
        v = lazy(int)
        number = v(4)
        assert repr(number).startswith('LazyObject: <function')
        assert str(number) == '4'
        assert number + 1 == 5
        assert repr(number) == 'LazyObject: 4'

    def test_lazy_set(self):
        class A:
            """"""
            name = 1

        LazyA = lazy(A)

        number = LazyA()
        number.v = 1
        assert number.v == 1
        del number.v
        assert not hasattr(number, 'v')
        with raises(TypeError, match='cannot delete _wrapped'):
            del number._wrapped

        new = lazy(A)()
        with raises(AttributeError):
            del new.name
