import unittest

from gokart.utils import flatten, map_flattenable_items


class TestFlatten(unittest.TestCase):
    def test_flatten_dict(self):
        self.assertEqual(flatten({'a': 'foo', 'b': 'bar'}), ['foo', 'bar'])

    def test_flatten_list(self):
        self.assertEqual(flatten(['foo', ['bar', 'troll']]), ['foo', 'bar', 'troll'])

    def test_flatten_str(self):
        self.assertEqual(flatten('foo'), ['foo'])

    def test_flatten_int(self):
        self.assertEqual(flatten(42), [42])

    def test_flatten_none(self):
        self.assertEqual(flatten(None), [])


class TestMapFlatten(unittest.TestCase):
    def test_map_flattenable_items(self):
        self.assertEqual(map_flattenable_items({'a': 1, 'b': 2}, func=lambda x: str(x)), {'a': '1', 'b': '2'})
        self.assertEqual(
            map_flattenable_items({'a': [1, 2, 3, '4'], 'b': {'c': True, 'd': {'e': 5}}}, func=lambda x: str(x)),
            {'a': ['1', '2', '3', '4'], 'b': {'c': 'True', 'd': {'e': '5'}}},
        )
