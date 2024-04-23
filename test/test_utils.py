import unittest

from gokart.utils import flatten


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
