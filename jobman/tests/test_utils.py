import types
import unittest

from .. import utils


class BaseTestCase(unittest.TestCase): pass

class GetKeyOrAttrTestCase(BaseTestCase):
    def test_returns_key_first(self):
        src = {'my_key': 'my_val'}
        self.assertEqual(utils.get_key_or_attr(src=src, key='my_key'), 'my_val')

    def test_returns_attr_if_no_key(self):
        src = types.SimpleNamespace()
        src.my_key = 'my_val'
        self.assertEqual(utils.get_key_or_attr(src=src, key='my_key'), 'my_val')

    def test_returns_default_if_no_key_no_attr(self):
        src = {}
        self.assertEqual(
            utils.get_key_or_attr(src=src, key='my_key', default='my_val'),
            'my_val'
        )

    def test_raises_if_no_default_no_key_no_attr(self):
        src = {}
        with self.assertRaises(KeyError):
            utils.get_key_or_attr(src=src, key='my_key')

