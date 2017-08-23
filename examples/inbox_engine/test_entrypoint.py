import unittest


from .entrypoint import Entrypoint


class TestExample(unittest.TestCase):
    def test_example(self):
        Entrypoint().run()
