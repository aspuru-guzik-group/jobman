import unittest


class BaseTestCase(unittest.TestCase):
    def setUp(self): pass

class GetJobStateTestCase(BaseTestCase):
    def test_something(self):
        self.fail()
