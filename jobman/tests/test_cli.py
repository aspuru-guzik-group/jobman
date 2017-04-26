import unittest


class BaseTestCase(unittest.TestCase):
    pass

class SubmitTestCase(BaseTestCase):
    def test_dispatches_to_submit(self):
        self.fail()

class PollTestCase(BaseTestCase):
    def test_dispatches_to_poll(self):
        self.fail()

if __name__ == '__main__': unittest.main()
