import unittest

from src.utils.run_func_once import run_once


class TestRunFuncOnce(unittest.TestCase):

    def test_run_once(self):
        """Test with an argument."""
        count = 0

        @run_once
        def add(n):
            nonlocal count
            count += n
        add(1)
        add(1)
        add(2)
        add(2)
        self.assertEqual(count, 3)

    def test_run_once_no_args(self):
        """Test with no args."""
        count = 0

        @run_once
        def incr():
            nonlocal count
            count += 1
        incr()
        incr()
        incr()
        self.assertEqual(count, 1)
