"""
Tests for the relation engine client.
"""

import unittest
from src.utils import re_client

# TODO more tests


class TestREClient(unittest.TestCase):

    def test_key_cleaner(self):
        """
        Test removing ArangoDB '_key' illegal characters from a string.
        """

        # check all legal characters
        string = ''
        for i in range(48, 57):
            string += chr(i)
        for i in range(65, 91):
            string += chr(i)
        for i in range(97, 123):
            string += chr(i)

        string += "_-:.@()+,=;$!*'%"

        self.assertEqual(re_client.clean_key(string), string)

        # check some illegal characters
        self.assertEqual(re_client.clean_key("f|g~i]u/oԊy"), "f_g_i_u_o_y")
