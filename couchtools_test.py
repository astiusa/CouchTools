#!/usr/bin/env python

from couchtools import Couchtools
import unittest

class TestCouchTools(unittest.TestCase):
    def setUp(self):
        self.db = Couchtools('test')

    def test_change_server(self):
        first = 'teststuff'
        second = 'test'
        self.assertEqual(self.db.change_db('teststuff'), 'teststuff')

if __name__ == '__main__':
    unittest.main()
