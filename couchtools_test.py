#!/usr/bin/env python

from couchtools import CouchTools
import unittest

class TestCouchTools(unittest.TestCase):
    def setUp(self):
        self.db = CouchTools()

    def test_change_db(self):
        self.assertTrue(self.db.change_db('testdb'))

    def test_drop_db(self):
        self.assertTrue(self.db.drop('testdb'))

#     def test_load_view(self):
#         pass
#
#     def test_query_view(self):
#         pass

if __name__ == '__main__':
    unittest.main()
