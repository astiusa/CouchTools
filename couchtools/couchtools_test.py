#!/usr/bin/env python

# Copyright 2012, Advanced Simulation Technology, inc. http://www.asti-usa.com/
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from couchtools import CouchTools
import json
import unittest


class TestCouchTools(unittest.TestCase):

    def setUp(self):
        self.db = CouchTools(init_new=True)

    def test_change_db(self):
        ''' set the current DB '''
        self.assertTrue(self.db.use('testdb'))

    def test_drop_db(self):
        ''' drop the db '''
        self.assertTrue(self.db.drop('testdb'))

    def test_load_view(self):
        ''' load a view '''
        if self.db.get('_design/testrender'):
            self.assertTrue(self.db.delete('_design/testrender'))
        self.assertIsNotNone(self.db.loadview('views/map.js', '_design/testrender'))

    def test_load_data(self):
        ''' load some data. '''
        doc = {}
        doc['_id'] = self.db.docid()
        doc['herp'] = 'derp'
        self.assertTrue(self.db.save(doc))

    def test_load_more_data(self):
        ''' load some data, but check that it creates the UUID on its own bad self. '''
        doc = {}
        doc['hurr'] = 'durr'
        self.assertTrue(self.db.save(doc))

    def test_load_data_revision(self):
        ''' ensure that we update a document, rather than saving a new one. '''
        doc = {}
        doc['a'] = 'b'
        anid = self.db.docid()
        doc['_id'] = anid
        self.assertRegexpMatches(self.db.save(doc)[1], '^1')
        thedoc = self.db.get(anid)
        self.assertTrue(thedoc)
        thedoc['name'] = 'gregg'
        saved = self.db.save(thedoc)
        self.assertRegexpMatches(saved[1], '^2')

    def test_query_view(self):
        ''' Test the results of a query on a view. '''
        doc = {}
        doc['name'] = 'gregg'
        self.db.save(doc)
        if self.db.get('_design/testrender'):
            self.assertTrue(self.db.delete('_design/testrender'))
        self.db.loadview('views/map.js', '_design/testrender')
        results = self.db.view('testrender/name')
        for r in results:
            self.assertEqual(r['key'], 'gregg')

    def test_query_view_value(self):
        ''' Test the results of a query on a view, looking for a specific value in the returned values. '''
        doc = {}
        doc['name'] = 'gregg'
        self.db.save(doc)
        if self.db.get('_design/testrender'):
            self.assertTrue(self.db.delete('_design/testrender'))
        self.db.loadview('views/map.js', '_design/testrender')
        results = self.db.view('testrender/name')
        for r in results:
            self.assertEqual(r['value']['name'], 'gregg')

    def test_compound_key_view(self):
        ''' test that compound keys (emit([derp,hurr],hurrdurr)) works. '''
        doc = {}
        doc['name'] = 'gregg'
        doc['first_name'] = 'julius'
        doc['last_name'] = 'thomason'
        self.db.save(doc)
        if self.db.get('_design/testrender'):
            self.assertTrue(self.db.delete('_design/testrender'))
        self.db.loadview('views/map.js', '_design/testrender')
        result = self.db.view('testrender/compound')
        for r in result:
            self.assertEqual(r['key'][0], 'julius')

    def test_python_view(self):
        ''' load a python-based view. '''
        if self.db.get('_design/pythonview'):
            self.assertTrue(self.db.delete('_design/pythonview'))
        viewcode = open('views/map.py').read()
        view = json.loads(viewcode)
        self.assertIsNotNone(self.db.save(view))

    def test_nothing_at_all(self):
        ''' test to see what happens when nothing at all happens. '''
        if self.db.get('_design/testrender'):
            self.assertTrue(self.db.delete('_design/testrender'))
        self.db.loadview('views/map.js', '_design/testrender')
        for result in self.db.view('testrender/name', keyval="gary"):
            self.assertFalse(result)

    def test_key_in(self):
        ''' test to see if a particular value exists as a key. '''
        doc = {}
        doc['name'] = 'gregg'
        self.db.save(doc)
        if self.db.get('_design/testrender'):
            self.assertTrue(self.db.delete('_design/testrender'))
        self.db.loadview('views/map.js', '_design/testrender')
        self.assertTrue(self.db.exists('testrender/name', 'gregg'))

    def test_value_in(self):
        ''' test to see if a particular value exists, with a named key. '''
        doc = {}
        doc['name'] = 'gregg'
        self.db.save(doc)
        if self.db.get('_design/testrender'):
            self.assertTrue(self.db.delete('_design/testrender'))
        self.db.loadview('views/map.js', '_design/testrender')
        self.assertTrue(self.db.exists('testrender/name', 'gregg', 'name'))

    def test_query_python_view(self):
        ''' Test the results of a query on a python view. '''
        doc = {}
        doc['name'] = 'gregg'
        self.db.save(doc)
        if self.db.get('_design/pythonview'):
            self.assertTrue(self.db.delete('_design/pythonview'))
        viewcode = open('views/map.py').read()
        view = json.loads(viewcode)
        view['_id'] = "_design/pythonview"
        self.db.save(view)
        results = self.db.view('pythonview/name')
        for r in results:
            self.assertEqual(r['key'], 'gregg')


if __name__ == '__main__':
    unittest.main()
