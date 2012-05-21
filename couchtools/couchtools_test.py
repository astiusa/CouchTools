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
        self.assertTrue(self.db.use('testdb'))

    def test_drop_db(self):
        ''' drop the db '''
        self.assertTrue(self.db.drop('testdb'))

    def test_load_view(self):
        ''' load a view '''
        if self.db.get('_design/render'):
            self.assertTrue(self.db.delete('_design/render'))
        viewcode = open('views/map.js').read()
        view = json.loads(viewcode)
        view['_id'] = "_design/render"
        self.assertIsNotNone(self.db.save(view))

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
        ''' i should probably have all this stuff done in a not-test method, but oh well. '''
        doc = {}
        doc['name'] = 'gregg'
        if self.db.get('_design/render'):
            self.assertTrue(self.db.delete('_design/render'))
        viewcode = open('views/map.js').read()
        view = json.loads(viewcode)
        view['_id'] = "_design/render"
        self.db.save(view)
        self.assertEqual(self.db.view('render/name')['name'], 'gregg')

    # TODO: test compound keys

if __name__ == '__main__':
    unittest.main()
