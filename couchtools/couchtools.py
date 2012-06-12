#!/usr/bin/env python

# Copyright 2012, Advanced Simulation Technology, inc.
# http://www.asti-usa.com/
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

# TODO: incorporate mapping stuff so we can validate models, etc

import couchdb.client as couch
try:
    from uuid import uuid4 as uuid
except ImportError:
    from simpleuuid import uuid
import json
import os.path

VERSION = 0.5


class CouchSaveException(Exception):
    ''' At some point I guess I should do something useful with exceptions. '''
    pass


class CouchViewException(Exception):
    ''' At some point I guess I should do something useful with exceptions. '''
    pass


class CouchDBException(Exception):
    ''' generic "there was a problem at the database level" exception. '''
    pass


class CouchTools(object):
    '''
    wrap couchdb.client with some ostensibly sensible defaults and make it feel sort-of
    like a SQL db (familiar terms like drop, etc)
    '''
    def __init__(self, dbname='test', init_new=False, couchhost="localhost", couchport="5984"):
        '''
        # example usage
        from couchtools.couchtools import CouchTools
        d = CouchTools('derp')
        f = {blah: "something"} # some dict of things
        d.save(f) # saves to couch
        # to connect to a different couch instance, do something like
        d = Couchtools('hurr', couchhost='couch.haircrappery.net')
        '''
        serverstring = "http://" + couchhost + ":" + couchport + "/"
        self.server = couch.Server(serverstring)
        if init_new:
            if dbname in self.server:
                del self.server[dbname]
            self.db = self.server.create(dbname)
        self.use(dbname)

    def drop(self, dbname):
        ''' "drop" a database. doesn't try too hard. '''
        try:
            del self.server[dbname]
            return True
        except:
            raise CouchDBException("There was a problem dropping the database " + dbname)
            return False

    def since_change(self, changeid):
        ''' return the list of changes since changeid.
            TODO: This should almost certainly take other options. but it'll work for now.
        '''
        # keep the latest change we've asked for (latest_change_id), in case we want it,
        # and the highest (most recent) actual DB change (last_change_id)
        if changeid < self.last_change_id:
            self.latest_change_id = changeid
        else:
            self.last_change_id = changeid
        return self.db.changes(since=changeid)

    def use(self, dbname):
        ''' set the current DB '''
        try:
            self.db = self.server[dbname]
        except:
            # try to create the DB since it may not exist
            if dbname not in self.server:
                self.db = self.server.create(dbname)
            try:
                self.use(dbname)
            except:
                # AW SNAP
                return False
        return True

    def docid(self):
        ''' Don't let couch make its own uuid. Apparently that's bad.
            Something about idempotence. (call your sysadmin if it lasts for more than 4 hours)
        '''
        return uuid().hex

    def get(self, docid):
        '''
            Get a document by ID.
        '''
        # TODO: need to be able to fetch optional revision.
        return self.db.get(docid, default={})

    def put(self, doc, path_to_attach):
        ''' attach a document. you can pass an empty dict as doc or an existing dict/document. '''
        filename = open(path_to_attach)
        if '_id' not in doc:
            doc['_id'] = self.docid()
        try:
            self.db.put_attachment(doc, filename)
        except:
            return False

    # TODO it may be better to use update() instead of save().
    def load(self, path):
        ''' Given a path to a file, load the database, ie do lots of inserts.'''
        payload = open(path).readlines()
        for x in payload:
            try:
                self.save(x)
            except:
                raise CouchSaveException('Problem loading ' + x + ' from ' + path)

    def save(self, entry, overwrite=True):
        '''
        save wrapper. checks that you included a uuid. doesn't check that
        your thing is even remotely JSON-serializable.
        '''
        if '_id' not in entry:
            entry['_id'] = uuid().hex
        # check that _rev is in the doc, else updates will fail.
        if overwrite == True and '_rev' not in entry:
            try:
                thisentry = self.get(entry['_id'])
                if thisentry:
                    entry['_rev'] = thisentry['_rev']
            except Exception, e:
                raise CouchSaveException("Cannot save a revision without setting _rev." + e[1])
        try:
            savedata = self.db.save(entry)
        except Exception, e:
            raise CouchSaveException("There was a problem saving " + str(entry) + ' ' + e[1])
        except UnicodeDecodeError:  # TODO I still need to do something reasonable when there's a unicode problem.
            pass
        return savedata

    def exists(self, view, testforkey, testforvalue=None):
        '''
            Like 'view' but a predicate to check if a particular value exists.
            :param view the name of the view to query, like self.view()
            :param testforkey the value to test for, as the key value of the query.
            :param testforvalue (optional) optional value key to test again
            EG, yield of {key: gregg, value: name: gregg}
            testforkey == gregg TRUE
            testforvalue name testforkey == gregg TRUE
            I miss predicate methods in Ruby; this should be named exists?
        '''
        for item in self.view(view):
            if testforkey in item['key']:
                return True
            if testforvalue:
                if testforvalue in item['value'][testforkey]:
                    return True
        return False

    def view(self, viewname, keyval=None):
        '''
            generator for query results.
            :param viewname name of design doc and view, must include '/'
            :param keyval optional name of key to filter on (presently, string)
            yields generator of {"key": query key, "value": query value} values
        '''
        # TODO: This should probably check if keyval is a list or a tuple or
        # something since that changes the calling convention
        if '/' not in viewname:
            raise CouchViewException("View should probably have a slash in the name, eg render/derp")
        if keyval:
            query = self.db.view(viewname, key=keyval)
        else:
            query = self.db.view(viewname)
        for queryrow in query.rows:
            yield {"key": queryrow['key'], "value": queryrow['value']}

    def delete(self, docid):
        ''' delete a document. '''
        thedoc = self.get(docid)
        try:
            self.db.delete(thedoc)
            return True
        except:
            return False

    def loadview(self, view, viewname=None):
        '''
        load a view into the DB from a file or string. Accepts a (string) path to
        file or a string of view code.
        '''
        try:
            viewcode = open(view).read()
            filename = os.path.basename(view)
            if not viewname:
                viewname = "_design/" + filename.split('.')[0]
        except IOError:
            viewcode = view
            if not viewname:
                viewname = "_design/render"  # TODO improve this somehow.
        view = json.loads(viewcode)
        view['_id'] = viewname
        return self.db.save(view)
