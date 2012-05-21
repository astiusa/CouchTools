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


import couchdb.client as couch
try:
    from uuid import uuid4 as uuid
except ImportError:
    from simpleuuid import uuid


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
        from couchtools import CouchTools
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
        ''' "drop" a database. doesn't try to hard. '''
        try:
            del self.server[dbname]
            return True
        except:
            raise CouchDBException("There was a problem dropping the database " + dbname)
            return False

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

    def save(self, entry):
        '''
        save wrapper. checks that you included a uuid. doesn't check that
        your thing is even remotely JSON-serializable.
        '''
        if '_id' not in entry:
            entry['_id'] = uuid().hex
        try:
            savedata = self.db.save(entry)
        except Exception:  # TODO do something here if the entry isn't saveable
            raise CouchSaveException("There was a problem saving " + str(entry))
        except UnicodeDecodeError:  # TODO I still need to do something reasonable when there's a unicode problem.
            pass
        return savedata

    def view(self, viewname, keyval=None):
        ''' get a view with an optional specific key. '''
        # TODO: This should probably check if keyval is a list or a tuple or
        # something since that changes the calling convention
        if '/' not in viewname:
            raise CouchViewException("View should probably have a slash in the name, eg render/derp")
        if keyval:
            query = self.db.view(viewname, key=keyval)
        else:
            query = self.db.view(viewname)
        # TODO: this method is pretty bad if your view has a compound key. You're going to get back crap.
        if len(query.rows) == 1:
            return query.rows[0]['value']
        elif len(query.rows) > 1:
            valuedata = []
            for queryrow in query.rows:
                valuedata.append(queryrow['value'])
            return valuedata
        else:
            return None

    def delete(self, docid):
        ''' delete a document. '''
        thedoc = self.db.get(docid)
        try:
            self.db.delete(thedoc)
            return True
        except:
            return False
