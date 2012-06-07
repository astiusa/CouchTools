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

VERSION = 0.4


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

    # TODO: if overwrite is set up False, create a new document rather than overwriting a new one.
    def save(self, entry, overwrite=True):
        '''
        save wrapper. checks that you included a uuid. doesn't check that
        your thing is even remotely JSON-serializable.
        '''
        if '_id' not in entry:
            entry['_id'] = uuid().hex
        # TODO: check that _rev is in the doc, else updates will fail.
        try:
            savedata = self.db.save(entry)
        except Exception, e:  # TODO do something here if the entry isn't saveable
            raise CouchSaveException("There was a problem saving " + str(entry) + ' ' + e[1])
        except UnicodeDecodeError:  # TODO I still need to do something reasonable when there's a unicode problem.
            pass
        return savedata

    def view(self, viewname, keyval=None):
        '''
            get a view with an optional specific key. assume 'derp' is the results of the view
            For simple keys (ie emit(doc.key, doc))
            value of view is simply derp[0] (whatever that may be; dict, tuple, etc)
            for compound keys (ie emit([doc.a, doc.b], doc))
            value of view is derp[1], derp[0] contains the keys
            so to access the second key, derp[0][1]
        '''
        # TODO: This should probably check if keyval is a list or a tuple or
        # something since that changes the calling convention
        if '/' not in viewname:
            raise CouchViewException("View should probably have a slash in the name, eg render/derp")
        if keyval:
            query = self.db.view(viewname, key=keyval)
        else:
            query = self.db.view(viewname)
        if len(query.rows) == 1:
            return [query.rows[0]['key'], query.rows[0]['value']]
        elif len(query.rows) > 1:
            valuedata = []
            for queryrow in query.rows:
                valuedata.append([queryrow['key'], queryrow['value']])
            return valuedata
        else:
            return None

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

    #def find(self, doc):
    #    '''
    #    check to see if the doc already exists in the database.
    #    '''
    #    this_id = doc['_id']
    #    if self.get(this_id):
    #        return doc
    #    else:
    #        return False
