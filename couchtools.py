#!/usr/bin/env python

import couchdb.client as couch
import json
try:
    from uuid import uuid4 as uuid
except ImportError:
    from simpleuuid import uuid

class CouchSaveException(Exception):
    ''' At some point I guess I should do something useful with exceptions. '''
    pass


class CouchViewException(Exception):
    ''' Again at some point I should do something useful here. '''
    pass


class CouchTools(object):
    ''' wrap couchdb.client with some ostensibly sensible defaults. '''
    def __init__(self, dbname, init_new=False, couchhost="localhost", couchport="5984"):
        '''
        # example usage
        from couchtools import Couchtools
        d = Couchtools('rhel')
        f = {blah: "something"} # some dict of things
        d.save(f) # saves to couch
        # to connect to a different couch instance, do something like
        d = Couchtools('rhel', couchhost='asti-couch.asti-usa.com')
        '''
        serverstring = "http://" + couchhost + ":" + couchport + "/"
        self.server = couch.Server(serverstring)
        if init_new:
            if dbname in self.server:
                del self.server[dbname]
            self.db = self.server.create(dbname)
        self.db = self.server[dbname]

    def change_db(self,dbname):
        self.server[dbname]
        return self

    def docid(self):
        """ Don't let couch make its own uuid. Apparently that's bad I guess? """
        return uuid4().hex

    def get(self, docid):
        ''' Get a document by ID. '''
        return self.db.get(docid, default={})

    # FIXME this is not right at all.
    def put(self,id,path_to_attach):
        filename = open(path_to_attach)
        return self.db.put_attachment(id,filename,path_to_attach)

    # FIXME hurr needs maor werk durr
    def load(self,path):
        """ Given a path to a file, load the database, ie do lots of inserts."""
        payload = open(path).readlines()
        for x in payload:
            try:
                self.save(x)
            except:
                pass # TODO something? I guess.

    def save(self, entry):
        '''
        save wrapper. checks that you included a uuid. doesn't check that
        your thing is even remotely serializable
        '''
        if '_id' not in entry:
            entry['_id'] = uuid().hex
        try:
            savedata = self.db.save(entry)
        except Exception:  # TODO do something here if the entry isn't saveable
            raise CouchSaveException("There was a problem saving " + str(entry))
        except UnicodeDecodeError:  # TODO I still need to do something reasonable when there's a thingee.
            pass
        return savedata


    # TODO: Also, this method is pretty shitty if your view has a compound key.
    def view(self, viewname, keyval=None):
        ''' get a view with an optional specific key. '''
        # TODO: This should probably check if keyval is a list or a tuple or
        # some shit since that changes the calling convention
        if '/' not in viewname:
            raise RhelViewException("View should probably have a slash in the name, eg render/derp")
        if keyval:
            query = self.db.view(viewname, key=keyval)
        else:
            query = self.db.view(viewname)
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
