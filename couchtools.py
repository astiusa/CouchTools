#!/usr/bin/env python

import couchdb.client as couch
import json
from uuid import uuid4

class CouchTools(object):
    """ a class for doing things with couch. """
    def __init__(self,dbname='Test',init_new=False):
        self.server = couch.Server()
        if init_new:
            if dbname in self.server:
                del self.server[dbname]
            self.db = self.server.create(dbname)

    def change_db(self,dbname):
        self.server[dbname]
        return self

    def docid(self):
        """ Don't let couch make its own uuid. Apparently that's bad I guess? """
        return uuid4().hex

    # FIXME this sucks.
    def clean_up_data(self,textobj):
        """ Turn a dict into a JSON string then back into a dict, but sans bullshit."""
        try:
            jsond = json.dumps(textobj)
            jsond = jsond.replace('\\r\\n','').replace('\\u2013','').replace('\xc5','')
            return json.loads(jsond)
        except UnicodeDecodeError:
            print "There was a UnicodeDecodeError: ",textobj

    def get(self,id):
        """ Get a document by ID. """
        return self.db.get(id, default={})

    # FIXME this is not right at all.
    def put(self,id,path_to_attach):
        filename = open(path_to_attach)
        return self.db.put_attachment(id,filename,path_to_attach)

    # FIXME needs maor werk
    def load(self,path):
        """ Given a path to a file, load the database, ie do lots of inserts."""
        payload = open(path).readlines()
        for x in payload:
            try:
                self.save(x)
            except: 
                pass # TODO something? I guess. 

    def save(self,doc,id=None):
        """ save thing to couch. """
        try:
            if id:
                doc['_id'] = id
                self.db.save(doc)
            else:
                doc['_id'] = self.docid()
                self.db.save(doc)
        except UnicodeDecodeError: # this should be redone
            cleandoc = self.clean_up_data(doc)
            self.db.save(cleandoc)
