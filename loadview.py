#!/usr/bin/env python

from couchtools import couchtools
import json
import sys

# TODO: use getopts or whatever python calls it
if __name__ == '__main__':
    dbname = sys.argv[1]
    db = Couchtools(dbname)
    if db.get('_design/render'):
        db.delete('_design/render')
    viewcode = open('views/map.js').read()
    view = json.loads(viewcode)
    view['_id'] = "_design/render"
    db.save(view)
