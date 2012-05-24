{
    "_id": "_design/testrender",
    "views": {
        "name": {"map" : "function(doc){emit(doc.name,doc);}"},
        "compound": {"map" : "function(doc){emit([doc.first_name,doc.last_name],doc);}"}
    }
}
