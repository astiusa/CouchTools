{
    "_id": "_design/render",
    "views": {
        "name": {"map" : "function(doc){emit(doc.name,doc);}"}
    }
}