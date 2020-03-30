from pymongo import MongoClient
import subprocess
import pickle
import json
import util_func


def to_mongo(username, password, **kwargs):
    client = MongoClient()
    db = client[kwargs["db"]]
    tables = list()
    fkeys = list()
    with open('Migrations/tables', 'rb') as fp:
        tables = pickle.load(fp)
    # embedding
    for table in tables:
        filename = "Migrations/" + table + "_constraints"
        with open(filename, 'rb') as fp:
            fkeys = pickle.load(fp)
        print("working in " + table)
        if fkeys and len(fkeys) == 1:
            nest_name = table + "_embed"
            key = fkeys[0][1]
            rkey = fkeys[0][2]
            rtable = fkeys[0][3]
            filename = "Migrations/" + table + ".json"
            with open(filename, 'rb') as fp:
                child = list(json.load(fp))
            filename = "Migrations/" + rtable + ".json"
            with open(filename, 'rb') as fp:
                parent = list(json.load(fp))
            parent_dict = {dict[rkey]: {**dict} for dict in parent}
            for dict in child:
                parent = parent_dict[dict[key]]
                del dict[key]
                parent.setdefault(nest_name, []).append(dict)
            parent = [dict for i, dict in parent_dict.items()]
            with open(filename, 'w') as fp:
                json.dump(parent, fp)
            tables.remove(table)

    # insert
    for table in tables:
        filename = "Migrations/" + table + ".json"
        with open(filename, 'rb') as fp:
            data_load = list(json.load(fp))
        db[table].insert_many(data_load)
        print(table + "is finished")
