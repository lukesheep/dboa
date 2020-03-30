from pymongo import MongoClient
import subprocess
import pickle
import util_func


def data_sort(type_list):
    if "string" in type_list:
        return "string"
    elif "date" in type_list:
        return "date"
    elif "double" in type_list:
        return "double"
    elif "long" in type_list:
        return "long"
    else:
        return type_list[0]


def from_mongo(username, password, **kwargs):
    client = MongoClient()
    db = client[kwargs["db"]]

    # Getting tables
    c = db.list_collection_names()
    full_keys = dict()
    util_func.makedir("Migrations")
    with open('Migrations/tables', 'wb') as fp:
        pickle.dump(c, fp)
    # Getting columns information
    for col in c:
        c2 = db[col].find({})
        full_keys[col] = []
        columns = list()
        for doc in c2:
            key_set1 = set((doc.keys()))
            key_set2 = set((full_keys[col]))
            key_set = key_set2.union(key_set1)
            full_keys[col] = key_set
        for key in full_keys[col]:
            columns_name = key
            columns_dtype = "data_type"
            null_check = "YES"
            if key == "_id":
                pk_check = "PRI"
            else:
                pk_check = "NO"
            default_check = "None"
            auto_check = "NO"
            columns.append((columns_name, columns_dtype, null_check, pk_check, default_check, auto_check))
        filename ="Migrations/" + col + "_column"
        with open(filename, 'wb') as fp:
            pickle.dump(columns, fp)

        for index in range(len(columns)):
            columns[index] = list(columns[index])
            field = str(columns[index][0])
            fielddolar = "$" + field
            pipeline = [{'$sample': {'size': 10000}}]
            dict1 = {}
            dict1['$type'] = fielddolar
            dict2 = {}
            dict2[field] = dict1
            pipeline.append({'$project': dict2})
            c3 = db[col].aggregate(pipeline)
            for dicio in c3:
                listoftype = []
                listoftype.append(dicio[field])
            if len(listoftype) > 1:
                columns[index][1] = data_sort(listoftype)
            else:
                columns[index][1] = listoftype[0]
        with open(filename, 'wb') as fp:
            pickle.dump(columns, fp)

    # Getting the data itself

    for col in c:
        process_name = "mongoexport --db "+ kwargs["db"] +" --collection "+col+" --out Migrations/"+col+".json --jsonArray"
        subprocess.call(process_name)
