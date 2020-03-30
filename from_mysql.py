import mysql.connector
import json
import pickle
import util_func


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super(DatetimeEncoder, obj).default(obj)
        except TypeError:
            return str(obj)


def from_mysql(username, password, **kwargs):
    parameters = {'user': username, 'passwd': password, 'host': kwargs["host"], 'database': kwargs["db"]}
    mydb = mysql.connector.connect(**parameters)
    tables = list()
    mycursor = mydb.cursor()
    d_cursor = mydb.cursor(dictionary=True)

    # Getting tables
    dbname = kwargs["db"]
    mycursor.execute("SHOW FULL TABLES FROM %s where Table_Type = 'BASE TABLE'" % dbname)
    tables_dump = mycursor.fetchall()
    print("got tables")
    util_func.makedir("Migrations")

    for x in tables_dump:
        table_name = str(x[0])
        tables.append(table_name)
    print("tables listed")
    with open('Migrations/tables', 'wb') as fp:
        pickle.dump(tables, fp)
    print("tables file")
    # Getting columns information
    for x in tables:
        mycursor.execute("SHOW columns FROM %s" % (x))
        columns_dump = mycursor.fetchall()
        print(x)
        columns = list()
        table_name = x
        for x in columns_dump:
            columns_name = str(x[0])
            columns_dtype = str(x[1])
            null_check = str(x[2])
            pk_check = str(x[3])
            if pk_check == "" or pk_check == "MUL":
                pk_check = "NO"
            default_check = str(x[4])
            auto_check = str(x[5])
            if auto_check == 'auto_increment':
                auto_check = 'YES'
            else:
                auto_check = 'NO'
            columns.append((columns_name, columns_dtype, null_check, pk_check, default_check, auto_check))
        filename = "Migrations/" + table_name + "_column"
        with open(filename, 'wb') as fp:
                pickle.dump(columns, fp)
        print(table_name + " column file")
    # Getting the data itself

    for x in tables:
        d_cursor.execute("select * FROM %s" % (x))
        data_dump = d_cursor.fetchall()
        print(x + "data load")
        filename = "Migrations/" + x + ".json"
        with open(filename, 'w') as fp:
            json.dump(data_dump, fp, cls=DatetimeEncoder)
        print(x + "data loaded")

    # Getting Referential Constraints
    for x in tables:
        mycursor.execute("select distinct c.constraint_name, c.column_name, c.referenced_column_name, c.referenced_table_name, t.update_rule, t.delete_rule from information_schema.key_column_usage c, information_schema.referential_constraints t where c.constraint_name<>'PRIMARY' and  c.constraint_name = t.constraint_name and c.constraint_schema = '{}' and c.table_name = '{}' ".format(dbname,x))
        constraint_dump = mycursor.fetchall()

        constraints = list()
        for cons in constraint_dump:
            table = x
            column = cons[1]
            ref_col = cons[2]
            ref_table = cons[3]
            upd_rule = cons[4]
            delete_rule = cons[5]
            constraints.append((table, column, ref_col, ref_table, upd_rule, delete_rule))
        filename = "Migrations/" + x + "_constraints"

        with open(filename, 'wb') as fp:
            pickle.dump(constraints, fp)
