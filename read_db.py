from pymongo import MongoClient
import pymongo
import pyodbc
import mysql.connector
import cx_Oracle
import psycopg2
import json
import pickle
import util_func
import subprocess


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super(DatetimeEncoder, obj).default(obj)
        except TypeError:
            return str(obj)


def type_read(type, **kwargs):
    type_string = type
    if type.upper() == "NUMERIC" or type.upper() == "DECIMAL":
        if kwargs["scale"]:
            type_string = type + "(" + str(kwargs["precision"]) + ", " + str(kwargs["scale"]) + ")"
        else:
            type_string = type + "(" + str(kwargs["precision"]) + ")"
    time_list = ("datetimeoffset", "datetime2", "time")
    if type.lower() in time_list:
        type_string = type + "(" + str(kwargs["time"]) + ")"
    string_list = ("char", "varchar", "nchar", "nvarchar", "binary", "varbinary")
    if type.lower() in string_list:
        type_string = type + "(" + str(kwargs["length"]) + ")"
    return type_string


def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]

    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow


def data_type(type, **kwargs):
    type_dict = {"bigserial": "serial(8)", "bit varying": "varbit", "character": "char", "character varying": "varchar"}
    numeric_list = ["numeric", "time without time zone", "time with time zone", "timestamp without time zone", "timestamp with time zone"]
    max_list = ["bit", "bit varying", "character", "character varying"]
    if type in type_dict.keys():
        columns_dtype = type_dict[type]
    else:
        columns_dtype = type
    if type in max_list:
        columns_dtype = columns_dtype + "(" + kwargs["m"] + ")"
    if type in numeric_list:
        columns_dtype = columns_dtype + "(" + kwargs["p"] + ")"
    if type == "numeric" and kwargs["s"] != 0 and kwargs["s"] != "None":
        columns_dtype = columns_dtype[:-1] + ", " + kwargs["s"] + ")"
    if type == "user-defined":
        columns_dtype = "enum("

    return columns_dtype


def conn_mongo(username, password, **kwargs):
    client = MongoClient()
    db = client[kwargs["db"]]
    c = client.list_database_names()
    if kwargs["db"] in c:
        return ["Connection worked", "#69e653"]
    else:
        return ["Connection Failed", "#d63324"]

    client.close()


def conn_mssql(dbuser, dbpass, **kwargs):

    server = kwargs["host"]
    database = kwargs["db"]
    username = dbuser
    password = dbpass
    try:
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cnxn.close()
    except Exception as e:
        print(e)
        return ["Connection Failed", "#d63324"]
    else:
        return ["Connection worked", "#69e653"]


def conn_mysql(username, password, **kwargs):
    parameters = {'user': username, 'passwd': password, 'host': kwargs["host"], 'database': kwargs["db"]}
    try:
        mydb = mysql.connector.connect(**parameters)
        mydb.close()
    except Exception as e:
        print(e)
        return ["Connection Failed", "#d63324"]
    else:
        return ["Connection worked", "#69e653"]


def conn_oracle(username, password, **kwargs):
    try:
        conn = cx_Oracle.connect(username, password)
        conn.close()
    except Exception as e:
        print(e)
        return ["Connection Failed", "#d63324"]
    else:
        return ["Connection worked", "#69e653"]


def conn_psql(username, password, **kwargs):
    try:
        conn = psycopg2.connect(dbname=kwargs["db"], user=username, password=password)

    except Exception as e:
        print(e)
        return ["Connection Failed", "#d63324"]
    else:
        return ["Connection worked", "#69e653"]


def from_mongo(username, password, **kwargs):
    pass


def from_mssql(dbuser, dbpass, **kwargs):

    server = kwargs["host"]
    database = kwargs["db"]
    username = dbuser
    password = dbpass
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    # Getting TABLES
    tables = list()
    columns = list()
    constraints = list()
    cursor.execute("select table_name from information_schema.tables where table_type = 'BASE TABLE'" )
    rows = cursor.fetchall()
    for row in rows:
        tables.append(row.table_name)
    util_func.makedir("Migrations")
    with open("Migrations/tables","wb") as fp:
        pickle.dump(tables, fp)
    print("Got tables")

    # Getting Columns
    for table in tables:
        cursor.execute("SELECT column_name, column_default, is_nullable, data_type, character_maximum_length, NUMERIC_PRECISION, DATETIME_PRECISION, NUMERIC_SCALE from information_schema.columns where table_name = '%s'" % table)
        rows = cursor.fetchall()
        cursor.execute("SELECT name FROM sys.identity_columns WHERE OBJECT_NAME(object_id) = '%s'" % table)
        identity = cursor.fetchall()
        pk_list = list()
        for row in cursor.primaryKeys(table=table):
            pk_list.append(row.column_name)
        for row in rows:

            if any(e[0] == row.column_name for e in identity):
                auto_increment = "YES"
            else:
                auto_increment = "NO"
            if row[0] in pk_list:
                pk = "PRI"
            else:
                pk = "NO"
            default = str(row[1])
            if "((" in default:
                default = int(default[2:-2])
            elif "(" in default:
                default = default[1:-1]
            type = type_read(row.data_type, length=row.character_maximum_length, precision=row.NUMERIC_PRECISION, scale=row.NUMERIC_SCALE, time = row.DATETIME_PRECISION)
            columns.append((row.column_name, type, row.is_nullable, pk, default, auto_increment, "NO"))
        filename = "Migrations/" + table + "_columns"
        with open(filename, "wb") as fp:
            pickle.dump(columns, fp)

    # Gettin Data
    for table in tables:
        cursor.execute("select * from %s" % table)
        columns = [column[0] for column in cursor.description]
        data_dump = list()
        for row in cursor.fetchall():
            data_dump.append(dict(zip(columns, row)))
        filename = "Migrations/" + table + ".json"
        with open(filename, "w") as fp:
            json.dump(data_dump, fp)

    # Getting constraints
    for table in tables:
        for row in cursor.foreignKeys(table=table):
            table = table
            column = row[7]
            ref_col = row[3]
            ref_table = row[2]
            upd_rule = row[9]
            delete_rule = row[10]
            constraints.append((table, column, ref_col, ref_table, upd_rule, delete_rule))
        filename = "Migration/" + table + "_constraints"
        with open(filename, 'wb') as fp:
            pickle.dump(constraints, fp)


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
    util_func.makedir("Migrations")

    for x in tables_dump:
        table_name = str(x[0])
        tables.append(table_name)

    with open('Migrations/tables', 'wb') as fp:
        pickle.dump(tables, fp)

    # Getting columns information
    for x in tables:
        mycursor.execute("SHOW columns FROM %s" % (x))
        columns_dump = mycursor.fetchall()
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
    # Getting the data itself

    for x in tables:
        d_cursor.execute("select * FROM %s" % (x))
        data_dump = d_cursor.fetchall()
        filename = "Migrations/" + x + ".json"
        with open(filename, 'w') as fp:
            json.dump(data_dump, fp, cls=DatetimeEncoder)

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


def from_oracle(username, password, **kwargs):
    conn = cx_Oracle.connect(username, password)
    mycursor = conn.cursor()

    # Getting TABLES
    tables = list()
    constraints = list()
    mycursor.execute("select table_name from user_tables")
    tables_dump = mycursor.fetchall()

    for x in tables_dump:
        table_name = str(x[0])
        tables.append(table_name)
    with open('Migration/tables', 'wb') as fp:
        pickle.dump(tables, fp)

    # Getting column information
    for table in tables:
        parameter = {'table_name': table.upper()}
        mycursor.execute('select column_name, data_type, data_length, data_precision, nullable, data_default from all_tab_columns where table_name = :table_name', parameter)
        columns_dump = mycursor.fetchall()
        parameter = {'table_name': table.upper(), 'ct': 'P'}
        mycursor.execute('select c.column_name from all_cons_columns c, all_constraints t where t.table_name = :table_name AND t.constraint_type = :ct AND t.constraint_name = c.constraint_name ', parameter)
        pk_check_list = mycursor.fetchall()

        columns = list()
        pk_list = list()
        for x in pk_check_list:
            pk_name = x[0]
            pk_list.append(pk_name)
        for x in columns_dump:
            columns_name = str(x[0])
            columns_dtype = str(x[1]) + "(" + str(x[2]) + ")"
            if columns_name in pk_list:
                pk_check = "PRI"
            else:
                pk_check = "NO"
            if x[4] == 'Y':
                null_check = "YES"
            else:
                null_check = "NO"
            default_check = str(x[5])
            if "nextval" in default_check:
                default_check = "None"
                auto_check = "YES"
            else:
                auto_check = "NO"
            columns.append((columns_name, columns_dtype, null_check, pk_check, default_check, auto_check))
        filename = "Migration/" + table + "_column"
        print(columns)
        with open(filename, 'wb') as fp:
            pickle.dump(columns, fp)
    # Getting the data itself
    for table in tables:
        mycursor.execute('select * from %s' % (table))
        mycursor.rowfactory = makeDictFactory(mycursor)
        data_dump = mycursor.fetchall()
        print(data_dump)
        filename = "Migration/" + table + ".json"
        with open(filename, 'w') as fp:
            json.dump(data_dump, fp)

    # Getting the constraints
    for table in tables:
        mycursor.execute("select c.column_name, c1.table_name, c1.column_name, con.delete_rule from all_constraints con, all_cons_columns c, all_cons_columns c1 where con.CONSTRAINT_TYPE = 'R' AND con.table_name='%s' AND con.constraint_name = c.constraint_name and con.r_constraint_name = c1.constraint_name" % table)
        rows = mycursor.fetchall()
        for row in rows:
            constraints.append((table, row[0], row[2], row[1], "NO ACTION" ,row[3]))
    filename = "Migration/" + table + "_constraints"
    with open(filename, 'wb') as fp:
        pickle.dump(constraints, fp)
    mycursor.close()
    conn.close()


def from_psql(username, password, **kwargs):
    conn = psycopg2.connect(
        dbname=kwargs["db"],
        user=username,
        password=password
    )

    cur = conn.cursor()
    d_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    tables = list()

    # Getting tables
    cur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog'AND schemaname != 'information_schema';")
    tables_dump = cur.fetchall()

    for x in tables_dump:
        table_name = str(x[0])
        tables.append(table_name)
    util_func.makedir("Migrations")
    with open("Migrations/tables", 'wb') as fp:
        pickle.dump(tables, fp)

    # Getting columns information
    for x in tables:
        cur.execute("SELECT column_name, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale from information_schema.columns where table_name = '%s'" % (x))
        columns_dump = cur.fetchall()
        print(columns_dump)
        cur.execute("SELECT c.column_name FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.table_name = '%s' AND t.constraint_type = 'PRIMARY KEY';" % (x))
        pk_check_list = cur.fetchall()
        table_name = x
        columns = list()
        pk_list = list()
        for x in pk_check_list:
            pk_name = x[0]
            pk_list.append(pk_name)
        for x in columns_dump:
                columns_name = str(x[0])
                columns_dtype = data_type(str(x[3]).lower(), m=str(x[4]), p=str(x[5]), s=str(x[6]))
                if columns_dtype == "enum(":
                    if x[1]:
                        default_check = str(x[1].split("::")[0])
                    cur.execute("select p.enumlabel from pg_type t, pg_enum p where p.enumtypid=t.oid AND t.typname='%s' " % str(x[1].split("::")[1]))
                    rows = cur.fetchall()
                    for row in rows:

                        columns_dtype = columns_dtype + "'" + row[0] + "', "
                    columns_dtype = columns_dtype[:-2] + ")"
                else:
                    default_check = str(x[1])
                null_check = str(x[2])
                if columns_name in pk_list:
                    pk_check = "PRI"
                else:
                    pk_check = "NO"
                dna_check = str(x[1])
                dna_check = dna_check[0:7]
                auto_check = "NO"
                if dna_check == "nextval":
                    auto_check = "YES"
                    default_check = "None"
                columns.append((columns_name, columns_dtype, null_check, pk_check, default_check, auto_check, "NO"))
        filename = "Migrations/" + table_name + "_column"
        with open(filename, 'wb') as fp:
            pickle.dump(columns, fp)

    # Getting the data itself

    for x in tables:
        d_cur.execute("select * FROM %s" % (x))
        data_dump = d_cur.fetchall()
        data = []
        for row in data_dump:
            data.append(dict(row))
        filename = "Migrations/" + x + ".json"
        with open(filename, 'w') as fp:
            json.dump(data, fp, cls=DatetimeEncoder)

    # Getting Referential Constraints
    for x in tables:
        cur.execute("select pg_get_constraintdef(c.oid), c.confupdtype, c.confdeltype from pg_catalog.pg_constraint as c, pg_catalog.pg_class as p where c.conrelid = p.oid and p.relname = '%s' and c.contype ='f'" % (x))
        results = cur.fetchall()
        constraints = list()
        for r in results:

            table = x
            split_string = r[0].split(" ", 5)
            split_rel = split_string[4].split("(")
            column = split_string[2][1:-1]
            ref_col = split_rel[1][:-1]
            ref_table = split_rel[0]
            if r[1] == "a":
                upd_rule = 'NO ACTION'
            elif r[1] == "r":
                upd_rule = 'RESTRICT'
            elif r[1] == "c":
                upd_rule = 'CASCADE'
            elif r[1] == "n":
                upd_rule = 'SET NULL'
            elif r[1] == "d":
                upd_rule = 'SET DEFAULT'
            if r[2] == "a":
                del_rule = 'NO ACTION'
            elif r[2] == "r":
                del_rule = 'RESTRICT'
            elif r[2] == "c":
                del_rule = 'CASCADE'
            elif r[2] == "n":
                del_rule = 'SET NULL'
            elif r[2] == "d":
                del_rule = 'SET DEFAULT'

            constraints.append((table, column, ref_col, ref_table, upd_rule, del_rule))
        filename = "Migrations/" + x + "_constraints"
        with open(filename, 'wb') as fp:
            pickle.dump(constraints, fp)
