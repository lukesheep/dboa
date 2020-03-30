import pyodbc
import pickle, json, util_func

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
