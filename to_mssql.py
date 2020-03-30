import pyodbc
import pickle, json, util_func

def to_mssql(dbuser, dbpass, **kwargs):
    server = kwargs["host"]
    database = kwargs["db"]
    username = dbuser
    password = dbpass
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    with open('Migrations/tables', 'rb') as fp:
        tables = pickle.load(fp)
    for x in tables:
        creation_string = "CREATE TABLE " + x + " ("
        pk_string = ", PRIMARY KEY("
        column_file = "Migrations/" + x + "_column"
        with open(column_file, 'rb') as fp:
            columns = pickle.load(fp)

        for x in columns:
            creation_string = creation_string + x[0] + " "
            print(x)
            if x[5] == "YES":
                creation_string = creation_string + " GENERATED ALWAYS AS IDENTITY "
            else:
                creation_string = creation_string + x[1]
            if x[2] == "NO":
                creation_string = creation_string + " NOT NULL "
            if x[4] != "None":
                creation_string = creation_string + " DEFAULT " + x[4]
            if x[3] == "PRI":
                pk_string = pk_string + x[0] + ", "
            if x[3] == "UNI":
                creation_string = creation_string + " UNIQUE "
            creation_string = creation_string + ", "
        creation_string = creation_string[:-2]
        pk_string = pk_string[:-2] + ")"
        creation_string = creation_string + pk_string + ")"
        print(creation_string)
        cursor.execute(creation_string)

    for x in tables:
        data_file = "Migrations/" + x + ".json"
        with open(data_file, 'rb') as fp:
            data_load = json.load(fp)
        for row in data_load:
            insert_string = 'insert into ' + x + '('
            value_string = ' values ('
            for key, value in row.items():
                insert_string = insert_string + key + ", "
                if str(value).isnumeric():
                    value_string = value_string + str(value) + ', '
                else:
                    value_string = value_string + "'" + str(value) + "', "
            insert_string = insert_string[:-2] + ")"
            value_string = value_string[:-2] + ")"
            insert_string = insert_string + value_string
            print(insert_string)
            cursor.execute(insert_string)

        # Keys
    for x in tables:
        constraint_file = "Migrations/" + x + "_constraints"
        with open(constraint_file, 'rb') as fp:
            constraints = pickle.load(fp)
        for constraint in constraints:
            if not constraint:
                pass
            else:

                alter_string = "ALTER TABLE " + x + " ADD FOREIGN KEY (" + constraint[1] + ") references " + constraint[3] + "(" + constraint[2] + ") "
                if constraint[5] == 'CASCADE':
                    alter_string = alter_string + "ON DELETE CASCADE "
                elif constraint[5] == 'RESTRICT':
                    alter_string = alter_string + "ON DELETE RESTRICT "
                elif constraint[5] == 'SET NULL':
                    alter_string = alter_string + "ON DELETE SET NULL "
                elif constraint[5] == 'RESTRICT':
                    alter_string = alter_string + "ON DELETE SET DEFAULT "
                if constraint[4] == 'CASCADE':
                    alter_string = alter_string + "ON UPDATE CASCADE "
                elif constraint[4] == 'RESTRICT':
                    alter_string = alter_string + "ON UPDATE RESTRICT "
                elif constraint[4] == 'SET NULL':
                    alter_string = alter_string + "ON UPDATE SET NULL "
                elif constraint[4] == 'RESTRICT':
                    alter_string = alter_string + "ON UPDATE SET DEFAULT "
                print(alter_string)
                cursor.execute(alter_string)
    cnxn.commit()
    cursor.close()
    cnxn.close()
