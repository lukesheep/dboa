import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
import pickle


def to_psql(username, password2, **kwargs):
    if username != "alexjr2":
        print(username)
        mydb = psycopg2.connect(
            user=username,
            password=password2
        )
        db_name = kwargs["db"]
        host_name = kwargs["host"]
        mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = mydb.cursor()
        # Create database
        cur.execute("DROP DATABASE IF EXISTS % s;" % db_name)
        cur.execute("CREATE DATABASE %s;" % db_name)
        mydb = psycopg2.connect(
            dbname=db_name,
            user=username,
            password=password2
        )
        cur = mydb.cursor()
        try:
            with open("Migrations/create_type", 'rb') as fp:
                data_creation = pickle.load(fp)
            for string in data_creation:
                cur.execute(string)
        except FileNotFoundError:
            pass

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
                    creation_string = creation_string + " SERIAL "
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
            cur.execute(creation_string)

        # Insert data
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
                cur.execute(insert_string)

        # Insert keys
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
                    cur.execute(alter_string)
        mydb.commit()
        cur.close()
        mydb.close()

    if username == "alexjr2":
        mydb = psycopg2.connect(
            user="postgres",
            password=password2
        )
        db_name = kwargs["db"]
        host_name = kwargs["host"]
        mydb.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = mydb.cursor()
        # Create database
        cur.execute("DROP DATABASE IF EXISTS % s;" % db_name)
        cur.execute("CREATE DATABASE %s;" % db_name)
        mydb = psycopg2.connect(
            dbname=db_name,
            user="postgres",
            password=password2
        )
        cur = mydb.cursor()
        try:
            with open("Migrations/pres/create_type", 'rb') as fp:
                data_creation = pickle.load(fp)
            for string in data_creation:
                cur.execute(string)
        except FileNotFoundError:
            pass

        with open('Migrations/pres/tables', 'rb') as fp:
            tables = pickle.load(fp)
        for x in tables:
            creation_string = "CREATE TABLE " + x + " ("
            pk_string = ", PRIMARY KEY("
            column_file = "Migrations/pres/" + x + "_column"
            with open(column_file, 'rb') as fp:
                columns = pickle.load(fp)

            for x in columns:
                creation_string = creation_string + x[0] + " "
                print(x)
                if x[5] == "YES":
                    creation_string = creation_string + " SERIAL "
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
            cur.execute(creation_string)

        # Insert data
        for x in tables:
            data_file = "Migrations/pres/" + x + ".json"
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
                cur.execute(insert_string)

        # Insert keys
        for x in tables:
            constraint_file = "Migrations/pres/" + x + "_constraints"
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
                    cur.execute(alter_string)
        mydb.commit()
        cur.close()
        mydb.close()
