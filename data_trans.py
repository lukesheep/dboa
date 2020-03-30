import pickle
import json



def to_mysql():
    with open('Migrations/tables', 'rb') as fp:
        tables = pickle.load(fp)

    for table in tables:
        filename = "Migrations/" + table + "_column"
        with open(filename, 'rb') as fp:
            columns = pickle.load(fp)

        for index in range(len(columns)):
            columns[index] = list(columns[index])
            columns[index][1] = columns[index][1].upper()

            if columns[index][5] == "YES":
                columns[index][1] = "INT"

            # Character data type
            elif "ARRAY" in columns[index][1]:
                columns[index][1] = "LONGTEXT"

            elif "TSVECTOR" in columns[index][1]:
                columns[index][1] = "LONGTEXT"

            elif "NCHAR" in columns[index][1]:
                columns[index] = "CHAR" + columns[index][1][5:]

            elif "NVARCHAR2" in columns[index][1]:
                columns[index] = "VARCHAR" + columns[index][1][9:]

            elif "NVARCHAR" in columns[index][1] or "VARCHAR2" in columns[index][1]:
                columns[index][1] = "VARCHAR" + columns[index][1][8:]

            elif ("NTEXT" in columns[index][1] or "CLOB" in columns[index][1] or "STR" in columns[index][1]):
                columns[index][1] = "TEXT"

            # Numeric Types
            elif "INTEGER" in columns[index][1]:
                columns[index][1] = "INT"

            elif "NUMBER" == columns[index][1] or "NUMBER(*)" == columns[index][1]:
                columns[index][1]= "DOUBLE"

            elif "NUMBER" in columns[index][1]:


                columns[index][1] = columns[index][1][7:-1]
                if "," in columns[index][1]:
                    r = columns[index][1].split(",")
                    if int(r[1]) > 0:
                        columns[index][1] = "DECIMAL(" + r[0] + "," + r[1] + ")"

                    elif int(r[0]) < 9 :
                        columns[index][1] = "INT"

                    elif int(r[0]) >= 9 and int(r[0]) < 19:
                        columns[index][1] = "BIGINT"

                    columns[index][1] = "DECIMAL(" + r[0] + ")"

                elif int(columns[index][1]) < 9 :
                    columns[index][1] = "INT"

                elif int(columns[index][1]) < 19 and int(columns[index][1]) >= 9:
                    columns[index][1] = "BIGINT"

                columns[index][1] = "DECIMAL(" + columns[index][1] + ")"

            # Binary Types
            elif "REAL" in columns[index][1] or "BINARY_DOUBLE" in columns[index][1] or "DOUBLE PRECISION" in columns[index][1]:
                columns[index][1] = "DOUBLE"

            elif "NUMERIC" in columns[index][1]:
                columns[index][1] = "DECIMAL" + columns[index][1][7:]

            elif "MONEY" in columns[index][1] or "SMALLMONEY" in columns[index][1]:

                columns[index][1] = "DECIMAL"

            elif "BINARY_FLOAT" in columns[index][1]:
                columns[index][1] = "FLOAT"

            elif "IMAGE" in columns[index][1]:
                columns[index][1] = "VARBINARY(MAX)"

            elif "BFILE" in columns[index][1]:
                columns[index][1] = "VARCHAR(255)"

            elif "LONG RAW" in columns[index][1] or "BYTEA" in columns[index][1]:
                columns[index][1] = "LONGBLOB"

            elif "RAW" in columns[index][1]:
                columns[index][1] = columns[index][3:-1]
                if columns[index][1] < 256:
                    columns[index][1] = "BINARY(" + columns[index][1] + ")"

                elif columns[index][1] >= 256:
                    columns[index][1] = "VARBINARY(" + columns[index][1] + ")"

            # DATE Types
            elif "DATIMEOFFSET" in columns[index][1] or "DATETIME2" in columns[index][1] or "SMALLDATETIME" in columns[index][1]:
                columns[index][1] = "DATETIME"
            elif "INTERVAL" in columns[index][1]:
                columns[index][1] = "VARCHAR(30)"
            elif "TIME ZONE" in columns[index][1]:
                columns[index][1] = "TIMESTAMP"
        with open(filename, 'wb') as fp:
            pickle.dump(columns, fp)

def to_psql():
    create_type = list()
    with open('Migrations/tables', 'rb') as fp:
        tables = pickle.load(fp)

    for table in tables:
        filename = "Migrations/" + table + "_column"
        with open(filename, 'rb') as fp:
            columns = pickle.load(fp)

        for index in range(len(columns)):
            columns[index] = list(columns[index])
            columns[index][1] = columns[index][1].upper()
            if columns[index][5] == "YES":
                columns[index][1] = "INTEGER"

            # Character data type
            elif "NCHAR" in columns[index][1]:
                columns[index][1] = "CHAR" + columns[index][1][5:]

            elif "NVARCHAR" in columns[index][1] or "VARCHAR2" in columns[index][1]:
                columns[index][1] = "VARCHAR" + columns[index][1][8:]

            elif "NTEXT" in columns[index][1] or "TINYTEXT" in columns[index][1] or "MEDIUMTEXT" in columns[index][1] or "LONGTEXT" in columns[index][1] or "CLOB" in columns[index][1] or "NCLOB" in columns[index][1] or "STR" in columns[index][1]:
                columns[index][1] = "TEXT"

            elif "NVARCHAR2" in columns[index][1]:
                columns[index][1] = "VARCHAR" + columns[index][1][9:]

            elif "ENUM" in columns[index][1]:
                type_string = "CREATE TYPE " + columns[index][0] + "_ENUM AS " + columns[index][1]
                columns[index][1] = columns[index][0] + "_ENUM"
                create_type.append(type_string)

            # Numeric types

            elif "TINYINT" in columns[index][1]:
                columns[index][1] = "SMALLINT"

            elif "MEDIUMINT" in columns[index][1] or "INT" in columns[index][1]:
                columns[index][1] = "INTEGER"

            elif "LONG" in columns[index][1]:
                columns[index][1] = "TEXT"

            elif "NUMBER" in columns[index][1]:
                columns[index][1] = columns[index][1][7:-1]
                if "," in columns[index][1]:
                    r = columns[index][1].split(",")
                    if int(r[1]) > 0:
                        columns[index][1] = "NUMERIC(" + r[0] + "," + r[1] + ")"

                    elif int(r[0]) < 9 :
                        columns[index][1] = "INT"

                    elif int(r[0]) >= 9 and int(r[0]) < 19:
                        columns[index][1] = "BIGINT"

                    columns[index][1] = "NUMERIC(" + r[0] + ")"

                elif int(columns[index][1]) < 9 :
                    columns[index][1] = "INT"

                elif int(columns[index][1]) < 19 and int(columns[index][1]) >= 9:
                    columns[index][1] = "BIGINT"

                columns[index][1] = "NUMERIC(" + columns[index][1] + ")"

            # Binary types
            elif "FLOAT" in columns[index][1] or "BINARY_DOUBLE" in columns[index][1]:
                columns[index][1] = "DOUBLE PRECISION"
            elif "BINARY_FLOAT" in columns[index][1]:
                columns[index][1] = "REAL"
            elif "SMALLMONEY" in columns[index][1]:
                columns[index][1] = "MONEY"
            elif "BINARY" in columns[index][1] or "VARBINARY" in columns[index][1] or "BLOB" in columns[index][1] or "RAW" in columns[index][1] or "binDATA" in columns[index][1]:
                columns[index][1] = "BYTEA"
            elif "BFILE" in columns[index][1]:
                columns[index][1] = "VARCHAR(255)"

            # Date types
            elif "SMALLDATETIME" in columns[index][1]:
                columns[index][1] = "TIMESTAMP(0)"

            elif "DATETIME" in columns[index][1]:
                columns[index][1] = "TIMESTAMP(3)"

            elif "YEAR" in columns[index][1]:
                columns[index][1] = "INTEGER"

            elif "DATATIMEOFFSET" in columns[index][1]:
                columns[index][1] = "TIMESTAMP(" + columns[index][1][10:-1] + ") WITH TIME ZONE"

            elif "BIT" in columns[index][1]:
                columns[index][1] = "BOOLEAN"
        with open(filename, 'wb') as fp:
            pickle.dump(columns, fp)
    filename = "Migrations/create_type"
    with open(filename, 'wb') as fp:
        pickle.dump(create_type, fp)

def to_oracle():
    with open('Migrations/tables', 'rb') as fp:
        tables = pickle.load(fp)

    for table in tables:
        filename = "Migrations/" + table + "_column"
        with open(filename, 'rb') as fp:
            columns = pickle.load(fp)

        for index in range(len(columns)):
            columns[index] = list(columns[index])
            columns[index][1] = columns[index][1].upper()

            if columns[index][5] == "YES":
                columns[index][1] = "NUMBER GENERATED ALWAYS AS IDENTITY"

            elif "ENUM" in columns[index][1]:
                columns[index][1] = "VARCHAR2(50)"

            # Character types

            elif "VARCHAR" in columns[index][1]:
                columns[index][1] = "VARCHAR2" + columns[index][1][7:]
            elif "NVARCHAR" in columns[index][1]:
                columns[index][1] = "NVARCHAR2" + columns[index][1][8:]
            elif "TEXT" in columns[index][1] or "STR" in columns[index][1]:
                columns[index][1] = "CLOB"


            # Numeric Types
            elif "INT" in columns[index][1] or "NUMERIC" in columns[index][1]:
                columns[index][1] = "NUMBER"

            # Binary types
            elif "DECIMAL" in columns[index][1] or "FLOAT" in columns[index][1] or "MONEY" in columns[index][1]:
                columns[index][1] = "BINARY_FLOAT"

            elif "REAL" in columns[index][1] or "DOUBLE" in columns[index][1] or "DOUBLE PRECISION" in columns[index][1]:
                columns[index][1] = "BINARY_DOUBLE"

            elif "BINARY" in columns[index][1]:
                columns[index][1] = "RAW" + columns[index][1][6:]

            elif "VARBINARY" in columns[index][1]:
                if columns[index][1][10:-1] == "MAX":
                    columns[index][1] = "LONG RAW"
                else:
                    columns[index][1] = "RAW" + columns[index][1][9:]
            elif "BYTEA" in columns[index][1]:
                columns[index][1] = "BLOB"

            # Time and Date timezone

            elif "TIME" in columns[index][1]:
                columns[index][1] = "DATE"
            elif "YEAR" in columns[index][1]:
                columns[index][1] = "NUMBER"

            # Boolean

            elif "BOOLEAN" in columns[index][1] or "BOOL" in columns[index][1] or "BIT" in columns[index][1]:
                columns[index][1] = "NUMBER(1)"
        with open(filename, 'wb') as fp:
            pickle.dump(columns, fp)
def to_mssql():
    with open('Migrations/tables', 'rb') as fp:
        tables = pickle.load(fp)

    for table in tables:
        filename = "Migrations/" + table + "_column"
        with open(filename, 'rb') as fp:
            columns = pickle.load(fp)

        for index in range(len(columns)):
            columns[index] = list(columns[index])
            columns[index][1] = columns[index][1].upper()

            if columns[index][5] == "YES":
                columns[index][1] = "INT IDENTITY(1,1)"

            elif "ENUM" in columns[index][1]:
                columns[index][1] = "VARCHAR"

            # Character Types
            elif columns[index][1] == "TINYTEXT" or columns[index][1] == "MEDIUMTEXT" or columns[index][1] == "LONGTEXT":
                columns[index][1] = "TEXT"
            elif columns[index][1] == "STRING" or columns[index][1] == "STR":
                columns[index][1] = "TEXT"
            elif columns[index][1] == "CLOB":
                columns[index][1] = "VARCHAR(MAX)"
            elif columns[index][1] == "NCLOB":
                columns[index][1] = "NVARCHAR(MAX)"
            elif "VARCHAR2" in columns[index][1]:
                columns[index][1] = "VARCHAR" + columns[index][1][8:]
            elif "NVARCHAR2" in columns[index][1]:
                columns[index][1] = "NVARCHAR" + columns[index][1][9:]

            # Numeric Types
            elif "INT" in columns[index][1]:
                columns[index][1] = "int"
            elif "NUMBER" in columns[index][1]:
                columns[index][1] = columns[index][1][7:-1]
                if "," in columns[index][1]:
                    r = columns[index][1].split(",")
                    if int(r[1]) > 0:
                        columns[index][1] = "NUMERIC(" + r[0] + "," + r[1] + ")"

                    elif int(r[0]) < 9:
                        columns[index][1] = "INT"

                    elif int(r[0]) >= 9 and int(r[0]) < 19:
                        columns[index][1] = "BIGINT"

                    columns[index][1] = "NUMERIC(" + r[0] + ")"

                elif int(columns[index][1]) < 9 :
                    columns[index][1] = "INT"

                elif int(columns[index][1]) < 19 and int(columns[index][1]) >= 9:
                    columns[index][1] = "BIGINT"

                columns[index][1] = "NUMERIC(" + columns[index][1] + ")"

            elif "MEDIUMINT" in columns[index][1] or "LONG" in columns[index][1]:
                columns[index][1] = "BIGINT"

            elif "DOUBLE" in columns[index][1]:
                columns[index][1] = "FLOAT"

            elif "BINARY_FLOAT" in columns[index][1]:
                columns[index][1] = "FLOAT"

            # Binary Types

            elif "BLOB" in columns[index][1] or "BFILE" in columns[index][1]:
                columns[index][1] = "VARCHAR(MAX)"
            elif "RAW" in columns[index][1]:
                if "LONG" in columns[index][1]:
                    columns[index][1] = "VARBINARY(MAX)"
                else:
                    columns[index][1] = "VARBINARY" + columns[index][1][3:]
            elif "BTYA" in columns[index][1]:
                columns[index][1] = "VARCHAR(MAX)"
            elif "BINDATA" in columns[index][1]:
                columns[index][1] = "BINARY"

            # Date type
            elif "YEAR" in columns[index][1]:
                columns[index][1] = "INT"
            elif "INTERVAL" in columns[index][1]:
                columns[index][1] = "DATETIME"

            elif "BOOLEAN" in columns[index][1] or "BOOL" in columns[index][1]:
                columns[index][1] = "BIT"
        with open(filename, 'wb') as fp:
            pickle.dump(columns, fp)
def to_mongo():
    pass
