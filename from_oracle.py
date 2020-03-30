import cx_Oracle
import json
import pickle
import util_func

def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]

    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow


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
