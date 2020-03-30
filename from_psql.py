import psycopg2
import psycopg2.extras
import json
import pickle
import util_func


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super(DatetimeEncoder, obj).default(obj)
        except TypeError:
            return str(obj)


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
                columns.append((columns_name, columns_dtype, null_check, pk_check, default_check, auto_check))
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
