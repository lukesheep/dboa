from pymongo import MongoClient
import pymongo
import pyodbc
import mysql.connector
import cx_Oracle
import psycopg2


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

    except  Exception as e:
        print(e)
        return ["Connection Failed", "#d63324"]
    else:
        return ["Connection worked", "#69e653"]
