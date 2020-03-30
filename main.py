from kivy.app import App
from kivy.uix.textinput import TextInput
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.graphics import Rectangle, Color
from kivy.uix.scrollview import ScrollView
from kivy.core.window import Window
import kivy.utils
import data_trans, from_mysql, to_mysql, from_psql, to_psql, from_mongo, from_mssql, from_oracle,to_mssql, to_oracle, conn_test, to_mongo
import threading
import readb

Window.size = (900, 700)


class Janela(BoxLayout):

    def migration(self):
        self.ids["source_lb"].text = ""
        self.ids["datatrans_lb"].text = ""
        self.ids["finished_lb"].text = ""
        event = threading.Event()
        event2 = threading.Event()

        def read_db(infoarg, userarg, passarg, **kwargs):
            self.ids["source_lb"].text = "Extracting..."
            {
             "MySQL": from_mysql.from_mysql,
             "Postgres": from_psql.from_psql,
             "Oracle": from_oracle.from_oracle,
             "SQL Server": from_mssql.from_mssql,
             "Mongo DB" : from_mongo.from_mongo
            }[infoarg](userarg, passarg, host=kwargs['hostkwarg'], db=kwargs['dbkwarg'])
            print("got data")
            event.set()

        def trans_db(inforarg):
            event.wait()
            self.ids["source_lb"].text = "Extraction complete"
            self.ids["datatrans_lb"].text = "Transforming..."
            {
             "MySQL": data_trans.to_mysql,
             "Postgres": data_trans.to_psql,
             "Oracle": data_trans.to_oracle,
             "SQL Server": data_trans.to_mssql,
             "Mongo DB" : data_trans.to_mongo
            }[inforarg]()
            print("data transformed")
            event2.set()

        def load_db(infoarg, userarg, passarg, **kwargs):
            event2.wait()
            self.ids["datatrans_lb"].text = "Transformation complete"
            self.ids["finished_lb"].text = "Loading..."
            {
            "MySQL": to_mysql.to_mysql,
            "Postgres": to_psql.to_psql,
            "Oracle": to_oracle.to_oracle,
            "SQL Server": to_mssql.to_mssql,
            "Mongo DB" : to_mongo.to_mongo
            }[infoarg](userarg, passarg, host=kwargs['hostkwarg'], db=kwargs['dbkwarg'])
            print("migration done")
            self.ids["finished_lb"].text = "Finished"

        db_info = self.ids["source"].text
        db_host = self.ids["dbhost1"].text
        db_user = self.ids["dbuser1"].text
        db_pass = self.ids["dbpass1"].text
        db1 = self.ids["db1"].text
        db_info2 = self.ids["destination"].text
        db_host2 = self.ids["dbhost2"].text
        db_user2 = self.ids["dbuser2"].text
        db_pass2 = self.ids["dbpass2"].text
        db2 = self.ids["db2"].text
        conntest = {
        "MySQL": conn_test.conn_mysql,
        "Postgres": conn_test.conn_psql,
        "Oracle": conn_test.conn_oracle,
        "SQL Server": conn_test.conn_mssql,
        "Mongo DB" : conn_test.conn_mongo
        }[db_info](db_user, db_pass, host=db_host, db=db1)
        self.ids["Connection_test_lb"].text = conntest[0]
        if conntest[0] == "Connection Failed":
            return
        t = threading.Thread(target=read_db, args=(db_info, db_user, db_pass), kwargs={'hostkwarg': db_host, 'dbkwarg': db1}, daemon=True)
        t.start()
        t2 = threading.Thread(target=trans_db, args=(db_info2,), daemon=True)
        t2.start()
        t3 = threading.Thread(target=load_db, args=(db_info2, db_user2, db_pass2), kwargs={'hostkwarg': db_host2, 'dbkwarg': db2}, daemon=True)
        t3.start()
        return

class DboaApp(App):
    def build(self):
        return Janela()

if __name__ == "__main__":
    DboaApp().run()
