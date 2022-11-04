import psycopg2
import numpy as np
import psycopg2.extras as extras
import pandas as pd
from sqlalchemy import create_engine
from datetime import date
import random

class ConnString:
    def __init__(self,dbname="findb",host="localhost",user="findb",password=input("input password"),port="5432", dialect="postgresql", driver="psycopg2"):
        self.dbname=dbname 
        self.host=host
        self.user=user
        self.password=password
        self.port=port
        self.dialect = dialect
        self.driver = driver

    
    def sqlalch_conn_string(self):
        #dialect+driver://username:password@host:port/database
        return  fr"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
    
    def psycopg2_conn_string(self):
        return f"dbname={self.dbname} host={self.host} user={self.user} password={self.password}"
    


#Psycopg2 functions

def psy_conn(dbname="findb", host="localhost", user="findb", password=input("input server password")):
    try:
        conn = psycopg2.connect(f"dbname={dbname} host={host} user={user} password={password}")
    except: 
        print("an exception occured")
    return conn

def psy_create_cur(conn, com=True):
    cur = conn.cursor()
    if com == True:
        conn.autocommit = True
    else:
        return(cur)
    return(cur)

def execute_query(cur, query, filepath=False):
    if filepath==False:
        cur.execute(f"""{query}
        """)
    else:
        try: 
            file = open(filepath, encoding="utf-8")
            cur.execute(f"""file""")
        finally:
            file.close()

def close_connection(conn):
    conn.close()

#Sql Alchemy Functions

def create_engine_sqlalch(connstr):
    engine = create_engine(connstr)
    engine.connect()
    return engine



##Pandas Functions

def get_sql_df(filepath):
    df = pd.read_csv(filepath)
    return df

def rename_columns(df):
    df2 = df.rename(columns={'oldName1': 'newName1', 'oldName2': 'newName2'})
    return df2

def add_columns_bofa(df, statement_owner_name="no_name",upload_date=date.today()):
    statement_owner = []
    upload_date_column = []
    upload_batch_id = []
    batch_id = random.random()
    
    for i in df['Reference Number'].values:
        statement_owner.append(statement_owner_name)
    for i in df['Reference Number'].values:
        upload_date_column.append(upload_date)
    for i in df['Reference Number'].values:
        upload_batch_id.append(batch_id)
    df['statement_owner'] = statement_owner
    df['upload_date_'] = upload_date_column
    df['upload_batch_id'] = upload_batch_id
    return df

def write_df_to_db(df, conn, table_name):
    df2 = df.to_sql(table_name, conn, if_exists = 'append', index=False)
    


#testing script

#Posted Date	Reference Number	Payee	Address	Amount

my_df = get_sql_df(filepath="test_file.csv")
my_df2 = add_columns_bofa(my_df)
print(my_df2)

connstr = ConnString()
connstr2 = connstr.sqlalch_conn_string()
conn2 = create_engine_sqlalch(connstr2)
write_df_to_db(my_df2, conn2, 'test')
connstr2 = connstr.psycopg2_conn_string()
con = psy_conn()
psy_create_cur(con)
close_connection(con)

#script

# connstr = ConnString()
# s = connstr.sqlalch_conn_string()

# conn = psy_conn()
# cur = psy_create_cur(conn)

# conn.close()

# if __name__ == "__main__":
#     main()