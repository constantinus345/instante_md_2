#& d:/Python_Code/ActeLocale/Scripts/python.exe d:/Python_Code/ActeLocale/creating_DB.py
import sys
import psycopg2
from sqlalchemy import create_engine
import configs 

def create_DB(DB = configs.Databasex):
   try:
      conn = psycopg2.connect(
         database= DB , user= configs.DB_user , password= configs.DB_password , host= configs.DB_host, port= configs.DB_port
      )
      cur = conn.cursor()
      conn.close()
   except Exception as e:
      print(e)
      conn = psycopg2.connect(
         database="postgres", user= configs.DB_user , password= configs.DB_password , host= configs.DB_host, port= configs.DB_port)
      
      cursor = conn.cursor()
      
      conn.autocommit = True
      sql = f'''CREATE database {configs.Databasex}'''
      
      #Creating a database
      cursor.execute(sql)
      print("Database created successfully........")
      conn.commit()
      #Closing the connection
      conn.close()
   #Creating a cursor object using the cursor() method

   conn = psycopg2.connect(
      database=configs.Databasex, user= configs.DB_user , password= configs.DB_password , host= configs.DB_host, port= configs.DB_port
   )
   cursor = conn.cursor()

"""
Fetches all tables and if Table_Codes is not there, it creates it
Uses ColumnsX schema
"""


def create_table(Table_name,Column_List):
   conn = psycopg2.connect(
      database=configs.Databasex, user= configs.DB_user , password= configs.DB_password , host= configs.DB_host, port= configs.DB_port
   )
   cursor = conn.cursor()
   cursor.execute("""SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'public'""")
   tables = [i[0] for i in cursor.fetchall()] # A list() of tables.

   if Table_name.lower() not in tables:
      #Creating table as per requirement
      sql =f'''CREATE TABLE {Table_name}({Column_List})'''
      #print(repr(sql))
      cursor.execute(sql)
      
      print(f"Table {Table_name} created successfully")
      conn.commit()
   else: 
      print(f"{Table_name} already exists")

   conn.close()


create_DB()

create_table(configs.Inst_Agend_Table, configs.Inst_Agend_Cols_DB)
create_table(configs.Inst_Hotar_Table, configs.Inst_Hotar_Cols_DB)
create_table(configs.Inst_Inche_Table, configs.Inst_Inche_Cols_DB)
create_table(configs.Inst_Citat_Table, configs.Inst_Citat_Cols_DB)
create_table(configs.Inst_Dosar_Table, configs.Inst_Dosar_Cols_DB)


if __name__ == "__main__":
    print("Executing the main")
else: 
    print(f"Imported {sys.argv[0]}")