# d:/Python_Code/instante_md/Scripts/python.exe d:/Python_Code/instante_md/WORKLOAD/DW_PDF_Hot.py

from time import perf_counter, sleep
time_start = perf_counter()

from random import uniform as rdm
import pandas as pd
import DB_Scripts
import configs
import os
from Telegram_funcs import Send_Telegram_Message
from urllib.request import urlretrieve as udw

def dfHotar_dplink(Table = configs.Inst_Hotar_Table):
    dfHotar_Query = f"""
    SELECT TO_DATE(data_publicarii, 'YYYY-MM-DD') as datap, doc_link
    FROM public.{Table}
    ORDER BY datap DESC 
    """
    df = DB_Scripts.sql_readpd_custom(Table, dfHotar_Query)
    df["doc_link"] = [x.split("/")[-1] for x in df["doc_link"]]
    #print(df["doc_link"][1])
    return df


PDF_Hot_ALL = set(os.listdir(configs.Folder_PDF_Hotar) + os.listdir(configs.Folder_PDF_Hotar_eSSD))
print(f"Already existing {len(PDF_Hot_ALL):,} docs")


dfHotar = dfHotar_dplink()
print(len(dfHotar))


Downloaded = 0
for index, value in enumerate(dfHotar["doc_link"]):
    if f"{value}.pdf" in PDF_Hot_ALL:
        continue
    
    time_start_dl = perf_counter()
    docy = value
    print(f"Downloading #{Downloaded} : {docy}")
    docy_link = f"{configs.PDF_Generic}{docy}"
    docy_path = f"{configs.Folder_PDF_Hotar}/{docy}.pdf"
    udw(docy_link, docy_path)
    
    time_end_dl = perf_counter()
    took_dl = round(time_end_dl - time_start_dl, 2)
    print(f"_{took_dl} sec_{'_'*60}")
    Downloaded += 1
    #sleep(rdm(0.1,0.3))
    
time_end = perf_counter()
took = round(time_end - time_start, 2)
Send_Telegram_Message(configs.T_Constantin, f"DONE DWD {Downloaded} hotarari, took {took}")