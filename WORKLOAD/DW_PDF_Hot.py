"""
d:/Python_Code/instante_md/Scripts/python.exe d:/Python_Code/instante_md/WORKLOAD/DW_PDF_Hot.py
"""

from time import perf_counter, sleep
time_start = perf_counter()

from random import uniform as rdm
import pandas as pd
import DB_Scripts
import configs
import os
from Telegram_funcs import Send_Telegram_Message
from urllib.request import urlretrieve as udw
import urllib.request
from urllib.error import HTTPError 

import logging
from datetime import date
today = date.today()
# LOGGING
logger_filename = "Download_PDFs"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
Folder = f"Logs/{today} Logs"
if not os.path.exists(Folder):
    os.makedirs(Folder)
formatter = logging.Formatter(
    '%(asctime)s >> %(levelname)s >> %(name)s >> %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
file_handler = logging.FileHandler(f'{Folder}/{logger_filename}.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
print(f"# {'_'*60}")



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


PDF_Hot_ALL = set(os.listdir(configs.Folder_PDF_Hotar_eSSD))


dfHotar = dfHotar_dplink()
print(len(dfHotar))



opener = urllib.request.build_opener()
opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36')]
urllib.request.install_opener(opener)

Downloaded = 0
for index, value in enumerate(dfHotar["doc_link"]):
    if f"{value}.pdf" in PDF_Hot_ALL:
        continue
    
    time_start_dl = perf_counter()
    docy = value
    print(f"Downloading #{Downloaded} : {docy}")
    docy_link = f"{configs.PDF_Generic}{docy}"
    docy_path = f"{configs.Folder_PDF_Hotar_eSSD}/{docy}.pdf"
    try:
        udw(docy_link, docy_path)
    except HTTPError as e:
        logger.debug(f"{e} >>{value}<<")
        print(e)
    
    time_end_dl = perf_counter()
    took_dl = round(time_end_dl - time_start_dl, 2)
    print(f"_{took_dl} sec_{'_'*60}")
    Downloaded += 1
    sleep(rdm(0.1,0.9))
    
time_end = perf_counter()
took = round(time_end - time_start, 2)
Send_Telegram_Message(configs.T_Constantin, f"DONE DWD {Downloaded} hotarari, took {took}")