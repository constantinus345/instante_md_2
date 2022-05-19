#& d:/Python_Code/instante_md/Scripts/python.exe d:/Python_Code/instante_md/WORKLOAD/Update_Citat.py
"""
Agend!!! SKA
Hotar
Inche
Citat
Dosar
"""

from time import perf_counter as time
time_start = time()
from DW_Funcs import Update_Table
import configs
from Telegram_funcs import Send_Telegram_Message

#Citat
vTable = configs.Inst_Citat_Table
vURL = configs.URL_Citat
vCols = configs.Inst_Citat_Cols
vitems_page = 1000
vDuplicate_criteria2_column = "data_sedintei"

update_all = False
update_until_page = 2
page =0
try:
        Total_rows = Update_Table (vTable , vURL, vCols, vitems_page, vDuplicate_criteria2_column, update_all, update_until_page, page)
except Exception as e:
        time_end = time()
        took = int(time_end-time_start)
        Report = f"""Update_Citat: Error {e}, took = {took} seconds
                """.replace("  ","")
        Send_Telegram_Message(configs.T_Constantin, Report)