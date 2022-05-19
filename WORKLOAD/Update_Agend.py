#& d:/Python_Code/instante_md/Scripts/python.exe d:/Python_Code/instante_md/WORKLOAD/Update_Agend.py
"""
Agend!!! SKA
Hotar
Inche
Citat
Dosar
"""
from time import perf_counter as time
time_start = time()
from DW_Funcs import Update_Table_Agend_grequests
import configs
from Telegram_funcs import Send_Telegram_Message
update_until_page = 20

try:
        Total_rows = Update_Table_Agend_grequests(update_until_page = update_until_page)
        print("DONE")
except Exception as e:
        print(e)
        time_end = time()
        took = int(time_end-time_start)
        Report = f"""Update_Agenda: error in Update_Table_Agend_grequests(), {e}, , took = {took} seconds
        """.replace("  ","")
        Send_Telegram_Message(configs.T_Constantin, Report)
        print("ERROR")