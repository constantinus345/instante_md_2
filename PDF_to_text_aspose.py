"""
d:/Python_Code/instante_md/Scripts/python.exe d:/Python_Code/instante_md/PDF_to_text_aspose.py
D:/Python/Python3.8.8/python.exe d:/Python_Code/instante_md/PDF_to_text_aspose.py
D:/Python/Python36/python.exe d:/Python_Code/instante_md/PDF_to_text_aspose.py
"""

import time
time_start_global = time.perf_counter()

import aspose.words as aw #Finally this worked as pdf parser! Motherfucker
import os
import configs

#from Telegram_funcs import Send_Telegram_Message

import logging
from datetime import date
today = date.today()
# LOGGING
logger_filename = "PDFs_toText_Aspose"

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


Files_PDF_Hotar = os.listdir(configs.Folder_PDF_Hotar_eSSD)
Files_TXT_Hotar = set(os.listdir(configs.Folder_TXT_Hotar))
print(len(Files_PDF_Hotar))
#print(Files_PDF_Hotar[0])
#print(len(set(Files_PDF_Hotar)))

text1 = "Evaluation Only. Created with Aspose.Words. Copyright 2003-2022 Aspose Pty Ltd."
text2 = "Created with an evaluation copy of Aspose.Words. To discover the full versions of our APIs please visit: https://products.aspose.com/words/"

def delete_text_txt(filename, *args):
    # Read in the file
    with open(filename, 'r', encoding= 'utf-8') as file :
        filedata = file.read()
    # Delete the target strings
    for arg in args:
        filedata = filedata.replace(arg, "")
    # Write the file out again
    with open(filename, 'w', encoding= 'utf-8') as file:
        file.write(filedata)

Problematic_PDFs = []
len_Files_PDF_Hotar = len(Files_PDF_Hotar)
Already_Existing = 0

for index, file_pdf in enumerate(Files_PDF_Hotar):
    try:
        if f"{file_pdf[:-4]}.txt" in Files_TXT_Hotar:
            Already_Existing += 1
            #print(Already_Existing)
            continue
        #file_pdf = "000000F9-42FC-E311-8E3E-005056A5D154.pdf"
        print(file_pdf)
        file_pdf_path = f"{configs.Folder_PDF_Hotar_eSSD}/{file_pdf}"
        file_txt_save = f"{configs.Folder_TXT_Hotar}/{file_pdf[:-4]}.txt"
        doc = aw.Document(file_pdf_path)
        print("doc")
        doc.save(file_txt_save)
        delete_text_txt(file_txt_save, text1, text2,"\n\n")
        print(f"DONE {index}/{len_Files_PDF_Hotar}, Already = {Already_Existing}")
            
    except Exception as e:
        print(e)
        #logger.exception(e, exc_info=True)
        logger.debug(f"{file_pdf}, e")
        Problematic_PDFs.append(file_pdf)

time_end_global = time.perf_counter()
took = round(time_end_global - time_start_global, 2)

Report = f"<<Aspose>> {len(Problematic_PDFs)} PDFs were not converted to text.\ntook = {took} seconds"
#Send_Telegram_Message(configs.T_Constantin, Report)
