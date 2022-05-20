from sqlalchemy import create_engine
import os
from sys import argv

T_key = "5333658490:AAHQ5hbHir5feIAO79FHHNs1uHj8eEFfWOI"
T_Constantin = 1307289323


items_page = 1000
"""
Agend
Hotar
Inche
Citat
Dosar
"""

#items_page_str = f"&items_per_page={items_page}"
items_page_str = ""
URL_Generic = "https://instante.justice.md/ro/"
URL_Agend = f"agenda-sedintelor?Instance=All&Denumire_dosar=&Numarul_cazului=&Obiectul_cauzei=&Tipul_dosarului=All{items_page_str}"
URL_Hotar = f"hotaririle-instantei?Instance=All&Numarul_dosarului=&Denumirea_dosarului=&date=&Tematica_dosarului=&Tipul_dosarului=All{items_page_str}"
URL_Inche = f"incheierile-instantei?Instance=All&Denumirea_dosarului=&Numarul_dosarului=&date=&Tematica_dosarului=&Tipul_dosarului=All{items_page_str}"
URL_Citat = f"citatii-publice?Instance=All&Numarul_dosarului=&solr_document_3=&PersoanaCitata=&solr_document_2={items_page_str}"
URL_Dosar = f"cereri-si-doasare-pendite?Instance=All&Denumirea_dosarului=&Numarul_dosarului=&Statutul_dosarului=&date=&Tipul_dosarului=All{items_page_str}"


DB_user='postgres'
DB_password='4721'
DB_host='127.0.0.1'
DB_port= '5432'

Databasex = "instante_md".lower()
engine = create_engine(f'postgresql://postgres:{DB_password}@localhost:{DB_port}/{Databasex}')

Inst_Agend_Table = "Agenda_Sedintelor".lower()
Inst_Agend_Cols = ["instanta", "nr_dosar", "judecator", "data_sedintei", "ora_sedintei", "sala_sedintei", "dosar_nume", "obiectul_cauzei", "dosar_tip", "rezultat_sedinta", "doc_tip"]
Inst_Agend_Cols_DB = """
id SERIAL PRIMARY KEY,
instanta TEXT,
nr_dosar TEXT,
judecator TEXT,
data_sedintei TEXT,
ora_sedintei TEXT,
sala_sedintei TEXT,
dosar_nume TEXT,
obiectul_cauzei TEXT,
dosar_tip TEXT,
rezultat_sedinta TEXT,
doc_tip TEXT,
doc_link TEXT
"""

Inst_Hotar_Table = "Hotararile_Instantei".lower()
Inst_Hotar_Cols = ["instanta", "nr_dosar","dosar_nume", "data_pronuntarii", "data_inregistrarii", "data_publicarii", "dosar_tip", "dosar_tematica", "judecator", "doc_tip", "doc_link"]
Inst_Hotar_Cols_DB = """
id SERIAL PRIMARY KEY,
instanta TEXT,
nr_dosar TEXT,
dosar_nume TEXT,
data_pronuntarii TEXT,
data_inregistrarii TEXT,
data_publicarii TEXT,
dosar_tip TEXT,
dosar_tematica TEXT,
judecator TEXT,
doc_tip TEXT,
doc_link TEXT
"""

Inst_Inche_Table = "Incheierile_Instantei".lower()
Inst_Inche_Cols = ["instanta", "nr_dosar", "dosar_nume", "data_inregistrarii", "data_publicarii", "dosar_tip", "dosar_tematica", "judecator", "doc_tip", "doc_link" ]
Inst_Inche_Cols_DB = """
id SERIAL PRIMARY KEY,
instanta TEXT,
nr_dosar TEXT,
dosar_nume TEXT,
data_inregistrarii TEXT,
data_publicarii TEXT,
dosar_tip TEXT,
dosar_tematica TEXT,
judecator TEXT,
doc_tip TEXT,
doc_link TEXT
"""

Inst_Citat_Table = "citatii_publice".lower()
Inst_Citat_Cols = ["instanta", "nr_dosar", "dosar_nume", "obiectul_cauzei", "data_sedintei", "ora_sedintei", "persoana_citata", "sala_sedintei", "judecator", "data_publicarii", "doc_tip", "doc_link"]
Inst_Citat_Cols_DB = """
id SERIAL PRIMARY KEY,
instanta TEXT,
nr_dosar TEXT,
dosar_nume TEXT,
obiectul_cauzei TEXT,
data_sedintei TEXT,
ora_sedintei TEXT,
persoana_citata TEXT,
sala_sedintei TEXT,
judecator TEXT,
data_publicarii TEXT,
doc_tip TEXT,
doc_link TEXT
"""

Inst_Dosar_Table = "dosare_cereri".lower()
Inst_Dosar_Cols = ["instanta", "nr_dosar", "nr_dosar_ref", "data_inregistrarii", "dosar_statut", "dosar_tip", "dosar_nume", "doc_tip", "doc_link" ]
Inst_Dosar_Cols_DB = """
id SERIAL PRIMARY KEY,
instanta TEXT,
nr_dosar TEXT,
nr_dosar_ref TEXT,
data_inregistrarii TEXT,
dosar_statut TEXT,
dosar_tip TEXT,
dosar_nume TEXT,
doc_tip TEXT,
doc_link TEXT
"""

PDF_Generic = "https://instante.justice.md/ro/pigd_integration/pdf/"

def mkdirx(Folderx):
     if not os.path.exists(Folderx):
          os.makedirs(Folderx)
"""
Agend
Hotar
Inche
Citat
Dosar
"""

Folder_Core = "E:/instante_md"
#Folder_PDF_Hotar = f"{Folder_Core}/PDF_Hotar"
Folder_PDF_Hotar_eSSD = f"{Folder_Core}/PDFs_Instante_eSSD"
Folder_TXT_Hotar = f"{Folder_Core}/TXT_Hotar"

mkdirx(Folder_Core)
#mkdirx(Folder_PDF_Hotar)
mkdirx(Folder_TXT_Hotar)



if __name__ == "__main__":
    print("Executing the main")
else: 
    print(f"Imported {argv[0]}")