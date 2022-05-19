import os
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



