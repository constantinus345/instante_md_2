import shutil
from time import time
import configs
import os
from msilib.schema import File
import pathlib


def list_of_files(Folder_Source, Folder_Dest):
    Files_Source = []
    if not os.path.exists(Folder_Dest):
        os.makedirs(Folder_Dest)

    for file in pathlib.Path(Folder_Source).glob('**/*'):
        filex = str(file.absolute()).replace(os.path.sep,"/")
        #"." not in filex means getting only directories and files without extensions
        #if "." in filex and all(exc.casefold() not in filex.casefold() for exc in configs.Exclusion_List): 
        Files_Source.append(filex)
    #.replace(Folder_Source, Folder_Dest)
    Files_Dest = [x.replace(Folder_Source, Folder_Dest) for x in Files_Source]
    return (Files_Source, Files_Dest)

def move_files(Folder_Source, Folder_Dest):
    Filess = list_of_files(Folder_Source, Folder_Dest)
    #configs.Folder_Source_3520, configs.Folder_Dest_3520
    #print(len(Filess))
    #print(len(Filess[0]))
    #print(len(Filess[1]))
    len_to_copy = len(Filess[0])
    Errors = []
    for file_index, file_name in enumerate(Filess[0]):
        file_source = Filess[0][file_index]
        file_dest= Filess[1][file_index]
        try:
            #shutil.copytree(file_source, file_dest, dirs_exist_ok=False)
            os.makedirs(os.path.dirname(file_dest), exist_ok=True)
            shutil.copyfile(file_source, file_dest) 
            if file_index% 1000 ==0:
                print(f"Done {file_index}/{len_to_copy}")
        except Exception as e: 
            print(e)
            Errors.append(e)

    print(Errors)
    print("Done")

Folder_From = 'W:/instante_md'
Folder_To = 'F:/instante_md'

move_files(Folder_From, Folder_To)