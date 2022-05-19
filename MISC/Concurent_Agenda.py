from time import perf_counter

import grequests  


import configs
from DW_Funcs import total_results

Linkx = "agenda-sedintelor?Instance=All&Denumire_dosar=&Numarul_cazului=&Obiectul_cauzei=&Tipul_dosarului=All&page="
Linkx_results = Linkx+"1"
print(Linkx_results)
Pages = total_results(Linkx_results, 10) //10

print(Pages)
Pages_to_update = 10

Linkx_10 = [f"{configs.URL_Generic}{Linkx}{pagenr}" for pagenr in range(Pages, Pages -Pages_to_update -1, -1)]
print(Linkx_10[1])
print(Linkx_10[-1])


urls = Linkx_10
unsent_request = (grequests.get(url) for url in urls)

time_start = perf_counter()
results = grequests.map(unsent_request) 

#print(results[1].text)

import pandas as pd

df1 = pd.read_html(results[1].text)[0]

time_end = perf_counter()
took = int(time_end-time_start)
print(took)