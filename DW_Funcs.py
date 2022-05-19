from time import sleep
import pandas as pd
import grequests #grequests must be imported before requests
import requests
from bs4 import BeautifulSoup as bs
import configs
from DB_Scripts import insert_df_data, sql_readpd_custom, execute_sql
from random import uniform as rdm
import re



def total_results(Linkx, items_page = 10):

    if Linkx != configs.URL_Agend:
        URLx = f"{configs.URL_Generic}{Linkx}&items_per_page={items_page}"
    else: 
        URLx = f"{configs.URL_Generic}{Linkx}"
    print(URLx)
    response = requests.get(URLx)
    soup = bs(response.text, 'html.parser')
    results = soup.find("div", {"class": "aaij-pager"}).find("span", {"class": "results"}).get_text()
    results = re.sub('[^0-9]', '', results)
    try: 
        Total_results = int(results)
    except ValueError:
        Total_results = 10**4
    return Total_results


def total_pages_results(results, items_per_page =10):
    items_per_page =10
    pages = results // items_per_page
    if results % items_per_page == 0:
        pages -= 1
    #print(pages)
    return pages

#Gets the links of pdfs
def list_links(Linkx, page, items_page):
    try:

        if page == 0:
            URLx = f"{configs.URL_Generic}{Linkx}&items_per_page={items_page}"
        else: 
            URLx = f"{configs.URL_Generic}{Linkx}&items_per_page={items_page}&page={page}"
        response = requests.get(URLx)
        soup = bs(response.text, 'html.parser')
        table = soup.find('table')
        links = []
        for tr in table.findAll("td"):
            try:
                link = tr.find('a')['href']
                links.append(link)
            except TypeError:
                #print(e)
                #links.append("")
                pass
        return links
    except AttributeError: 
        return []


def df_html(Linkx, page, items_page):
    if page == 0:
        URLx = f"{configs.URL_Generic}{Linkx}&items_per_page={items_page}"
    else: 
        URLx = f"{configs.URL_Generic}{Linkx}&items_per_page={items_page}&page={page}"
    print("URLx =" , URLx)
    try:
        dft = pd.read_html(URLx, index_col=False)[0]
    except ValueError:
        URLx = f"{configs.URL_Generic}{Linkx}"
        dft_0 = pd.read_html(URLx, index_col=False)[0]
        dft = pd.DataFrame(columns=dft_0.columns)
    return dft


def df_and_links(Linkx, page, cols_list, items_page):
    listx = list_links(Linkx, page, items_page)
    
    sleep_time = round(rdm(1,3),2)
    sleep(sleep_time)
    
    dfx = df_html(Linkx, page, items_page)
    dfx= dfx.replace({"'":"",",":";","'":""})
    dfx["doc_link"] = listx
    dfx= dfx.fillna("x")
    dfx.columns = tuple(cols_list)
    return dfx


def Update_Table (vTable , vURL, vCols, vitems_page, vDuplicate_criteria2_column, update_all = False, update_until_page = 10, page =0):

    vDuplicate_criteria2 = f"AND  T1.{vDuplicate_criteria2_column} = T2.{vDuplicate_criteria2_column}"
    Total_results = total_results(vURL)
    Total_results_check = 0
    dft_copy = []
    
    retry_count = 0
    if update_all:
        retry_max = 100
    else:
        retry_max = 10

    while True:
        try:
            if Total_results_check > Total_results: break

            dft = df_and_links(vURL, page, vCols, items_page = vitems_page)
            if len(dft)==0:
                break

            if len(dft_copy) == 0:
                #if dft["data_publicarii"][1] raises error, might break here. Hence checking with copy might be futile
                print(f"page = {page}, \ndft['{vDuplicate_criteria2_column}'][1] = {dft[vDuplicate_criteria2_column][1]}")
            else: 
                print(f"page = {page}, \ndft['{vDuplicate_criteria2_column}'][1] = {dft[vDuplicate_criteria2_column][1]},\ndft_copy['{vDuplicate_criteria2_column}'][1] = {dft_copy[vDuplicate_criteria2_column][1]}")
                if (page>0 and dft["doc_link"].tolist() == dft_copy["doc_link"].tolist()) or len(dft)==0:
                    print("Breaking")
                    break #Here break
                        
            insert_df_data(dft, vTable)
            print(f"Inserted df to DB {vTable} from page = {page}")
            #Delete duplicates, keeps latest
            
            dft_copy = dft.copy()
            
            #Checks if the first element of dtf["doc_link"] exists in DB, thus break
            #?? Is it wise- Assumptions: they cannot modify docs once uploaded and keep same link
            # = check if nothing new on 2nd! page, as the first page seems populated with bad dates
            if page > update_until_page and not update_all:
                first_link = dft["doc_link"][0]
                sqlq = f"""
                SELECT count(doc_link)
                FROM public.{vTable}
                where doc_link = '{first_link}'
                LIMIT 1
                """
                if sql_readpd_custom(configs.Inst_Hotar_Table,sqlq)["count"][0] > 0:
                    print("Top link exists or page limit reached, breaking")
                    break
            
            Total_results_check += len(dft)
            
            #Just for tests
            """            
            if page % 5 == 0 and page>0:
                page += 0
            else:
                page += 1
            """
            page += 1

            sleep_time = round(rdm(1,3),2)
            if update_all:
                sleep_time *= 2 #Sleep twice as more if updating all tables == True
            sleep(sleep_time)

        except Exception as e:
            print(e)
            if retry_count < retry_max :
                retry_count += 1
                sleep(round(rdm(10,50),2))
                continue
                
            else:
                break

        
        print(f"{'_'*60}")

    sql_dupl = f"""
        DELETE   FROM public.{vTable} T1
        USING       public.{vTable} T2
        WHERE  T1.id       < T2.id          --deletes the older onces, as low index = older
        AND    T1.doc_link = T2.doc_link
        {vDuplicate_criteria2}
        """
    execute_sql(sql_dupl)

    return Total_results_check


def Update_Table_Agend (vTable= configs.Inst_Agend_Table , vURL = configs.URL_Agend,\
    vCols = configs.Inst_Agend_Cols, reversed = True, vitems_page= 10, \
    vDuplicate_criteria2_column = "data_sedintei",\
    update_all = False, update_until_page = 10, page =0, page_last = 10**15):
    
    a = 1



#Reversed pages!!!
def Update_Table_Agend (vTable= configs.Inst_Agend_Table , vURL = configs.URL_Agend,\
    vCols = configs.Inst_Agend_Cols, reversed = True, vitems_page= 10, \
    vDuplicate_criteria2_column = "data_sedintei",\
    update_all = False, update_until_page = 10, page =0, page_last = 10**15):

    retry_count = 0
    if update_all:
        retry_max = 100
    else:
        retry_max = 10
    
    vDuplicate_criteria2 = f"AND  T1.{vDuplicate_criteria2_column} = T2.{vDuplicate_criteria2_column}"
    Total_results = total_results(vURL, vitems_page)
    print(Total_results)
    Total_pages_results = total_pages_results(Total_results, vitems_page)
    print(Total_pages_results)


    Total_results_check = 0
    dft_copy = []

    if update_all:
        update_until_page = Total_pages_results

    if reversed:
        rangex = range(Total_pages_results, Total_pages_results- update_until_page, -1)
    else:
        rangex = range(0, update_until_page)

    loops = 0
    for page in rangex:
        try:
            if reversed == True and page > page_last:
                    continue
            else:
                pass

            if Total_results_check > Total_results: break
            
            dft = df_and_links(vURL, page, vCols, items_page = vitems_page)
            if len(dft)==0 and loops > 0:
                break

            if len(dft_copy) == 0:
                #if dft["data_publicarii"][1] raises error, might break here. Hence checking with copy might be futile
                print(f"page = {page}, \ndft['{vDuplicate_criteria2_column}'][1] = {dft[vDuplicate_criteria2_column][1]}")
            else: 
                print(f"page = {page}, \ndft['{vDuplicate_criteria2_column}'][1] = {dft[vDuplicate_criteria2_column][1]},\ndft_copy['{vDuplicate_criteria2_column}'][1] = {dft_copy[vDuplicate_criteria2_column][1]}")
                if (page>0 and dft["doc_link"].tolist() == dft_copy["doc_link"].tolist()) or len(dft)==0:
                    print("Breaking")
                    break #Here break
                        
            insert_df_data(dft, vTable)
            print(f"Inserted df to DB {vTable} from page = {page}")
            #Delete duplicates, keeps latest
            
            dft_copy = dft.copy()
            
            #Checks if the first element of dtf["doc_link"] exists in DB, thus break
            #?? Is it wise- Assumptions: they cannot modify docs once uploaded and keep same link
            # = check if nothing new on 2nd! page, as the first page seems populated with bad dates
            if loops > update_until_page and not update_all:
                first_link = dft["doc_link"][0]
                sqlq = f"""
                SELECT count(doc_link)
                FROM public.{vTable}
                where doc_link = '{first_link}'
                LIMIT 1
                """
                if sql_readpd_custom(configs.Inst_Hotar_Table,sqlq)["count"][0] > 0:
                    print("Top link exists, breaking")
                    break
            
            Total_results_check += len(dft)
            
            loops += 1

            sleep_time = round(rdm(0.03,0.8),2)
            if update_all:
                sleep_time *= 2 #Sleep twice as more if updating all tables
                if int(rdm(1,20)) == 10: sleep_time+= 10 #even more sleep with 5% chance
            sleep(sleep_time)

        except Exception as e:
            print(e)
            if retry_count < retry_max :
                retry_count += 1
                sleep(round(rdm(10,50),2))
                continue
                
            else:
                break

        
        print(f"{'_'*60}")

    sql_dupl = f"""
        DELETE   FROM public.{vTable} T1
        USING       public.{vTable} T2
        WHERE  T1.id       < T2.id          --deletes the older onces, as low index = older
        AND    T1.doc_link = T2.doc_link
        {vDuplicate_criteria2}
        """
    execute_sql(sql_dupl)
    
    return Total_results_check, page



#Reversed pages!!!
def Update_Table_Agend_grequests(vTable= configs.Inst_Agend_Table , vURL = configs.URL_Agend,\
    vCols = configs.Inst_Agend_Cols, vitems_page= 10, update_until_page = 10):
    
    """    #________________
    vTable= configs.Inst_Agend_Table
    vURL = configs.URL_Agend
    vCols = configs.Inst_Agend_Cols
    reversed = True
    vitems_page= 10
    vDuplicate_criteria2_column = "data_sedintei"
    update_all = False
    update_until_page = 10
    page =0
    page_last = 10**15
    #________________
    """
    Total_results = total_results(vURL, vitems_page)
    Total_results = total_results(configs.URL_Agend, 10)
    
    print(f"Total_results = {Total_results}")
    
    Total_pages_results = total_pages_results(Total_results, vitems_page)
    print(f"Total_pages_results {Total_pages_results}")
    rangex = range(Total_pages_results, Total_pages_results- update_until_page, -1)


    Request_URLs = []
    for page in rangex:
        Request_URLs.append(f"{configs.URL_Generic}{vURL}&items_per_page={vitems_page}&page={page}")


    rs = (grequests.get(u) for u in Request_URLs)
    print("created grequests ")
    Request_Responses = grequests.map(rs, size= 5)


    dfa = pd.DataFrame(columns= configs.Inst_Agend_Cols)
    for page, response in enumerate(Request_Responses):
        try:
            dfx1 = pd.read_html(response.text)[0]
            dfx1.columns =  vCols
            dfa = pd.concat([dfa, dfx1], ignore_index=True)
        except Exception as e:
            print(e)
                
    insert_df_data(dfa, vTable)


    Total_results_check = len(dfa)
            
    sql_dupl = f"""
        DELETE   FROM public.{vTable} T1
        USING       public.{vTable} T2
        WHERE  T1.id       < T2.id          --deletes the older onces, as low index = older
        AND    T1.nr_dosar = T2.nr_dosar
        AND T1.ora_sedintei = T2.ora_sedintei
        AND T1.data_sedintei = T2.data_sedintei 
        """
    execute_sql(sql_dupl)
    
    return Total_results_check
