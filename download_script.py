import os
import io
import zipfile
import datetime
from tqdm import tqdm

def get_url(year):
    if year >=2022:
        return (f"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/precos-semestrais-ca.zip" if year == 2022 else f"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{year}-01.zip",
                f"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{year}-02.zip" if year != current_year else None)
    
    
    return (f"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{year}-01.csv",
            f"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{year}-02.csv" if year != current_year else None)

def create_folder(year):
    if not os.path.exists("csv_data"):
        os.mkdir("csv_data")
    if not os.path.exists(f"csv_data/{year}"):
        os.mkdir(f"csv_data/{year}")
        return True
    
    if len(os.listdir(f"csv_data/{year}/"))>0:
            return False
   

def unzip_csv(year,data):
    zip_content = io.BytesIO(data)
    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
        zip_ref.extractall(f"csv_data/{year}/")
        filelist= os.listdir(f"csv_data/{year}/")
        if len(filelist) == 2:
            for file in filelist:
                splited_name = file.split("-") if year == 2022 else file.split("_")
                semester = splited_name[splited_name.__len__()-1].split(".")[0] if year == 2022 else splited_name[splited_name.__len__()-1].split(".")[1]
                os.rename(f"csv_data/{year}/{file}", f"csv_data/{year}/preco_combustivel_{year}_{int(semester)}.csv")

def download_csv(year):
    urls_tuple = get_url(year)
    
    if not create_folder(year):
        tqdm.write(f"Skipping {year}. The CSV has already been downloaded")
        return False
    
    semester = 1
    import requests
    for url in urls_tuple:
        tqdm.write(f"Downloading from: {url}")
        response = requests.get(url)
        if not response.ok:
            tqdm.write(f"Something got wrong downloading: {url}")
            return False
        
        filename = f"csv_data/{year}/preco_combustivel_{year}_{semester}.csv"
        if year >= 2022:
            unzip_csv(year,response.content)
            tqdm.write(f"Donwloaded: {year}/{semester}")
            semester+=1
            continue
        
        with open(filename, "wb") as file:
            file.write(response.content)

        tqdm.write(f"Donwloaded: {year}/{semester}")
        semester+=1
    
    return True
    

date = datetime.date.today()
current_month = date.month
current_year = date.year
years_list = list(range(2004,current_year+1)) if current_month>6 else list(range(2004,current_year))
url_list = list(map(download_csv,years_list))