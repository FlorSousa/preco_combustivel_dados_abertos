import pandas as pd
import os

columns_to_remove = ["Numero Rua","Complemento","Bairro","Cep","Valor de Compra","Unidade de Medida","CNPJ da Revenda"]
date = { "years":[int(year) for year in os.listdir("csv_data")], "semester":1 }
for year in date["years"]:
    chunks = pd.read_csv(f"csv_data\\{year}\\preco_combustivel_{year}_{ date["semester"]}.csv", chunksize=10000, delimiter=";")
    chunks = map(lambda chunk: chunk.drop(columns_to_remove, axis=1), chunks)
    df = pd.concat(list(chunks))
    #df.to_csv(f"csv_data/{year}/{year}_{ date["semester"]}.csv")
    date["semester"] = 2 if  date["semester"] == 1 else 1