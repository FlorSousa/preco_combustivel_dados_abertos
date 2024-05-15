import psycopg2
import numpy as np
import os
import pandas as pd

def get_connection():
    from dotenv import load_dotenv
    load_dotenv()
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def sanitizer_str(string):
    import re
    return re.sub(re.compile(r"'"),"''",string)

def load_dim_flag(dataframe):
    df_flags = dataframe['Bandeira'].unique()
    df_flags = df_flags.astype(object)
    df_flags = df_flags[~pd.isnull(df_flags)]
    flags_string = ",".join([f"'{sanitizer_str(flag)}'" for flag in df_flags])
    cursor.execute(f"SELECT * FROM public.dim_flag WHERE flag_type IN ({flags_string})")
    flags_in_db = [flag_type[1] for flag_type in cursor.fetchall()]
    flags_to_insert = filter(lambda f: True if sanitizer_str(f) not in flags_in_db else False, df_flags)
    queries = list(map(lambda flag: f"INSERT INTO dim_flag(flag_type) VALUES ('{sanitizer_str(flag)}');", flags_to_insert))
    [cursor.execute(q) for q in queries]

def load_dim_city(dataframe):
    df_cities = dataframe['Municipio'].unique().astype(object)
    df_cities = df_cities[~pd.isnull(df_cities)]
    cities_str = ",".join([f"'{sanitizer_str(city)}'" for city in df_cities])
    cursor.execute(f"SELECT * FROM public.dim_city WHERE city_name IN ({cities_str})")
    cities_in_db = [city[1] for city in cursor.fetchall()]
    cities_to_insert = filter(lambda f: True if sanitizer_str(f) not in cities_in_db else False, df_cities)
    queries = list(map(lambda city: f"INSERT INTO dim_city(city_name) VALUES ('{sanitizer_str(city)}');", cities_to_insert))
    [cursor.execute(q) for q in queries]
    
def replace_by_id(dataframe,dim,field_name) -> None:
    cursor.execute(f"SELECT * FROM public.{dim}")
    select_return = cursor.fetchall() 
    dim_map = {value: key for key, value in select_return}
    dataframe[field_name] = dataframe[field_name].replace(dim_map)
    
    
def insert_in_db(dataframe):
    dim_to_replace = {
        "dim_city":"Municipio",
        "dim_fuel":"Produto",
        "dim_region":"Regiao - Sigla",
        "dim_flag":"Bandeira",
        "dim_uf":"Estado - Sigla"
    }
    for dimension in dim_to_replace:
        replace_by_id(dataframe,dimension,dim_to_replace[dimension])
    
    columns_to_rename = {
        "Regiao - Sigla":"region_id",
        "Estado - Sigla":"uf_id",
        "Municipio":"city_id",
        "Revenda":"gas_station_name",
        "CNPJ da Revenda":"cnpj_gas_station",
        "Produto":"fuel_type_id",
        "Valor de Venda":"price",
        "Data da Coleta":"measurement_date"
    }
    dataframe = dataframe.rename(columns=columns_to_rename)
    print("Inserting data to DB")
    from datetime  import datetime
    dataframe['measurement_date'] = dataframe['measurement_date'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d'))
    query_insert = 'INSERT INTO measurment(price, measurement_date, gas_station_name, cnpj_gas_station, fuel_type_id, uf_id, city_id, region_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
    data_to_insert = dataframe[['price', 'measurement_date', 'gas_station_name', 'cnpj_gas_station', 'fuel_type_id', 'uf_id', 'city_id', 'region_id']].values.tolist()
    cursor.executemany(query_insert, data_to_insert)
    try:
        conn.commit()
        print("Data inserted")
    except Exception as error:
        print(f"Something got wrong: {error}")
        exit()
    
def load_data():
    columns_to_remove = ["Numero Rua","Nome da Rua","Complemento","Bairro","Cep","Valor de Compra","Unidade de Medida"]
    
    years = [int(year) for year in os.listdir("csv_data")]
    for year in years:
        num_semester = len(os.listdir(f"csv_data/{year}"))
        for semester in range(1,num_semester+1):
            chunks = pd.read_csv(f"csv_data/{year}/preco_combustivel_{year}_{semester}.csv", chunksize=10000, delimiter=";",encoding='ISO-8859-1')
            chunks = map(lambda chunk: chunk.drop(columns_to_remove, axis=1), chunks)
            cleaned_df = pd.concat(list(chunks))
            cleaned_df = cleaned_df.dropna()
            cleaned_df = cleaned_df[cleaned_df["Produto"].isin(["GASOLINA","ETANOL","DIESEL", "GNV"])]
            cleaned_df.columns = cleaned_df.columns.str.replace('ï»¿', '')
            cleaned_df["Valor de Venda"] = cleaned_df["Valor de Venda"].str.replace(',','.').astype(float)
            load_dim_flag(cleaned_df)
            load_dim_city(cleaned_df)
            insert_in_db(cleaned_df)
        print(f"\n{year} inserted in DB")

    print("\nProcess finished")
        

def create_dimesions():
    print("Droping Dimensions if already exists")
    drop_query = 'DROP TABLE IF EXISTS dim_fuel, dim_region, dim_flag, dim_uf, dim_city, measurment'
    cursor.execute(drop_query)
    
    print("Creating Dimensions")
    queries_array =[
        "CREATE TABLE IF NOT EXISTS dim_fuel (id_fuel SERIAL PRIMARY KEY, fuel_name VARCHAR)",
        "CREATE TABLE IF NOT EXISTS dim_region (id_region SERIAL PRIMARY KEY, region_name varchar) ",
        "CREATE TABLE IF NOT EXISTS dim_flag (id_flag SERIAL PRIMARY KEY, flag_type varchar)",
        "CREATE TABLE IF NOT EXISTS dim_uf (id_uf SERIAL PRIMARY KEY, uf_name varchar)",
        "CREATE TABLE IF NOT EXISTS dim_city (id_city SERIAL PRIMARY KEY, city_name varchar)",
        "CREATE TABLE IF NOT EXISTS measurment (id SERIAL PRIMARY KEY, price varchar, measurement_date DATE,gas_station_name varchar, cnpj_gas_station varchar,fuel_type_id INTEGER,uf_id INTEGER,city_id INTEGER,region_id INTEGER,FOREIGN KEY(fuel_type_id) REFERENCES dim_fuel(id_fuel),FOREIGN KEY(uf_id) REFERENCES dim_uf(id_uf), FOREIGN KEY(city_id) REFERENCES dim_city(id_city), FOREIGN KEY(region_id) REFERENCES dim_region(id_region))"
    ]
    regions = ["'SE'","'NE'","'CO'","'N'","'S'"]
    queries_array += list(map(lambda region: f"INSERT INTO dim_region(region_name) VALUES ({region});",regions))
    
    uf_list = ["'AC'", "'AL'", "'AP'", "'AM'", "'BA'", "'CE'", "'DF'", "'ES'", "'GO'", "'MA'", "'MT'", "'MS'", "'MG'", "'PA'", "'PB'", "'PR'", "'PE'", "'PI'", "'RJ'", "'RN'", "'RS'", "'RO'", "'RR'", "'SC'", "'SP'", "'SE'", "'TO'"]
    queries_array += list(map(lambda uf: f"INSERT INTO dim_uf(uf_name) VALUES ({uf});",uf_list))

    fuel_list = ["'GASOLINA'","'ETANOL'","'DIESEL'", "'GNV'"]
    queries_array += list(map(lambda fuel:f"INSERT INTO dim_fuel(fuel_name) VALUES ({fuel});",fuel_list))
    list(map(lambda query: cursor.execute(query), queries_array)) 
    try:
        conn.commit()
        print("Dimensions created")
    except Exception as error:
        print(f"Something got wrong: {error}")
        exit()
        
if __name__== "__main__":
    conn = get_connection()
    cursor = conn.cursor()
    create_dimesions()
    load_data()
    cursor.close()
    conn.close()