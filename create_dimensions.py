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
    return re.sub(re.compile(r"'"),"",string)

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
    
def load_gas_station(dataframe):
    df_gas_list = dataframe['Revenda'].unique().astype(object)
    df_gas_list = df_gas_list[~pd.isnull(df_gas_list)]
    gas_str = ",".join([f"'{sanitizer_str(gas_station)}'" for gas_station in df_gas_list])
    cursor.execute(f"SELECT * FROM public.dim_city WHERE city_name IN ({gas_str})")
    gas_db = [gas_station_name[1] for gas_station_name in cursor.fetchall()]
    station_insert = filter(lambda f: True if sanitizer_str(f) not in gas_db else False, df_gas_list)
    queries = list(map(lambda station: f"INSERT INTO dim_gas_station(gas_station_name) VALUES ('{sanitizer_str(station)}');", station_insert))
    [cursor.execute(q) for q in queries]

def load_data():
    columns_to_remove = ["Numero Rua","Nome da Rua","Complemento","Bairro","Cep","Valor de Compra","Unidade de Medida","CNPJ da Revenda"]
    years = [int(year) for year in os.listdir("csv_data")]
    for year in years:
        num_semester = len(os.listdir(f"csv_data/{year}"))
        for semester in range(1,num_semester+1):
            chunks = pd.read_csv(f"csv_data/{year}/preco_combustivel_{year}_{semester}.csv", chunksize=10000, delimiter=";")
            chunks = map(lambda chunk: chunk.drop(columns_to_remove, axis=1), chunks)
            cleaned_df = pd.concat(list(chunks))
            load_dim_flag(cleaned_df)
            load_dim_city(cleaned_df)
            load_gas_station(cleaned_df)

    

def create_dimesions():
    queries_array =[
        "CREATE TABLE IF NOT EXISTS dim_fuel (id_fuel SERIAL PRIMARY KEY, fuel_name VARCHAR)",
        "CREATE TABLE IF NOT EXISTS dim_region (id_region SERIAL PRIMARY KEY, region_name varchar) ",
        "CREATE TABLE IF NOT EXISTS dim_flag (id_flag SERIAL PRIMARY KEY, flag_type varchar)",
        "CREATE TABLE IF NOT EXISTS dim_uf (id_uf SERIAL PRIMARY KEY, uf_name varchar)",
        "CREATE TABLE IF NOT EXISTS dim_city (id_city SERIAL PRIMARY KEY, city_name varchar)",
        "CREATE TABLE IF NOT EXISTS dim_gas_station (id_gas_station SERIAL PRIMARY KEY, gas_station_name varchar)",
        "CREATE TABLE IF NOT EXISTS measurment (id SERIAL PRIMARY KEY, price varchar, measurement_date DATE,gas_station_id INTEGER,fuel_type_id INTEGER,uf_id INTEGER,city_id INTEGER,region_id INTEGER,FOREIGN KEY(gas_station_id) REFERENCES dim_gas_station(id_gas_station),FOREIGN KEY(fuel_type_id) REFERENCES dim_fuel(id_fuel),FOREIGN KEY(uf_id) REFERENCES dim_uf(id_uf), FOREIGN KEY(city_id) REFERENCES dim_city(id_city), FOREIGN KEY(region_id) REFERENCES dim_region(id_region))"
    ]
    regions = ["'SE'","'NE'","'CO'","'N'","'S'"]
    queries_array += list(map(lambda region: f"INSERT INTO dim_region(region_name) VALUES ({region});",regions))
    
    uf_list = ["'AC'", "'AL'", "'AP'", "'AM'", "'BA'", "'CE'", "'DF'", "'ES'", "'GO'", "'MA'", "'MT'", "'MS'", "'MG'", "'PA'", "'PB'", "'PR'", "'PE'", "'PI'", "'RJ'", "'RN'", "'RS'", "'RO'", "'RR'", "'SC'", "'SP'", "'SE'", "'TO'"]
    queries_array += list(map(lambda uf: f"INSERT INTO dim_uf(uf_name) VALUES ({uf});",uf_list))

    fuel_list = ["'gasolina'","'etanol'","'diesel'"]
    queries_array += list(map(lambda fuel:f"INSERT INTO dim_fuel(fuel_name) VALUES ({fuel});",fuel_list))
    list(map(lambda query: cursor.execute(query), queries_array)) 
  
    
if __name__== "__main__":
    conn = get_connection()
    cursor = conn.cursor()
    create_dimesions()
    load_data()
    conn.commit()
    cursor.close()
    conn.close()