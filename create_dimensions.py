import psycopg2
import numpy as np

def get_connection():
    from dotenv import load_dotenv
    import os
    load_dotenv()
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def create_dimesions():
    
    queries_array = np.array([
        "CREATE TABLE IF NOT EXISTS dim_fuel (id_fuel INTEGER PRIMARY KEY, fuel_name varchar)",
        "CREATE TABLE IF NOT EXISTS dim_region (id_region INTEGER PRIMARY KEY, region_name varchar) ",
        "CREATE TABLE IF NOT EXISTS dim_flag (id_flag INTEGER PRIMARY KEY, flag_type varchar)",
        "CREATE TABLE IF NOT EXISTS dim_uf (id_uf INTEGER PRIMARY KEY, uf_name varchar,  region_id INTEGER, FOREIGN KEY(region_id) REFERENCES dim_region(id_region))",
        "CREATE TABLE IF NOT EXISTS dim_city (id_city INTEGER PRIMARY KEY, city_name varchar, uf_id INTEGER, FOREIGN KEY(uf_id) REFERENCES dim_uf(id_uf))",
        "CREATE TABLE IF NOT EXISTS dim_gas_station (id_gas_station INTEGER PRIMARY KEY, gas_station_name varchar, city_id INTEGER, flag_id INTEGER, FOREIGN KEY(city_id) REFERENCES dim_city(id_city), FOREIGN KEY(flag_id) REFERENCES dim_flag(id_flag))",
        "CREATE TABLE IF NOT EXISTS measurment (id INTEGER PRIMARY KEY, price varchar, measurement_date DATE, gas_station_id INTEGER, fuel_type_id INTEGER,FOREIGN KEY(gas_station_id) REFERENCES dim_gas_station(id_gas_station),FOREIGN KEY(fuel_type_id) REFERENCES dim_fuel(id_fuel) )"
    ])
    
    [cursor.execute(str(query)) for query in np.nditer(queries_array)]


if __name__== "__main__":
    conn = get_connection()
    cursor = conn.cursor()
    create_dimesions()
    conn.commit()
    cursor.close()
    conn.close()