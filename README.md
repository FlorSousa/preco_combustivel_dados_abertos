# Data Analysis about the fuel price in Brazil

##### Author: Jhonatas Flor

## Idea
The present project is a simple presentation about how the prices of fuel in Brazil have changed since the first measurement in 2004.

In this repo you will find:
- The average price of gasoline, diesel, ethanol and GNC, since the first measurement
- The evolution of gasoline, diesel, ethanol, and natural gas prices from 2004 to 2023
- The brazilian state with the most expensive and cheapest gasoline price

## How to run

### What do you need

- Docker
- Docker-compose
- Jupyter notebook

### Preparing the environment

The .env.example is a template for a .env file. Use the following command to create a .env file and change the configurations to fit your needs

``` 
   cp .env.example .env
```

If you're using Docker, DB_HOST should be set to pg_database (the name of the PostgreSQL container)

### Running in docker

Use the docker-compose command to build and run the containers

``` 
   docker-compose up
```

The ***scripts*** container executes the files download_script.py and create_dimensions.py. After execution, it will be stopped

### Running in native SO | Windows example

``` 
   python download_script.py
```
and
``` 
   python create_dimensions.py
```

### Running jupyter notebook

***If you are using Docker: Check the port for pg_database and use it as the value for DB_PORT in your .env file. After running docker-compose, use 'localhost' as the value for DB_HOST.***

``` 
   jupyter lab
```