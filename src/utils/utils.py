import pandas as pd
import pymongo
import sqlite3

# Function to read data from a CSV file
def read_csv(file_path, file_sep):
    return pd.read_csv(file_path, sep=file_sep), str(f'{file_path}')

def read_parquet(file_path):
    return pd.read_parquet(file_path), str(f'{file_path}')

# Function to read data from a SQL database
def read_sql(host, port, username, password, database, table):
    conn = sqlite3.connect(f"sqlite://{host}:{port}/{database}")
    query = f"SELECT * FROM {table}"
    return pd.read_sql(query, conn), str(f'sqlite3.connect(f"sqlite://{host}:{port}/{database}")')

# Function to read data from a MongoDB database
def read_mongo(host, port, database, collection):
    client = pymongo.MongoClient(host, port)
    db = client[database]
    cursor = db[collection].find()
    return pd.DataFrame(list(cursor)), str(f'pymongo.MongoClient({host}, {port})')

def read_parquet(parquet_path):
    return pd.read_parquet(parquet_path), str(f'{parquet_path}')

# Function to register expectations in MongoDB
def register_expectations_mongo(host, port, database, collection, expectations):
    print(host)
    client = pymongo.MongoClient(host, port)
    db = client[database]
    db[collection].insert_one(expectations)
    