import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import sqlite3

# Known constants
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"

exchange_rate_csv = "./exchange_rate.csv"

def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + " : " + message + "\n")

def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all("tbody")
    rows = tables[0].find_all("tr")
    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            if col[1].find("a") is not None:
                data_dict = {
                    "Name": col[1].find("a").contents[0],
                    "MC_USD_Billion": float(col[2].contents[0].strip())
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    exchange_rate = pd.read_csv(csv_path, index_col=0).to_dict()["Rate"]
    df["MC_GBP_Billion"] = [round(x * exchange_rate["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [round(x * exchange_rate["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [round(x * exchange_rate["INR"], 2) for x in df["MC_USD_Billion"]]
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)