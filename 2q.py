# Import required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import sqlite3

# Define known values
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
csv_path = "./Largest_banks_data.csv"
log_file = "code_log.txt"

# Exchange rate CSV (must be in the same directory)
exchange_rate_csv = "./exchange_rate.csv"


def log_progress(message):
    """Logs progress messages with timestamp to code_log.txt"""
    timestamp_format = "%Y-%m-%d-%H:%M:%S"  # FIXED
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + " : " + message + "\n")


def extract(url, table_attribs):
    """Extracts bank data from Wikipedia archive page"""
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")

    df = pd.DataFrame(columns=table_attribs)

    # FIX: safer table selection
    tables = soup.find_all("table")
    rows = tables[0].find_all("tr")

    for row in rows:
        col = row.find_all("td")

        # FIX: ensure correct number of columns
        if len(col) >= 3:
            if col[1].find("a") is not None:
                try:
                    name = col[1].find("a").text.strip()

                    # FIX: clean numeric values
                    value = col[2].text.strip().replace(",", "")
                    value = value.split("[")[0]

                    data_dict = {
                        "Name": name,
                        "MC_USD_Billion": float(value)
                    }

                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)

                except:
                    continue

    return df


def transform(df, csv_path):
    """Transforms USD market cap to GBP, EUR, and INR using exchange rates"""
    exchange_rate = pd.read_csv(csv_path, index_col=0).to_dict()["Rate"]

    df["MC_GBP_Billion"] = [round(x * exchange_rate["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [round(x * exchange_rate["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [round(x * exchange_rate["INR"], 2) for x in df["MC_USD_Billion"]]

    return df


def load_to_csv(df, output_path):
    """Saves the dataframe to a CSV file"""
    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    """Loads the dataframe to a SQLite database table"""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    """Runs a SQL query and prints the result"""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


# ─────────────────────────────────────────────────────────────
# MAIN ETL PROCESS
# ─────────────────────────────────────────────────────────────

log_progress("Preliminaries complete. Initiating ETL process")

# Task 2: Extract
print("\n--- Task 2: Extract ---")
df = extract(url, table_attribs)
print(df)
log_progress("Data extraction complete. Initiating Transformation process")

# Task 3: Transform
print("\n--- Task 3: Transform ---")
df = transform(df, exchange_rate_csv)
print(df)
log_progress("Data transformation complete. Initiating Loading process")

# Task 4: Load to CSV
load_to_csv(df, csv_path)
log_progress("Data saved to CSV file")

# Task 5: Load to Database
sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table. Executing queries")

# Task 6: Run Queries
print("\n--- Task 6: Queries ---")
run_query(f"SELECT * FROM {table_name}", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}", sql_connection)
run_query(f"SELECT Name FROM {table_name} LIMIT 5", sql_connection)

log_progress("Process Complete")

sql_connection.close()
log_progress("Server Connection closed")

print("\nETL Process Complete! Check code_log.txt for logs.")
