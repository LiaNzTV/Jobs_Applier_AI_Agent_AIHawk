log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, ["Name", "MC_USD_Billion"])
log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, exchange_rate_csv)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, csv_path)
log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table. Executing queries")

# Task 3 queries
run_query(f"SELECT * FROM {table_name}", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}", sql_connection)
run_query(f"SELECT Name FROM {table_name} LIMIT 5", sql_connection)

log_progress("Process Complete")

sql_connection.close()
log_progress("Server Connection closed")