#this is the database.py file
import mysql.connector
import pandas as pd
from mysql.connector import Error


def save_to_database(data, host="localhost", user="root", password="manasx1", database="trading_data", table_name="stocks"):
    """
    Save a DataFrame to a MySQL database table.

    Args:
        data (pd.DataFrame): DataFrame to be saved.
        host (str): MySQL host address.
        user (str): MySQL username.
        password (str): MySQL password.
        database (str): Database name.
        table_name (str): Table name.
    """
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            print(f"Connected to MySQL database '{database}'.")

            cursor = connection.cursor()

            # Create table if it does not exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                Datetime DATETIME NOT NULL,
                Open FLOAT,
                High FLOAT,
                Low FLOAT,
                Close FLOAT,
                Volume BIGINT,
                Dividends FLOAT,
                StockSplits FLOAT,
                Symbol VARCHAR(10),
                PRIMARY KEY (Datetime, Symbol)
            );
            """
            cursor.execute(create_table_query)

            # Convert the 'Datetime' column to MySQL-compatible format
            data["Datetime"] = pd.to_datetime(data["Datetime"]).dt.strftime('%Y-%m-%d %H:%M:%S')


            # Insert data into the table
            insert_query = f"""
            INSERT INTO {table_name} (Datetime, Open, High, Low, Close, Volume, Dividends, StockSplits, Symbol)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Open = VALUES(Open),
                High = VALUES(High),
                Low = VALUES(Low),
                Close = VALUES(Close),
                Volume = VALUES(Volume),
                Dividends = VALUES(Dividends),
                StockSplits = VALUES(StockSplits);
            """

            # Iterate over the DataFrame rows
            for _, row in data.iterrows():
                cursor.execute(insert_query, (
                    row["Datetime"],
                    row["Open"],
                    row["High"],
                    row["Low"],
                    row["Close"],
                    row["Volume"],
                    row["Dividends"],
                    row["Stock Splits"],
                    row["Symbol"]
                ))

            # Commit changes
            connection.commit()
            print("Data successfully saved to the MySQL database.")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")
