#this is the data retrieval.py file
import yfinance as yf
import pandas as pd
import os
import time
from Database import save_to_database  # Ensure this is imported

class DataRetrieval:
    def __init__(self, symbol, interval="1m", csv_file="data.csv"):
        """
        Initializes the DataRetrieval class.

        Args:
            symbol (str): Stock ticker symbol (e.g., "AAPL").
            interval (str): Data retrieval interval (e.g., "1d", "1m", "1h").
            csv_file (str): Path to the CSV file where data will be saved.
        """
        self.symbol = symbol
        self.interval = interval
        self.csv_file = csv_file
        self.ticker = yf.Ticker(symbol)

    def _save_to_csv(self, data):
        """
        Save data to the CSV file, ensuring no duplicates.

        Args:
            data (pd.DataFrame): DataFrame containing the stock data.
        """
        if os.path.exists(self.csv_file):
            # Load existing data
            existing_data = pd.read_csv(self.csv_file)
            # Concatenate and drop duplicates
            updated_data = pd.concat([existing_data, data]).drop_duplicates(subset=["Datetime", "Symbol"])
        else:
            # No existing file, just use the new data
            updated_data = data

        # Save to CSV
        updated_data.to_csv(self.csv_file, index=False)
        print(f"Data saved to {self.csv_file}.")

    def _save_to_database(self, data):
        """
        Save data to the database.

        Args:
            data (pd.DataFrame): DataFrame containing the stock data.
        """
        try:
            save_to_database(data)  # Call the database module's save function
            print("Data saved to the database.")
        except Exception as e:
            print(f"Error saving data to the database: {e}")

    def fetch_historical_data(self, start_date, end_date):
        """
        Fetch historical data for the stock symbol.

        Args:
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: DataFrame containing historical stock data.
        """
        try:
            data = self.ticker.history(start=start_date, end=end_date, interval=self.interval)
            if data.empty:
                print(f"No historical data found for {self.symbol} between {start_date} and {end_date}.")
                return None

            # Prepare data for saving
            data.reset_index(inplace=True)
            data.rename(columns={"index": "Datetime"}, inplace=True)
            data["Symbol"] = self.symbol

            # Convert Datetime to MySQL-compatible string format
            data["Datetime"] = data["Datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Save to CSV and Database
            self._save_to_csv(data)
            self._save_to_database(data)

            print(f"Fetched historical data for {self.symbol} from {start_date} to {end_date}")
            return data
        except Exception as e:
            print(f"Error fetching historical data: {e}")
            return None

    def fetch_live_data(self):
        """
        Fetch the latest live data for the stock symbol.

        Returns:
            pd.DataFrame: DataFrame containing the latest stock data.
        """
        try:
            live_data = self.ticker.history(period="1d", interval=self.interval).tail(1)
            if live_data.empty:
                print(f"No live data found for {self.symbol}.")
                return None

            # Prepare data for saving
            live_data.reset_index(inplace=True)
            live_data.rename(columns={"index": "Datetime"}, inplace=True)
            live_data["Symbol"] = self.symbol

            # Convert Datetime to MySQL-compatible string format
            live_data["Datetime"] = live_data["Datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Save to CSV and Database
            self._save_to_csv(live_data)
            self._save_to_database(live_data)

            print(f"Fetched live data for {self.symbol}")
            return live_data
        except Exception as e:
            print(f"Error fetching live data: {e}")
            return None

    def start_periodic_update(self, update_interval):
        """
        Start periodic data updates.

        Args:
            update_interval (int): Update interval in seconds.
        """
        print(f"Starting periodic updates for {self.symbol} every {update_interval} seconds.")
        try:
            while True:
                self.fetch_live_data()
                time.sleep(update_interval)
        except KeyboardInterrupt:
            print(f"Stopped periodic updates for {self.symbol}.")
        except Exception as e:
            print(f"Error during periodic updates: {e}")


# Example usage
#if __name__ == "__main__":
    # Example: Fetching historical data
    #dr = DataRetrieval("TSLA", interval="1m", csv_file="stock_data.csv")
    #dr.fetch_historical_data("2024-11-20", None)

    # Example: Starting live updates every 60 seconds
    # Uncomment the next line to test live updates.
   # dr.start_periodic_update(60)
