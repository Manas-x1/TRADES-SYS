import sys
from PyQt5 import QtWidgets, QtGui, QtCore
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from data_retrieval import DataRetrieval
from database import save_to_database

class StockApp(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Stock Trading System")
        self.setGeometry(100, 100, 1200, 800)
        
        self.dataRetrieval = None

        # Create UI components
        self.init_ui()

    def init_ui(self):
        # Layouts
        layout = QtWidgets.QVBoxLayout()
        form_layout = QtWidgets.QFormLayout()

        # Stock Symbol Input
        self.symbol_input = QtWidgets.QLineEdit()
        form_layout.addRow("Stock Symbol:", self.symbol_input)

        # Date Range Inputs
        self.start_date_input = QtWidgets.QDateEdit()
        self.start_date_input.setCalendarPopup(True)
        self.end_date_input = QtWidgets.QDateEdit()
        self.end_date_input.setCalendarPopup(True)
        form_layout.addRow("Start Date:", self.start_date_input)
        form_layout.addRow("End Date:", self.end_date_input)

        # Buttons
        self.fetch_data_btn = QtWidgets.QPushButton("Fetch Data")
        self.fetch_data_btn.clicked.connect(self.fetch_data)

        self.start_update_btn = QtWidgets.QPushButton("Start Live Updates")
        self.start_update_btn.clicked.connect(self.start_live_updates)

        # Data Table
        self.table_view = QtWidgets.QTableView()

        # Graph
        self.graph_canvas = FigureCanvas(plt.figure(figsize=(10, 6)))

        # Layout setup
        layout.addLayout(form_layout)
        layout.addWidget(self.fetch_data_btn)
        layout.addWidget(self.start_update_btn)
        layout.addWidget(self.table_view)
        layout.addWidget(self.graph_canvas)
        self.setLayout(layout)

    def fetch_data(self):
        """Fetch and display stock data."""
        symbol = self.symbol_input.text()
        start_date = self.start_date_input.date().toString("yyyy-MM-dd")
        end_date = self.end_date_input.date().toString("yyyy-MM-dd")

        if symbol:
            self.dataRetrieval = DataRetrieval(symbol)
            data = self.dataRetrieval.fetch_historical_data(start_date, end_date)

            if data is not None:
                self.display_data_in_table(data)
                self.plot_data(data)

    def start_live_updates(self):
        """Start live data updates."""
        update_interval = 60  # Update every minute
        if self.dataRetrieval:
            self.dataRetrieval.start_periodic_update(update_interval)

    def display_data_in_table(self, data):
        """Display stock data in the table."""
        model = QtWidgets.QStandardItemModel(self.table_view)
        model.setHorizontalHeaderLabels(data.columns)
        for row in data.values:
            items = [QtWidgets.QStandardItem(str(val)) for val in row]
            model.appendRow(items)
        self.table_view.setModel(model)

    def plot_data(self, data):
        """Plot stock data on the graph."""
        plt.clf()
        ax = self.graph_canvas.figure.add_subplot(111)
        ax.plot(data["Datetime"], data["Close"], label="Close Price")
        ax.set_title(f"Stock Data: {self.dataRetrieval.symbol}")
        ax.set_xlabel("Date")
        ax.set_ylabel("Close Price")
        ax.grid(True)
        self.graph_canvas.draw()

# Main entry point
if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = StockApp()
    window.show()
    sys.exit(app.exec_())
