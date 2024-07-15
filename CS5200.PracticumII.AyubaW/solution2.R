# Load required libraries
library(DBI)
library(RSQLite)
library(XML)

# Connect to a new SQLite database
conn <- dbConnect(RSQLite::SQLite(), dbname = "pharma_sales.db")

# Create the Products table
dbExecute(conn, "
CREATE TABLE Products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
)")

# Create the Reps table
dbExecute(conn, "
CREATE TABLE Reps (
    rep_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    territory TEXT,
    commission REAL
)")

# Create the Customers table
dbExecute(conn, "
CREATE TABLE Customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    country TEXT NOT NULL
)")

# Create the Sales table
dbExecute(conn, "
CREATE TABLE Sales (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER,
    rep_id INTEGER,
    customer_id INTEGER,
    quantity INTEGER,
    sale_amount REAL,
    date TEXT,
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    FOREIGN KEY (rep_id) REFERENCES Reps(rep_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
)")


# List XML files in the directory
files <- list.files(pattern = "pharmaSalesTxn*.xml|pharmaReps*.xml")

# Function to parse and load XML data
load_xml_data <- function(file_name) {
  xml_data <- xmlParse(file_name)
  if (grepl("pharmaReps", file_name)) {
    # Transform rep data and load into Reps table
    reps_data <- xmlToDataFrame(nodes = getNodeSet(xml_data, "//rep"))
    dbWriteTable(conn, "Reps", reps_data, append = TRUE, row.names = FALSE)
  } else if (grepl("pharmaSalesTxn", file_name)) {
    # Transform transaction data and load into Sales table
    transactions <- xmlToDataFrame(nodes = getNodeSet(xml_data, "//txn"))
    # Convert date to a standard format if needed
    transactions$date <- as.Date(transactions$date, format = "%Y-%m-%d")
    dbWriteTable(conn, "Sales", transactions, append = TRUE, row.names = FALSE)
  }
}

# Process each file
sapply(files, load_xml_data)

# Close the database connection
dbDisconnect(conn)
