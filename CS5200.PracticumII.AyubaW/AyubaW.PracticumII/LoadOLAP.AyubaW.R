# Script Metadata
# Author: Waliu Ayuba
# Course: CS5200 - Database Management Systems
# Date: 2024-04-17
# Description: Script to parse XML data from pharmaceutical sales and reps, and load into an SQLite database.

library(RMySQL)
library(DBI)
library(RSQLite)  # Make sure to load this package for SQLite operations

# Connect to MySQL Database
Mysqlcon <- dbConnect(RMySQL::MySQL(),
                      host = "sql5.freemysqlhosting.net",
                      dbname = "sql5699606",
                      username = "sql5699606",
                      password = "GY1IlvutbB",
                      port = 3306)

# Check MySQL connection
if (is.null(Mysqlcon)) stop("Failed to connect to MySQL database")

# Create product_facts tables in MySQL Database
dbExecute(Mysqlcon, "
CREATE TABLE IF NOT EXISTS product_facts (
    product_name VARCHAR(255),
    year INT,
    quarter INT,
    total_amount_sold DECIMAL(10, 2),
    total_units_sold INT,
    region VARCHAR(255)
)")

#Create rep_facts in MySQL Database
dbExecute(Mysqlcon, "
CREATE TABLE IF NOT EXISTS rep_facts (
    rep_name VARCHAR(255),
    year INT,
    quarter INT,
    total_amount_sold DECIMAL(10, 2),
    average_amount_sold DECIMAL(10, 2)
)")

# Connect to the SQLite database
con <- dbConnect(RSQLite::SQLite(), dbname = "pharma_sales2.db")
if (is.null(con)) stop("Failed to connect to SQLite database")

# Extract and Load Product Facts
product_query <- "
SELECT 
    p.product_name,
    strftime('%Y', s.sale_date) AS year,
    (CAST(strftime('%m', s.sale_date) AS INTEGER) + 2) / 3 AS quarter,
    c.country AS region,
    SUM(s.total_amount) AS total_amount_sold,
    SUM(s.quantity) AS total_units_sold
FROM
    sales s
JOIN
    products p ON s.product_id = p.product_id
JOIN
    customers c ON s.customer_id = c.customer_id
GROUP BY
    p.product_name, year, quarter, c.country
"

product_data <- dbGetQuery(con, product_query)
if (nrow(product_data) == 0) {
  cat("No product data to load\n")
} else {
  dbWriteTable(Mysqlcon, "product_facts", product_data, append = TRUE, row.names = FALSE)
}

# Connect to the SQLite database
con <- dbConnect(RSQLite::SQLite(), dbname = "pharma_sales2.db")
if (is.null(con)) stop("Failed to connect to SQLite database")

# query for rep_id format difference
rep_query <- "
SELECT
    r.rep_name,
    strftime('%Y', s.sale_date) AS year,
    (CAST(strftime('%m', s.sale_date) AS INTEGER) + 2) / 3 AS quarter,
    SUM(s.total_amount) AS total_amount_sold,
    AVG(s.total_amount) AS average_amount_sold
FROM
    sales s
JOIN
    reps r ON s.rep_id = CAST(substr(r.rep_id, 2) AS INTEGER)
GROUP BY
    r.rep_name, year, quarter
"

rep_data <- dbGetQuery(con, rep_query)

# Check if data was retrieved and load it
if (nrow(rep_data) == 0) {
  cat("No rep data to load\n")
} else {
  cat("Data retrieved successfully. Number of rows: ", nrow(rep_data), "\n")
  dbWriteTable(Mysqlcon, "rep_facts", rep_data, append = TRUE, row.names = FALSE)
}

# Close database connections
dbDisconnect(con)
dbDisconnect(Mysqlcon)

