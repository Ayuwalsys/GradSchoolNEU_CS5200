# Script Metadata
# Author: Waliu Ayuba
# Course: CS5200 - Database Management Systems
# Date: 2024-04-17
# Description: Script to parse XML data from pharmaceutical sales and reps, and load into an SQLite database.


library(DBI)
library(RSQLite)
library(xml2)
library(dplyr)

# Specify the database path
db_path <- "pharma_sales2.db"

# Check if the directory exists, if not create it
dir.create(dirname(db_path), recursive = TRUE, showWarnings = FALSE)

# Create or open the SQLite database
con <- dbConnect(RSQLite::SQLite(), dbname = db_path)

# Function to safely execute SQL with table existence check
safeExecute <- function(con, query) {
  tryCatch({
    dbExecute(con, query)
    TRUE
  }, error = function(e) {
    message("Error executing SQL: ", e$message)
    FALSE
  })
}

# Create the Products table if not exists
safeExecute(con, "
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    unit_price REAL
)")

# Create the Reps table if not exists
safeExecute(con, "
CREATE TABLE IF NOT EXISTS reps (
    rep_id TEXT PRIMARY KEY,
    rep_name TEXT NOT NULL,
    territory TEXT,
    commission REAL
)")

# Create the Customers table if not exists
safeExecute(con, "
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT NOT NULL,
    country TEXT NOT NULL
)")

# Create the Sales table if not exists
safeExecute(con, "
CREATE TABLE IF NOT EXISTS sales (
    sale_id TEXT PRIMARY KEY,
    product_id INTEGER,
    rep_id TEXT,
    customer_id INTEGER,
    sale_date DATE,
    quantity INTEGER,
    total_amount REAL,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (rep_id) REFERENCES reps(rep_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
)")

# Function to process XML files and extract data
process_files <- function(relative_FolderPath) {
  # Construct full path from the relative path
  folder <- file.path(getwd(), relative_FolderPath)
  
  # List XML files in the directory
  files <- list.files(folder, pattern = "\\.xml$", full.names = TRUE)
  
  for (file in files) {
    if (grepl("pharmaReps", file)) {
      load_reps_data(file)
    } else if (grepl("pharmaSalesTxn", file)) {
      load_transaction_data(file)
    }
  }
}


# Load reps data with check for duplicates
load_reps_data <- function(file) {
  xml_data <- read_xml(file)
  reps <- xml_find_all(xml_data, "//rep")
  data_frame <- tibble(
    rep_id = xml_attr(reps, "rID"),
    rep_name = paste(xml_text(xml_find_all(reps, ".//name/first")), xml_text(xml_find_all(reps, ".//name/sur")), sep = " "),
    territory = xml_text(xml_find_all(reps, ".//territory")),
    commission = as.numeric(xml_text(xml_find_all(reps, ".//commission")))
  )
  
  # Check if rep_id already exists in the database
  existing_reps <- dbReadTable(con, "reps")
  
  for (row in 1:nrow(data_frame)) {
    if (data_frame$rep_id[row] %in% existing_reps$rep_id) {
      # Update existing record
      query <- sprintf("UPDATE reps SET rep_name = '%s', territory = '%s', commission = %f WHERE rep_id = '%s'",
                       data_frame$rep_name[row], data_frame$territory[row], data_frame$commission[row], data_frame$rep_id[row])
      dbExecute(con, query)
    } else {
      # Insert new record
      query <- sprintf("INSERT INTO reps (rep_id, rep_name, territory, commission) VALUES ('%s', '%s', '%s', %f)",
                       data_frame$rep_id[row], data_frame$rep_name[row], data_frame$territory[row], data_frame$commission[row])
      dbExecute(con, query)
    }
  }
}

#Function to Load Transaction_data
load_transaction_data <- function(file) {
  xml_data <- read_xml(file)
  transactions <- xml_find_all(xml_data, "//txn")
  
  for (txn in transactions) {
    sale_id <- xml_attr(txn, "txnID")
    
    # Check if sale_id already exists to avoid UNIQUE constraint error
    existing_ids <- dbGetQuery(con, "SELECT sale_id FROM sales WHERE sale_id = ?", list(sale_id))
    
    if (nrow(existing_ids) == 0) {  # Proceed only if sale_id does not exist
      rep_id <- xml_attr(txn, "repID")
      customer_name <- xml_text(xml_find_first(txn, ".//customer"))  
      country <- xml_text(xml_find_first(txn, ".//country")) 
      sale_date_str <- xml_text(xml_find_first(txn, ".//date"))
      
      # Attempt to parse date using different formats
      possible_formats <- c("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d")
      sale_date <- NA
      for (format in possible_formats) {
        try_date <- try(as.Date(sale_date_str, format))
        if (!inherits(try_date, "try-error")) {
          sale_date <- try_date
          break
        }
      }
      
      if (is.na(sale_date)) {
        cat("Failed to parse date:", sale_date_str, "\n")
        next  # Skip this transaction if date parsing fails
      }
      
      product_name <- xml_text(xml_find_first(txn, ".//product")) 
      quantity <- as.integer(xml_text(xml_find_first(txn, ".//qty"))) 
      total_amount <- as.numeric(xml_text(xml_find_first(txn, ".//total")))
      
      cat("Sale ID:", sale_id, "\n")
      cat("Rep ID:", rep_id, "\n")
      cat("Customer Name:", customer_name, "\n")
      cat("Country:", country, "\n")
      cat("Sale Date:", sale_date, "\n")
      cat("Product Name:", product_name, "\n")
      cat("Quantity:", quantity, "\n")
      cat("Total Amount:", total_amount, "\n")
      
      # Check if product exists, if not insert into products table
      product_id <- get_or_insert_product(product_name)
      
      # Check if customer exists, if not insert into customers table
      customer_id <- get_or_insert_customer(customer_name, country)
      
      dbExecute(con, "INSERT INTO sales (sale_id, product_id, rep_id, customer_id, sale_date, quantity, total_amount) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                list(sale_id, product_id, rep_id, customer_id, as.character(sale_date), quantity, total_amount))
    } else {
      cat("Skipping duplicate Sale ID:", sale_id, "\n")
    }
  }
}


get_or_insert_product <- function(product_name) {
  product_id <- dbGetQuery(con, "SELECT product_id FROM products WHERE product_name = ?", list(product_name))$product_id
  if (is.null(product_id) || length(product_id) != 1) {
    dbExecute(con, "INSERT INTO products (product_name) VALUES (?)", list(product_name))
    product_id <- dbGetQuery(con, "SELECT last_insert_rowid() AS product_id")$product_id
  }
  print(paste("Product ID:", product_id))
  return(product_id)
}

get_or_insert_customer <- function(customer_name, country) {
  customer_id <- dbGetQuery(con, "SELECT customer_id FROM customers WHERE customer_name = ? AND country = ?", list(customer_name, country))$customer_id
  if (is.null(customer_id) || length(customer_id) != 1) {
    dbExecute(con, "INSERT INTO customers (customer_name, country) VALUES (?, ?)", list(customer_name, country))
    customer_id <- dbGetQuery(con, "SELECT last_insert_rowid() AS customer_id")$customer_id
  }
  print(paste("Customer ID:", customer_id))
  return(customer_id)
}


# Process the XML files
process_files("txn-xml")

# Close the connection to the database
dbDisconnect(con)

cat("Data successfully imported into the database.")

