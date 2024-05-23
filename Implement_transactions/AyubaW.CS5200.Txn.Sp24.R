library(DBI)
library(RMySQL)

library(readr)
library(dplyr)
library(lubridate)

# Assuming the script with the function is named 'my_functions.R'
source("helper_functions.R")


#data base credential
connection <- dbConnect(RMySQL::MySQL(),
                        dbname = "sql5694554",
                        host = "sql5.freemysqlhosting.net",
                        port = 3306,
                        user = "sql5694554",
                        password = "uldtUsSbKn")





# Load the bird strikes data
bds_raw <- read_csv("BirdStrikesData-V3.csv")


# Helper function to generate a random date
random_date <- function() {
  start_date <- as.Date("2021-01-01")
  end_date <- as.Date("2024-01-01")
  random_number_of_days <- sample.int(as.integer(end_date - start_date), 1)
  return(format(start_date + days(random_number_of_days), "%m/%d/%Y"))
}

# Select five existing airports from the loaded data
existing_airports <- unique(bds_raw$airport)
selected_existing_airports <- sample(existing_airports, 5)

# Initialized two new airports
new_airports <- c("New Airport 1", "New Airport 2")

# Function to generate a single record
generate_record <- function(airport, origin) {
  data.frame(
    rid = sample(100000:999999, 1),
    aircraft = "Airplane",
    airport = airport,
    model = "B-737-800",
    impact = "None",
    flight_date = random_date(),
    damage = sample(c("Damage", "No damage"), 1),
    airline = "Sample Airline",
    origin = origin,
    flight_phase = "Climb",
    wildlife_size = sample(c("Small", "Medium", "Large"), 1),
    sky_conditions = sample(c("No Cloud", "Some Cloud", "Overcast"), 1),
    pilot_warned_flag = sample(c("Y", "N"), 1),
    altitude_ft = as.character(sample(0:10000, 1)),
    heavy_flag = sample(c("Yes", "No"), 1),
    stringsAsFactors = FALSE
  )
}

# Generate data for the new CSV_files
for (i in 1:5) {
  data <- data.frame()
  
  # Existing airports
  for (j in 1:5) {
    data <- rbind(data, generate_record(sample(selected_existing_airports, 1), "Sample State"))
  }
  
  # New airports
  for (j in 1:2) {
    data <- rbind(data, generate_record(sample(new_airports, 1), "New State"))
  }
  
  # Add 3 more records to make it 10 per file
  for (j in 1:3) {
    data <- rbind(data, generate_record(sample(existing_airports, 1), "Sample State"))
  }
  
  # Save the data for each CSV file
  write_csv(data, paste0("BirdStrikesData-New", i, ".csv"))
}


# Function to read and process new data from CSV
processNewData <- function(csvFilePath) {
  new_data <- read_csv(csvFilePath)
  
  # Process the data according to the schema from Practicum I
  airports_data <- prepareAirportsData(new_data)
 
  flights_data <- prepareFlightsData(new_data, airports_data)
 
  conditions_data <- prepareConditionsData(new_data)

  strikes_data <- prepareStrikesData(new_data, flights_data, conditions_data)
  
  
  return(list(airports_data = airports_data, flights_data = flights_data, conditions_data = conditions_data, strikes_data = strikes_data))
}



loadDataAndInsert <- function(csvFilePath, useTransactions) {
  if (tolower(useTransactions) == "true") {
    dbBegin(connection)
  }
  
  tryCatch({
    # Load CSV data
    bird_strikes_data <- read_csv(csvFilePath)
    
    # Used helper functions to preprocess data 
    airports_data <- prepareAirportsData(bird_strikes_data)
    flights_data <- prepareFlightsData(bird_strikes_data, airports_data)
    conditions_data <- prepareConditionsData(bird_strikes_data)
    strikes_data <- prepareStrikesData(bird_strikes_data, flights_data, conditions_data)
    
    # Insert data with delay
    dbWriteTable(connection, 'airports', airports_data, append = TRUE, row.names = FALSE)
    Sys.sleep(1)  # 1 second delay
    dbWriteTable(connection, 'flights', flights_data, append = TRUE, row.names = FALSE)
    Sys.sleep(1)  # 1 second delay
    dbWriteTable(connection, 'conditions', conditions_data, append = TRUE, row.names = FALSE)
    Sys.sleep(1)  # 1 second delay
    dbWriteTable(connection, 'strikes', strikes_data, append = TRUE, row.names = FALSE)
    Sys.sleep(1)  # 1 second delay
    
    if (tolower(useTransactions) == "true") {
      dbCommit(connection)
    }
  }, error = function(e) {
    if (tolower(useTransactions) == "true") {
      dbRollback(connection)
    }
    message("Error in transaction: ", e$message)
  })
}


removeCSV_Data <- function(csvFilePath, connection) {
  # if CSV has unique identifiers like 'rid' which can be used to delete data
  new_data <- read_csv(csvFilePath)
  rid_values <- new_data$rid
  
  # transaction start
  dbBegin(connection)
  
  tryCatch({
    # Data deletion from each table using the unique identifiers
    # there is need to ensure the deletions happen in the correct order to respect foreign key constraints
    dbExecute(connection, sprintf("DELETE FROM strikes WHERE fid IN (%s)", paste(rid_values, collapse = ",")))
    dbExecute(connection, sprintf("DELETE FROM flights WHERE fid IN (%s)", paste(rid_values, collapse = ",")))
    # ... similar deletions for other tables like airports, conditions
    airport_ids <- unique(new_data$origin)
    condition_ids <- unique(new_data$sky_conditions)
    
    if ("originAirport" %in% names(new_data)) {
      airport_ids <- unique(new_data$originAirport)
      # make sure airport_ids are integers
      if (all(sapply(airport_ids, is.numeric)) && length(airport_ids) > 0) {
        dbExecute(connection, sprintf("DELETE FROM airports WHERE aid IN (%s)", paste(airport_ids, collapse = ",")))
      }
    }
    
    if ("conditions" %in% names(new_data)) {
      condition_ids <- unique(new_data$conditions)
      # make sure condition_ids are integers
      if (all(sapply(condition_ids, is.numeric)) && length(condition_ids) > 0) {
        dbExecute(connection, sprintf("DELETE FROM conditions WHERE cid IN (%s)", paste(condition_ids, collapse = ",")))
      }
    }
    
    
    # Commit transaction
    dbCommit(connection)
  }, error = function(e) {
    # it Rollback in case of there is an error
    dbRollback(connection)
    stop(e)
  })
}

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)
csvFilePath <- args[1]
useTransactions <- args[2]  # "true" or "false"

# Check if the filePath is provided
if (is.null(csvFilePath)) {
  stop("No CSV file path provided as argument.")
}

csvFilePath <- "BirdStrikesData-New1.csv"  # Replace with your CSV file path
# it Run the function with the specified Csv_File and transaction setting
loadDataAndInsert(csvFilePath, useTransactions)

removeCSVData(csvFilePath, connection)



