## Prepare Airports Data
prepareAirportsData <- function(df) {
  # Extract unique airports along with their states
  unique_airports <- unique(df[, c("airport", "origin")])
  
  # Remove missing values or replace with a placeholder
  unique_airports <- na.omit(unique_airports)
  
  # Create a dataFrame modified according to schema
  df_airports <- data.frame(
    airportName = unique_airports$airport,
    airportState = unique_airports$origin,
    airportCode = rep('ZZZ', nrow(unique_airports))
  )
  
  return(df_airports)
}

## Prepare flight Data
prepareFlightsData <- function(df, airports_df) {
  # Convert flightDate to Date format
  df$flight_date <- as.Date(df$flight_date, format = "%m/%d/%Y")
  
  # Map originAirport to airport IDs
  df$originAirport <- match(df$airport, airports_df$airportName)
  
  # Handle missing airline values
  df$airline[is.na(df$airline)] <- "Unknown"
  
  # Convert heavy_flag to boolean
  df$isHeavy <- df$heavy_flag == "Yes"
  
  # Subset and rename columns as per table structure
  flights_df <- data.frame(
    fid = df$rid,
    date = df$flight_date,
    originAirport = df$originAirport,
    airlineName = df$airline,
    aircraftType = df$aircraft,
    isHeavy = df$isHeavy
  )
  
  return(flights_df)
}

## Prepare Conditions Data
prepareConditionsData <- function(df) {
  # Extract unique sky conditions and remove missing values
  unique_conditions <- unique(df$sky_conditions)
  
  # Create a dataFrame with sky_condition and empty explanation
  conditions_df <- data.frame(
    sky_condition = unique_conditions,
    explanation = rep("", length(unique_conditions)) # Setting explanation to empty string
  )
  
  return(conditions_df)
}

## Prepare Strike Data
prepareStrikesData <- function(df, flights_df, conditions_df) {
  # Convert altitude_ft to numeric and ensure it's non-negative
  df$altitude_ft <- as.numeric(gsub(",", "", df$altitude_ft))
  df$altitude_ft[df$altitude_ft < 0] <- 0
  
  # Map fid to flight IDs based on the record ID
  df$fid <- match(df$rid, flights_df$fid)
  df$fid[is.na(df$fid)] <- -1  # Handles unmatched fids with a default value
  
  # Map conditions to condition IDs based on sky conditions
  df$conditions <- match(df$sky_conditions, conditions_df$sky_condition)
  df$conditions[is.na(df$conditions)] <- -1  # Handle unmatched conditions with a default value
  
  # Convert damage description to boolean (TRUE for 'Damage', FALSE otherwise)
  df$damage <- df$damage == "Damage"
  
  # Handle numbirds
  if (!"numbirds" %in% names(df) || all(is.na(df$numbirds))) {
    df$numbirds <- rep(0, nrow(df))  # Assign 0 to all rows if the column doesn't exist or is entirely NA
  } else {
    df$numbirds <- as.integer(df$numbirds)
    df$numbirds[is.na(df$numbirds)] <- 0  # Replace NA with 0
  }
  
  # Subset and rename columns as per table structure
  strikes_df <- data.frame(
    fid = df$fid,
    numbirds = df$numbirds,
    impact = df$impact,
    damage = df$damage,
    altitude = df$altitude_ft,
    conditions = df$conditions
  )
  
  return(strikes_df)
}

###POPULATE THE DATABASE TABLES

## Populate the airport table
insertAirportsData <- function(connection, df_airports) {
  # Check for duplicate entries in airports data
  if(nrow(airports_data) != nrow(unique(airports_data))) {
    stop("Duplicate entries found in airports data")
  }
  # resolve single quotes by replacing with two single quotes
  df_airports$airportName <- gsub("'", "''", df_airports$airportName)
  df_airports$airportState <- gsub("'", "''", df_airports$airportState)
  df_airports$airportCode <- gsub("'", "''", df_airports$airportCode)
  
  dbBegin(connection)
  tryCatch({
    dbExecute(connection, sprintf("INSERT INTO airports (airportName, airportState, airportCode) VALUES %s",
                                  paste(sprintf("('%s', '%s', '%s')", df_airports$airportName, df_airports$airportState, df_airports$airportCode), collapse = ",")))
    dbCommit(connection)
  }, error = function(e) {
    dbRollback(connection)
    stop(e)
  })
}

## Populate Flight Table
insertFlightsData <- function(connection, df_flights) {
  # Convert isHeavy logical values to 1 (TRUE) or 0 (FALSE)
  df_flights$isHeavy <- ifelse(is.na(df_flights$isHeavy), 'NULL', as.integer(df_flights$isHeavy))
  
  dbBegin(connection)
  tryCatch({
    formatted_values <- apply(df_flights, 1, function(row) {
      row <- ifelse(is.na(row), 'NULL', row)
      sprintf("('%s', %s, '%s', '%s', %s)", row["date"], row["originAirport"], row["airlineName"], row["aircraftType"], row["isHeavy"])
    })
    sql_query <- sprintf("INSERT INTO flights (date, originAirport, airlineName, aircraftType, isHeavy) VALUES %s", 
                         paste(formatted_values, collapse = ","))
    dbExecute(connection, sql_query)
    dbCommit(connection)
  }, error = function(e) {
    dbRollback(connection)
    stop(e)
  })
}

## Populate 'conditions' table 
insertConditionsData <- function(connection, df_conditions) {
  dbBegin(connection)
  tryCatch({
    dbExecute(connection, sprintf("INSERT INTO conditions (sky_condition, explanation) VALUES %s",
                                  paste(sprintf("('%s', '%s')", df_conditions$sky_condition, df_conditions$explanation), collapse = ",")))
    dbCommit(connection)
  }, error = function(e) {
    dbRollback(connection)
    stop(e)
  })
}

## Populate 'strikes'
insertStrikesData <- function(connection, df_strikes) {
  # Replace NA values with 'NULL' in columns that can have NA
  df_strikes$fid <- ifelse(is.na(df_strikes$fid), 'NULL', df_strikes$fid)
  df_strikes$numbirds <- ifelse(is.na(df_strikes$numbirds), 'NULL', df_strikes$numbirds)
  df_strikes$impact <- ifelse(is.na(df_strikes$impact), 'NULL', df_strikes$impact)
  df_strikes$damage <- ifelse(is.na(df_strikes$damage), 'NULL', as.integer(df_strikes$damage))
  df_strikes$altitude <- ifelse(is.na(df_strikes$altitude), 'NULL', df_strikes$altitude)
  df_strikes$conditions <- ifelse(is.na(df_strikes$conditions), 'NULL', df_strikes$conditions)
  
  dbBegin(connection)
  tryCatch({
    formatted_values <- apply(df_strikes, 1, function(row) {
      sprintf("(%s, %s, '%s', %s, %s, %s)", row["fid"], row["numbirds"], row["impact"], row["damage"], row["altitude"], row["conditions"])
    })
    sql_query <- sprintf("INSERT INTO strikes (fid, numbirds, impact, damage, altitude, conditions) VALUES %s", 
                         paste(formatted_values, collapse = ","))
    dbExecute(connection, sql_query)
    dbCommit(connection)
  }, error = function(e) {
    dbRollback(connection)
    stop(e)
  })
}

