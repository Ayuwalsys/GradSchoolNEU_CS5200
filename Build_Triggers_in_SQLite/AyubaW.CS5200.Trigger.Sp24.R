#Assignment Name: 06.1: Build Triggers in SQLite
#Date: 2/20/2024
#Name: Waliu Ayuba


library(RSQLite)

# The function connect to the SQLite database
connect_db <- function(db_file) {
  dbcon <- dbConnect(SQLite(), dbname = db_file)
  return(dbcon)
}

# The function add a new column to the 'albums' table
add_column_play_time <- function(dbcon) {
  res <- dbSendQuery(dbcon, "PRAGMA table_info(albums)")
  info <- dbFetch(res)
  dbClearResult(res)
  
  if (!'play_time' %in% info$name) {
    dbExecute(conn, "ALTER TABLE albums ADD COLUMN play_time NUMERIC CHECK (play_time > 0)")
  }
}

# This Function calculate the total play_time for each album
update_play_time <- function(dbcon) {
  dbExecute(dbcon, "UPDATE albums SET play_time = (SELECT SUM(milliseconds / 60000.0) FROM tracks WHERE tracks.AlbumId = albums.AlbumId)")
}

# This Function create a trigger to update 'play_time' after insert, update, or delete on 'tracks'
create_triggers <- function(dbcon) {
  triggers <- list(
    c('after_insert_trigger', 'AFTER INSERT'),
    c('after_update_trigger', 'AFTER UPDATE'),
    c('after_delete_trigger', 'AFTER DELETE')
  )
  
  for (trigger in triggers) {
    dbExecute(dbcon, paste0("DROP TRIGGER IF EXISTS ", trigger[1]))
    dbExecute(dbcon, sprintf("s
      CREATE TRIGGER %s
      %s ON tracks
      FOR EACH ROW
      BEGIN
        UPDATE albums
        SET play_time = (
          SELECT SUM(milliseconds / 60000.0) 
          FROM tracks 
          WHERE tracks.AlbumId = NEW.AlbumId
        )
        WHERE AlbumId = NEW.AlbumId;
      END;", trigger[1], trigger[2]))
  }
}

# Test used cases that demonstrate how the trigger works
test_triggers <- function(dbcon) {
  dbExecute(dbcon, "INSERT INTO tracks (Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice) VALUES ('Test Track', 1, 1, 1, 'Test Composer', 240000, 0, 0.99)")
  result <- dbGetQuery(dbcon, "SELECT play_time FROM albums WHERE AlbumId = 1")
  print(paste0("Updated play_time for AlbumId 1: ", result$play_time, " minutes"))
}

# Execute Main 
main <- function() {
  database_file <- 'MediaDB.db'
 
  # Connection to database
  dbcon <- connect_db(database_file)
  
  #play_time column added if it doesn't exist
  add_column_play_time(dbcon)
  
  # Update play_time for each album
  update_play_time(dbcon)
  
  # Create triggers
  create_triggers(dbcon)
  
  # Test triggers
  test_triggers(dbcon)
  
  # Close connection
  dbDisconnect(dbcon)
}

# Run the main function
main()

