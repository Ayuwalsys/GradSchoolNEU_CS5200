
library(RSQLite)

dbfile <- 'MediaDB.db'

dbcon <- dbConnect(SQLite(), dbfile)

#Add a new column to the "albums" table
#check and add if the column 'play_time exists
if(!("play_time" %in% colnames(dbReadTable(dbcon, "albums")))){
  dbExecute(dbcon, "ALTER TABLE albums ADD COLUMN play_time NUMERIC CHECK (play_time >= 0 ) ")
}


#Calculate the total playtime of each album and update the 'play_time' column

dbExecute(dbcon, "
          UPDATE albums
          SET play_time = (
            SELECT SUM(Milliseconds) / 60000.0
            FROM tracks
            WHERE albums.AlbumId = tracks.AlbumId
          )")


#Create "after insert" trigger for "tracks"
#Create trigger to update "play_time" after a new track is inserted

dbExecute(dbcon,"
          CREATE TRIGGER IF NOT EXISTS after_track_insert
          AFTER INSERT ON tracks 
          BEGIN 
            UPDATE albums
            SET play_time = (
              SELECT SUM(Milliseconds / 60000.0)
              FROM tracks
              WHERE tracks.AlbumId = NEW.AlbumId
            )
            WHERE AlbumId = NEW.AlbumId;
          END;
            ")



#Create "after update" trigger for "track"
#Create trigger to update "play_time" after a new is track is updated 

dbExecute(dbcon, "
          CREATE TRIGGER IF NOT EXISTS after_track_update
          AFTER UPDATE ON tracks
          BEGIN
            UPDATE albums
            SET play_time = (
              SELECT SUM(Milliseconds / 60000.0)
              FROM tracks 
              WHERE tracks.AlbumId = NEW.AlbumId
            )
            WHERE AlbumId = NEW.AlbumId;
          END;
          ")


#Create "after delete" trigger for "tracks"
#Create trigger to update "play_time" after a new track is deleted

dbExecute(dbcon, "
          CREATE TRIGGER IF NOT EXISTS after_track_delete
          AFTER DELETE ON tracks
          BEGIN
          UPDATE albums
          SET play_time = (
            SELECT SUM(Milliseconds / 60000.0)
            FROM tracks
            WHERE tracks.AlbumId = OLD.AlbumId
          )
          WHERE AlbumId = OLD.AlbumId;
          END;
          ")


#Insert a new track to test the 'after insert' trigger
dbExecute(dbcon, "INSERT INTO tracks (Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice) VALUES('New Track', 1,1,1, 'Test Composer', 300000, 1024, 0.99)")

#Update a tract to test the "after update" trigger
dbExecute(dbcon, "UPDATE tracks SET Milliseconds = 400000 WHERE TrackId = (SELECT MAX(TrackId) FROM tracks)")

#Delete a track to test the "after delete" trigger 
dbExecution(dbcon, "DELETE FROM tracks WHERE TrackId = (SELECT MAX(TrackId) FROM tracks)")

#Check the "play_time" of an album to see if the triggers worked
dbGetQuery(dbcon, "SELECT AlbumId, play_time FROM albums WHERE AlbumId = 1")
