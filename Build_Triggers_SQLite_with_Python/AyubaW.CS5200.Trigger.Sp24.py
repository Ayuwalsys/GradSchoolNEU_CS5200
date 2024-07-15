import sqlite3

# This Function to connect to the SQLite database
def db_connection(db_file):
    dbcon = None
    try:
        dbcon = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return dbcon

# Function to add a new column to the 'albums' table
def add_column_play_time(dbcon):
    cursor = conn.cursor()
    # This is to check if the column 'play_time' exists in 'albums'
    cursor.execute("""
        SELECT * 
        FROM pragma_table_info('albums') 
        WHERE name='play_time'
    """)
    exists = cursor.fetchone()
    # If it does not exist, add it
    if not exists:
        cursor.execute("""
            ALTER TABLE albums 
            ADD COLUMN play_time NUMERIC CHECK (play_time > 0)
        """)
        dbcon.commit()

# This is the function that calculate the total_play_time for each album
def update_play_time(dbcon):
    cursor = dbcon.cursor()
    # This update the play_time column with the sum of the milliseconds from the tracks which converted to minutes
    cursor.execute("""
        UPDATE albums 
        SET play_time = (
            SELECT SUM(milliseconds / 60000.0) 
            FROM tracks 
            WHERE tracks.AlbumId = albums.AlbumId
        )
    """)
    dbcon.commit()

# Function to create a trigger to update 'play_time' after insert, update, or delete on 'tracks'
def create_triggers(dbcon):
    cursor = dbcon.cursor()
    triggers = [
        ('after_insert_trigger', 'AFTER INSERT'),
        ('after_update_trigger', 'AFTER UPDATE'),
        ('after_delete_trigger', 'AFTER DELETE')
    ]
    for trigger_name, trigger_event in triggers:
        # Drop the trigger if it already exists to avoid errors
        cursor.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
        # Create the trigger
        cursor.execute(f"""
            CREATE TRIGGER {trigger_name}
            {trigger_event} ON tracks
            FOR EACH ROW
            BEGIN
                UPDATE albums
                SET play_time = (
                    SELECT SUM(milliseconds / 60000.0) 
                    FROM tracks 
                    WHERE tracks.AlbumId = NEW.AlbumId
                )
                WHERE AlbumId = NEW.AlbumId;
            END;
        """)
    dbcon.commit()

# Test cases to demonstrate that the trigger works
def test_triggers(dbcon):
    cursor = dbcon.cursor()
    # Insert a test track to see if the trigger updates the play_time accordingly
    cursor.execute("""
        INSERT INTO tracks (Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice)
        VALUES ('Test Track', 1, 1, 1, 'Test Composer', 240000, 0, 0.99)
    """)
    dbcon.commit()

    # Query to check if the play_time has been updated for AlbumId 1
    cursor.execute("""
        SELECT play_time FROM albums WHERE AlbumId = 1
    """)
    result = cursor.fetchone()
    print(f"Updated play_time for AlbumId 1: {result[0]} minutes")

# Main execution
def main():
    database_file = 'MediaDB.db'
    
    # Connect to the database
    dbcon = db_connection(database_file)
    
    # It Add play_time column if it doesn't exist
    add_column_play_time(dbcon)
    
    # Update play_time for each album
    update_play_time(dbcon)
    
    # Create triggers
    create_triggers(dbcon)
    
    # Test the triggers
    test_triggers(dbcon)
    
    # Close the connection
    dbcon.close()

# Run the main function
main()



