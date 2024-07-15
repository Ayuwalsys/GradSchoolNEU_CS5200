import sqlite3

# The provided snippet already contains the function to connect to the SQLite database.
# Here, we complete the remaining functions and execute the program.

# Function to connect to the SQLite database
def connect_to_db(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn

# Function to add a new column to the 'albums' table
def add_column_play_time(conn):
    cursor = conn.cursor()
    # Check if the column 'play_time' exists in 'albums'
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
        conn.commit()

# Function to calculate the total play time for each album
def update_play_time(conn):
    cursor = conn.cursor()
    # Update the play_time column with the sum of the milliseconds from the tracks, converted to minutes
    cursor.execute("""
        UPDATE albums 
        SET play_time = (
            SELECT SUM(milliseconds / 60000.0) 
            FROM tracks 
            WHERE tracks.AlbumId = albums.AlbumId
        )
    """)
    conn.commit()

# Function to create a trigger to update 'play_time' after insert, update, or delete on 'tracks'
def create_triggers(conn):
    cursor = conn.cursor()
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
    conn.commit()

# Test cases to demonstrate that the trigger works
def test_triggers(conn):
    cursor = conn.cursor()
    # Insert a test track to see if the trigger updates the play_time accordingly
    cursor.execute("""
        INSERT INTO tracks (Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice)
        VALUES ('Test Track', 1, 1, 1, 'Test Composer', 240000, 0, 0.99)
    """)
    conn.commit()

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
    conn = connect_to_db(database_file)
    
    # Add play_time column if it doesn't exist
    add_column_play_time(conn)
    
    # Update play_time for each album
    update_play_time(conn)
    
    # Create triggers
    create_triggers(conn)
    
    # Test the triggers
    test_triggers(conn)
    
    # Close the connection
    conn.close()

# Run the main function
main()



