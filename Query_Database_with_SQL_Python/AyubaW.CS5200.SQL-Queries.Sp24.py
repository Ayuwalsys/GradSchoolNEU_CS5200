#Assignment Name: 06.2: Query a Database with SQL in Python 
#Date: 2/21/2024
#Name: Waliu Ayuba

import sqlite3
from pathlib import Path


#Database Name 
db_fileName = "MediaDB.db"

#Create path to the database file 
db_pathfile = Path.cwd() / db_fileName

#Connect to the SQLite database 
dbcon = sqlite3.connect(db_pathfile)

# This funciton execute a query and print the output
def execute_query(query):
  cursor = dbcon.cursor()
  cursor.execute(query)
  rows = cursor.fetchall()
  for row in rows:
    print(row)
  


#Query to get the last name, first name, title, and hire date of all employees, sorted by last name.
query_employees = """ SELECT LastName, FirstName, Title, HireDate 
                      FROM employees
                      ORDER BY LastName
                  """

#The Query to get the total number of bytes for all tracks.

query_total_bytes = """ SELECT SUM(Bytes)
                        FROM tracks;
                    """

#The Query to Display all of the genres.

query_genres = """ SELECT Name
                   FROM genres;
               """
               
#Queries execution and print result
print("Employee sorted by LastName:")
execute_query(query_employees)


print("\nTotal number of bytes for all tracks:")
execute_query(query_total_bytes)

print("\nList of all the genres:")
execute_query(query_genres)


#Close the connection to the databas
dbcon.close()

