dbFileName <- "lessonDB-AyubaW.sqlitedb"

dbcon <- dbConnect(RSQLite::SQLite(), dbname = dbFileName)

#Foreign key activated
dbExecute(dbcon, "PRAGMA foreign_keys = ON;")

#if Existing tables, drop it
dbExecute(dbcon, "DROP TABLE IF EXISTS LessonModule;")
dbExecute(dbcon, "DROP TABLE IF EXISTS Lesson;")
dbExecute(dbcon, "DROP TABLE IF EXISTS Module;")



#Lesson table defined
dbExecute(dbcon, "CREATE TABLE Lesson (
          category INTEGER NOT NULL,
          number INTEGER NOT NULL,
          title TEXT NOT NULL,
          PRIMARY KEY (category, number)
);")


#Module table defined 
dbExecute(dbcon, "CREATE TABLE Module (
          mid TEXT NOT NULL,
          title TEXT NOT NULL,
          lengthInMinutes INTEGER NOT NULL,
          difficulty TEXT NOT NULL DEFAULT 'beginner' CHECK (difficulty IN ('beginner', 'intermidiate', 'advance')),
          PRIMARY KEY (mid)
);")


#Junction table of Lesson and Module defined
dbExecute(dbcon, "CREATE TABLE LessonModule (
          category INTEGER NOT NULL,
          number INTEGER NOT NULL,
          mid TEXT NOT NULL, 
          PRIMARY KEY (category, number, mid),
          FOREIGN KEY (category, number) REFERENCES Lesson(category, number),
          FOREIGN KEY (mid) REFERENCES Module(mid)
);")


#Disable Foreign key constraint optionally before loading data
dbExecute(dbcon, "PRAGMA foreign_keys = OFF;")

#Test data inserted into the Lesson table 
dbExecute(dbcon, "INSERT INTO Lesson ( category, number, title) VALUES (1, 101, 'Database Management');")
dbExecute(dbcon, "INSERT INTO Lesson ( category, number, title) VALUES (2, 201,'Advance Database Management');")

#Test data inserted into the Module table 
dbExecute(dbcon, "INSERT INTO Module ( mid, title, lengthInMinutes, difficulty) VALUES ('MODL001', 'Basic of Database Schema', 30, 'beginner');")
dbExecute(dbcon, "INSERT INTO Module ( mid, title, lengthInMinutes, difficulty) VALUES ('MODL002','Mastery of Database Schema', 45, 'advance');")

#Test data inserted into the LessonModule table 
dbExecute(dbcon, "INSERT INTO LessonModule ( category, number, mid) VALUES (1, 101, 'MODL001');")
dbExecute(dbcon, "INSERT INTO LessonModule ( category, number, mid) VALUES (2, 201,'MODL002');")

#Re-enable Foreign key constraint optionally after loading data
dbExecute(dbcon, "PRAGMA foreign_keys = ON;")


dbListTables(dbcon)

#Consider a lesson.csv and module.csv file
lesson <- read.csv("lesson.csv")
module <- read.csv("module.csv")

#Load data frame into database table
dbWriteTable(dbcon, "lesson", lesson, append = TRUE, row.names = FALSE)
dbWriteTable(dbcon, "module", module, append = TRUE, row.names = FALSE)


dbDisconnect(dbcon)
