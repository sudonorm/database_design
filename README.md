# :clubs: database_design
A repo containing code which can be used to design and manage different types of databases

## :floppy_disk: MySQL

The MySQL folder contains a class which can be used to set up and manage a MySQL database, as well as the data used in the test run.

:hammer:  Requirements: MySQL, Python 3

:electric_plug:  Packages: mysql.connector (```pip install mysql-connector```)

:wrench:  Usage: 

  - read in the data: ```covid = pd.read_excel("COVID-19-geographic-disbtribution-worldwide-2020-04-19.xlsx")```

  - Initialize the Database class: ```objDB = Database(db, host, user, pswd)``` where db is the name of the database you want to create, host is the hostname, user is the username of the MySQL database, and pswd is the Password of the MySQL database

  - Create the database: ```objDB.create_db()```

  - Set up needed tables: ```objDB.setup_tables()```

  - Create or update the location table: ```objDB.update_location_file(objDB.make_location_file(covid))```

  - Update the database or add new data: ```objDB.update_data(covid)```
