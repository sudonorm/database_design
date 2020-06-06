# requires Python 3
# requires MySQL to be installed

import mysql.connector
from mysql.connector import Error
import os
import pandas as pd
path = os.getcwd()

class Database():
    
    def __init__(self, db, host, user, pswd):
        self.n = 0
        self.dbName = db
        self.host = host
        self.user = user
        self.pswd = pswd

    def create_db(self):
        try:
            db = mysql.connector.connect(host=self.host,
                                            user=self.user,
                                            password=self.pswd)

            cursor = db.cursor()

            dbstr = "CREATE DATABASE IF NOT EXISTS " + self.dbName + ";"
            cursor.execute(dbstr)
            cursor.execute("SHOW DATABASES;")
            for db_name in cursor:
                if db_name[0] == self.dbName:
                    print(str(db_name[0]) + " created or already exists!")
                    
        except Error as e:
            print("Error while connecting to MySQL", e)
            
    
    def connect(self):
        try:
            connection = mysql.connector.connect(host=self.host,
                                            user=self.user,
                                            database = self.dbName,
                                            password=self.pswd)

            if connection.is_connected():
                cursor = connection.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                cursor.close()

                return connection
        except Error as e:
            print("Error while connecting to MySQL", e)
            
            
    def setup_tables(self):
        connection = self.connect()
        cursor = connection.cursor()

        qryLoc = "CREATE TABLE IF NOT EXISTS location (id INT UNSIGNED NOT NULL, countriesAndTerritories VARCHAR(255) NOT NULL, geoId CHAR(10), code3L CHAR(10), PRIMARY KEY (id));"
        qryNames = "CREATE TABLE IF NOT EXISTS names (id INT UNSIGNED NOT NULL, value VARCHAR(255) NOT NULL, PRIMARY KEY (id));"

        qryCases = "CREATE TABLE IF NOT EXISTS cases (id INT UNSIGNED NOT NULL, countriesAndTerritories INT UNSIGNED NOT NULL, value INT, dateRep VARCHAR(50), source VARCHAR(255), PRIMARY KEY (id, dateRep), FOREIGN KEY (id)  REFERENCES names  (id), FOREIGN KEY (countriesAndTerritories)  REFERENCES location  (id));"
        qryDeaths = "CREATE TABLE IF NOT EXISTS deaths (id INT UNSIGNED NOT NULL, countriesAndTerritories INT UNSIGNED NOT NULL, value INT, dateRep VARCHAR(50), source VARCHAR(255), PRIMARY KEY (id, dateRep), FOREIGN KEY (id)  REFERENCES names  (id), FOREIGN KEY (countriesAndTerritories)  REFERENCES location  (id));"

        qryList = [qryLoc, qryNames, qryCases, qryDeaths]

        for qry in qryList:
            cursor.execute(qry)

        cursor.close()
        connection.close()
        
    def to_string(self, text):
        text = '"' + str(text) + '"'
        return text
    
    
    def insert(self, iType = "location", A = "", B = "", C = "", D = "", E = "", table=""):
        connection = self.connect()
        cursor = connection.cursor()

        if iType == "location":
            qry = "INSERT INTO location (id, countriesAndTerritories, geoId, code3L) VALUES ({}, {}, {}, {}) ON DUPLICATE KEY UPDATE countriesAndTerritories={}, geoId={}, code3L={};".format(A, self.to_string(B), self.to_string(C), self.to_string(D), self.to_string(B), self.to_string(C), self.to_string(D))
            
        elif iType == "name":
            qry = "INSERT INTO names (id, value) VALUES ({}, {}) ON DUPLICATE KEY UPDATE value = {};".format(A, self.to_string(B), self.to_string(B))
        
        elif iType == "cases":
            qry = "INSERT INTO cases (id, countriesAndTerritories, value, dateRep, source) VALUES ({}, {}, {}, {}, {}) ON DUPLICATE KEY UPDATE countriesAndTerritories={}, value={}, source={};".format(A, B, C, self.to_string(D), self.to_string(E), B, C, self.to_string(E))
        
        elif iType == "deaths":
            qry = "INSERT INTO deaths (id, countriesAndTerritories, value, dateRep, source) VALUES ({}, {}, {}, {}, {}) ON DUPLICATE KEY UPDATE countriesAndTerritories={}, value={}, source={};".format(A, B, C, self.to_string(D), self.to_string(E), B, C, self.to_string(E))
        
        cursor.execute(qry)
        connection.commit()
        cursor.close()
        connection.close()
    
    
    def make_location_file(self, covid):

        loc = covid[["countriesAndTerritories", "geoId", "countryterritoryCode"]].drop_duplicates().reset_index(drop=True)
        loc = loc.reset_index(drop=True).reset_index().rename(columns={"index":"id", "countryterritoryCode": "code3L"})
        loc["id"] = loc["id"] + 1

        return loc
    
    def update_location_file(self, loc):
        oldLoc = pd.read_excel(path +"\\covidLocationTable.xlsx" )
        maxId = max(oldLoc["id"])
        loc = loc[["countriesAndTerritories", "geoId", "code3L"]]
        loc = loc[~(loc.countriesAndTerritories.isin(list(oldLoc["countriesAndTerritories"].drop_duplicates())))].reset_index(drop=True)

        if len(loc) > 0:
            for row in loc.itertuples():
                maxId += 1
                cT = getattr(row, "countriesAndTerritories")
                gId = getattr(row, "geoId")
                c3L = getattr(row, "code3L")
                oldLoc = oldLoc.append({"id":maxId, "countriesAndTerritories":cT, "geoId": gId, "code3L":c3L}, ignore_index=True)
        
        oldLoc = oldLoc.fillna(value="na")
        oldLoc.to_excel(path +"\\covidLocationTable.xlsx", index=False)
        
    def add_special(self, df, add_type= "part", table = "cases", rStart = 0, rStop = 1):
        self.n = 0
        if add_type == "part":
            for row in df.iloc[rStart:rStop,:].itertuples():
                id = getattr(row, "id")
                cTs = getattr(row, "countriesAndTerritories")
                val = getattr(row, "value")
                date = getattr(row, "dateRep")
                source = getattr(row, "source")
                self.insert(iType = table, A = id, B = cTs, C = val, D = date, E=source)
                self.n += 1
                print(self.n, "Record(s) Inserted")
        elif add_type == "all":
            for row in df.itertuples():
                id = getattr(row, "id")
                cTs = getattr(row, "countriesAndTerritories")
                val = getattr(row, "value")
                date = getattr(row, "dateRep")
                source = getattr(row, "source")
                self.insert(iType = table, A = id, B = cTs, C = val, D = date, E=source)
                self.n += 1
                print(self.n, "Record(s) Inserted")

    
    def update_data(self, covid):

        #full_source: "https://www.ecdc.europa.eu/en/publications-data/download-todays-data-geographic-distribution-covid-19-cases-worldwide"
        source = "https://www.ecdc.europa.eu/"
        
        location = pd.read_excel(path +"\\covidLocationTable.xlsx" )

        cases = covid[["dateRep","countryterritoryCode", "cases"]].rename(columns={"cases":"value"}).merge(location[["id", "code3L"]], left_on = ["countryterritoryCode"], right_on=["code3L"]).rename(columns={"id":"countriesAndTerritories"}).drop(columns=["code3L"])
        deaths = covid[["dateRep","countryterritoryCode", "deaths"]].rename(columns={"deaths":"value"}).merge(location[["id", "code3L"]], left_on = ["countryterritoryCode"], right_on=["code3L"]).rename(columns={"id":"countriesAndTerritories"}).drop(columns=["code3L"])

        cases["source"] = source
        cases["id"] = 1
        deaths["source"] = source
        deaths["id"] = 2

        cases = cases[["id", "countriesAndTerritories", "value", "dateRep", "source"]]
        deaths = deaths[["id", "countriesAndTerritories", "value", "dateRep", "source"]]
        
        cases = cases.sort_values(by = ["id", "countriesAndTerritories", "dateRep"])
        deaths = deaths.sort_values(by = ["id", "countriesAndTerritories", "dateRep"])
        cases["value"] = cases["value"].astype(int)
        deaths["value"] = deaths["value"].astype(int)
        cases["countriesAndTerritories"] = cases["countriesAndTerritories"].astype(int)
        deaths["countriesAndTerritories"] = deaths["countriesAndTerritories"].astype(int)
        cases["dateRep"] = cases["dateRep"].astype(str)
        deaths["dateRep"] = deaths["dateRep"].astype(str)
        
        print(" ")
        print("Updating names table...")
        # add names
        namesList = ["Number of cases", "Number of deaths"]
        for indx, val in enumerate(namesList, start=1):
            self.insert(iType = "name", A = indx, B = val)
            self.n += 1
            print(self.n, "Record(s) Inserted")
        
        print(" ")
        print("Updating location table...")
        # add location table
        self.n = 0
        for row in location.itertuples():
            id = getattr(row, "id")
            cTs = getattr(row, "countriesAndTerritories")
            geoID = getattr(row, "geoId")
            c3L = getattr(row, "code3L")
            self.insert(iType = "location", A = id, B = cTs, C = geoID, D = c3L)
            self.n += 1
            print(self.n, "Record(s) Inserted")
        
        print(" ")
        print("Inserting number of cases...")
        # add cases  
        self.add_special(cases, add_type= "part", table = "cases", rStart = 0, rStop = 25)
        
        print(" ")
        print("Inserting number of deaths...")
        # add deaths
        self.add_special(deaths, add_type= "part", table = "deaths", rStart = 0, rStop = 25)
        
        print(" ")
        print("All records inserted!")