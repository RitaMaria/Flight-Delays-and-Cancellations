import sqlite3
import pandas as pd

################ DATA IMPORTATION AND DEFINITION ############

########################## AIRLINES #########################

# read the dataset into a pandas dataframe
df_airlines = pd.read_csv("airlines.csv")

# rename column to correspond to schema 
df_airlines.rename(columns = {"IATA_CODE":"AIRLINE_CODE"}, inplace = True)

# list of tuples
airlines_list = list(df_airlines.itertuples(index = False, name = None))

############# AIRPORTS #############

# read the dataset into a pandas dataframe
df_airports = pd.read_csv("airports.csv")

# new ordering of the columns (placing column "LONGITUDE" before column "LATITUDE") to correspond to schema
df_airports = df_airports.iloc[:, [0,1,2,3,4,6,5]]

# list of tuples
airports_list = list(df_airports.itertuples(index = False, name = None))

############# FLIGHTS #############

# read the dataset into a pandas dataframe
df_flights = pd.read_csv("flights.csv")

# rename columns to correpond to schema
df_flights.rename(columns = {"AIRLINE":"AIRLINE_CODE"}, inplace = True)

# select only january
df_flights = df_flights.loc[df_flights["MONTH"] == 1]

# drop unnecessary columns
df_flights = df_flights.drop(['YEAR', 'MONTH', 'SCHEDULED_DEPARTURE', 'DEPARTURE_TIME',
              'TAXI_OUT','WHEELS_OFF', 'SCHEDULED_TIME', 
              'ELAPSED_TIME', 'AIR_TIME',  
              'WHEELS_ON', 'TAXI_IN', 'SCHEDULED_ARRIVAL', 'ARRIVAL_TIME',
              'DIVERTED', 'CANCELLED', 'CANCELLATION_REASON',
              'AIR_SYSTEM_DELAY', 'SECURITY_DELAY', 'AIRLINE_DELAY',
              'LATE_AIRCRAFT_DELAY', 'WEATHER_DELAY'], axis = 1)


# new ordering of the columns (placing column "DISTANCE" after column "TAIL_NUMBER") to correspond to schema
df_flights = df_flights.iloc[:, [0,1,2,3,4,8,5,6,7,9]]

# print(df_flights.head())


# list of tuples
flights_list = list(df_flights.itertuples(index = False, name = None))


############ CREATION OF DATABASE WITH NO INDEX ##########

db=sqlite3.connect('group5_sql_no_index.db')
cur =db.cursor()

################## CREATION OF TABLES ####################

cur.execute('''CREATE TABLE Airlines (
AIRLINE_CODE TEXT(2) PRIMARY KEY, 
AIRLINE TEXT(100) NOT NULL);''')
print ('table "airlines" without indexing created successfully')
cur.executemany("INSERT INTO Airlines (AIRLINE_CODE, AIRLINE) VALUES(?, ?)", airlines_list)
db.commit()

#print("SELECT * FROM Airlines")
#for row in cur.execute("SELECT * FROM Airlines"):
#    print(row)

cur.execute('''CREATE TABLE Airports (
IATA_CODE TEXT(3) PRIMARY KEY, 
AIRPORT TEXT(100) NOT NULL,
CITY TEXT(50) NOT NULL,
STATE TEXT(2) NOT NULL,
COUNTRY TEXT(3) NOT NULL,
LONGITUDE FLOAT,
LATITUDE FLOAT);''')
print ('table "airports" without indexing created successfully')
cur.executemany("INSERT INTO Airports \
                 (IATA_CODE, AIRPORT, CITY, STATE, COUNTRY, LONGITUDE, LATITUDE) VALUES(?, ?, ?, ?, ?, ?, ?)", airports_list)
db.commit()

#print("SELECT * FROM Airports")
#for row in cur.execute("SELECT * FROM Airports"):
#    print(row)

cur.execute('''CREATE TABLE Flights (
ID INTEGER PRIMARY KEY AUTOINCREMENT,
DAY INTEGER(2) NOT NULL,
DAY_OF_THE_WEEK INTEGER(1) NOT NULL,
AIRLINE_CODE TEXT(2) NOT NULL,
FLIGHT_NUMBER INTEGER(4) NOT NULL,
TAIL_NUMBER TEXT(6),
DISTANCE INTEGER(4) NOT NULL,
ORIGIN_AIRPORT TEXT(3) NOT NULL,
DESTINATION_AIRPORT TEXT(3) NOT NULL,
DEPARTURE_DELAY INTEGER(4),
ARRIVAL_DELAY INTEGER(4),
FOREIGN KEY (ORIGIN_AIRPORT) REFERENCES Airports (IATA_CODE) ON UPDATE CASCADE ON DELETE CASCADE,
FOREIGN KEY (DESTINATION_AIRPORT) REFERENCES Airports (IATA_CODE) ON UPDATE CASCADE ON DELETE CASCADE,
FOREIGN KEY (AIRLINE_CODE) REFERENCES Airlines (AIRLINE_CODE) ON UPDATE CASCADE ON DELETE CASCADE);''')
print ('table "flights" without indexing created successfully')
cur.executemany("INSERT INTO Flights \
                 (DAY, DAY_OF_THE_WEEK, AIRLINE_CODE, FLIGHT_NUMBER, TAIL_NUMBER, DISTANCE, \
                    ORIGIN_AIRPORT, DESTINATION_AIRPORT, DEPARTURE_DELAY, ARRIVAL_DELAY) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", flights_list)
db.commit()

#print("SELECT * FROM Flights")
#for row in cur.execute("SELECT * FROM Flights LIMIT 100"):
#    print(row)

db.close()


############ CREATION OF DATABASE WITH INDEX ##########

db=sqlite3.connect('group5_sql_index.db')
cur =db.cursor()

############ CREATION OF TABLES #######################

cur.execute('''CREATE TABLE Airlines (
AIRLINE_CODE TEXT(2) PRIMARY KEY, 
AIRLINE TEXT(100) NOT NULL);''')
print ('table "airlines" for indexing created successfully')
cur.executemany("INSERT INTO Airlines (AIRLINE_CODE, AIRLINE) VALUES(?, ?)", airlines_list)
db.commit()

# print("SELECT * FROM Airlines")
# for row in cur.execute("SELECT * FROM Airlines"):
#     print(row)

cur.execute('''CREATE TABLE Airports (
IATA_CODE TEXT(3) PRIMARY KEY, 
AIRPORT TEXT(100) NOT NULL,
CITY TEXT(50) NOT NULL,
STATE TEXT(2) NOT NULL,
COUNTRY TEXT(3) NOT NULL,
LONGITUDE FLOAT,
LATITUDE FLOAT);''')
print ('table "airports" for indexing created successfully')
cur.executemany("INSERT INTO Airports \
                 (IATA_CODE, AIRPORT, CITY, STATE, COUNTRY, LONGITUDE, LATITUDE) VALUES(?, ?, ?, ?, ?, ?, ?)", airports_list)
db.commit()

# print("SELECT * FROM Airports")
# for row in cur.execute("SELECT * FROM Airports"):
#     print(row)

cur.execute('''CREATE TABLE Flights (
ID INTEGER PRIMARY KEY AUTOINCREMENT,
DAY INTEGER(2) NOT NULL,
DAY_OF_THE_WEEK INTEGER(1) NOT NULL,
AIRLINE_CODE TEXT(2) NOT NULL,
FLIGHT_NUMBER INTEGER(4) NOT NULL,
TAIL_NUMBER TEXT(6),
DISTANCE INTEGER(4) NOT NULL,
ORIGIN_AIRPORT TEXT(3) NOT NULL,
DESTINATION_AIRPORT TEXT(3) NOT NULL,
DEPARTURE_DELAY INTEGER(4),
ARRIVAL_DELAY INTEGER(4),
FOREIGN KEY (ORIGIN_AIRPORT) REFERENCES Airports (IATA_CODE) ON UPDATE CASCADE ON DELETE CASCADE,
FOREIGN KEY (DESTINATION_AIRPORT) REFERENCES Airports (IATA_CODE) ON UPDATE CASCADE ON DELETE CASCADE,
FOREIGN KEY (AIRLINE_CODE) REFERENCES Airlines (AIRLINE_CODE) ON UPDATE CASCADE ON DELETE CASCADE);''')
print ('table "flights" for indexing created successfully')
cur.executemany("INSERT INTO Flights \
                 (DAY, DAY_OF_THE_WEEK, AIRLINE_CODE, FLIGHT_NUMBER, TAIL_NUMBER, DISTANCE, \
                    ORIGIN_AIRPORT, DESTINATION_AIRPORT, DEPARTURE_DELAY, ARRIVAL_DELAY) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", flights_list)
db.commit()

# print("SELECT * FROM Flights")
# for row in cur.execute("SELECT * FROM Flights LIMIT 100"):
#     print(row)

db.close()