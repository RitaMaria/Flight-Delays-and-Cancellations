from pymongo import MongoClient
import sqlite3
import json

# function to print docs
def print_docs(docs):
    for x in docs:
        print(x)


# function to print the first five results (can be rows or documents)
def print_results_5(results):
    count = 0
    for x in results:
        if count < 5:
            print(x)
        count += 1

# CONNECTIONS

# sqlite3
conn = sqlite3.connect('group5_sql_no_index.db')
c = conn.cursor()

# mongodb
client = MongoClient()
db = client.group5_mongo_no_index
flights_collection = db['flights']
airports_collection = db['airports']
airlines_collection = db['airlines']

############## FLIGHT_NUMBER > 7000 ###########

# sqlite3
query1 = "SELECT FLIGHT_NUMBER FROM Flights WHERE FLIGHT_NUMBER > 7000"
c.execute(query1)
rows1 = c.fetchall()
#for row in rows1:
#    print(row)
c.execute(query1)
rows_1_many = c.fetchmany(5)
print("The first five results for the 1st query in SQLite are:")
print_results_5(rows_1_many)

print("----")

# mongodb
result1 = flights_collection.find({"FLIGHT_NUMBER": {"$gt":7000}}, {"FLIGHT_NUMBER":1})
#print_docs(result1)
print("The first five results for the 1st query in MongoDB are: ")
print_results_5(result1)

print("----")

######################## Airport and city ###############

# sqlite3
query2 = "SELECT AIRPORT, CITY FROM Airports"
c.execute(query2)
rows2 = c.fetchall()
#for row in rows2:
#    print(row)

c.execute(query2)
rows_2_many = c.fetchmany(5)
print("The first five results for the 2nd query in SQLite are:")
print_results_5(rows_2_many)

print("----")

# mongodb
result2 = airports_collection.find({}, {"AIRPORT": 1, "CITY": 1})
#print_docs(result2)
print("The first five results for the 2nd query in MongoDB are: ")
print_results_5(result2)

print("----")

############## Airports with 30 < latitude < 50 ###################

# sqlite3
query3 = "SELECT AIRPORT, LONGITUDE, LATITUDE FROM Airports WHERE LATITUDE BETWEEN 30 AND 50"
c.execute(query3)
rows3 = c.fetchall()
#for row in rows3:
#    print(row)
c.execute(query3)
rows_3_many = c.fetchmany(5)
print("The first five results for the 3rd query in SQLite are:")
print_results_5(rows_3_many)

print("----")

# mongodb
result3 = airports_collection.find({"COORDINATES": {"$gt":30, "$lt": 50}}, 
{"AIRPORT":1, "COORDINATES": 1})

#print_docs(result3)
print("The first five results for the 3rd query in MongoDB are: ")
print_results_5(result3)

print("----")

############ Flights with airlines and airports names ##################

# sqlite3
query4 = "SELECT F.ID, F.ORIGIN_AIRPORT, APT.AIRPORT, F.AIRLINE_CODE, AIR.AIRLINE \
            FROM FLIGHTS F \
            LEFT JOIN AIRPORTS APT ON (F.ORIGIN_AIRPORT = APT.IATA_CODE) \
            LEFT JOIN AIRLINES AIR ON (F.AIRLINE_CODE = AIR.AIRLINE_CODE)"
c.execute(query4)
rows4 = c.fetchall()
#for row in rows4:
#    print(row)
c.execute(query4)
rows_4_many = c.fetchmany(5)
print("The first five results for the 4th query in SQLite are:")
print_results_5(rows_4_many)

print("----")

# mongodb
result4 = flights_collection.aggregate(
    [
        {"$lookup": {
            "from": "airports",
            "localField": "IATA_CODE.ORIGIN_AIRPORT",
            "foreignField": "IATA_CODE",
            "as": "airport",
        }},
        {"$lookup": {
            "from": "airlines",
            "localField": "AIRLINE_CODE",
            "foreignField": "AIRLINE_CODE",
            "as": "airline",
        }},
        {"$project": {"IATA_CODE.ORIGIN_AIRPORT":1, "airport.AIRPORT":1, "AIRLINE_CODE":1, "airline.AIRLINE":1}}
    ]
)

#print_docs(result4)
print("The first five results for the 4th query in MongoDB are: ")
print_results_5(result4)

print("----")

###################### (origin airport, airline) with lowest average arrival delay  ################

# sqlite3
query5 = "SELECT F.ID, APT.AIRPORT, AIR.AIRLINE, AVG(ARRIVAL_DELAY) \
            FROM FLIGHTS F \
            LEFT JOIN AIRPORTS APT ON (F.ORIGIN_AIRPORT = APT.IATA_CODE) \
            LEFT JOIN AIRLINES AIR ON (F.AIRLINE_CODE = AIR.AIRLINE_CODE) \
            GROUP BY APT.AIRPORT, AIR.AIRLINE \
            ORDER BY AVG(ARRIVAL_DELAY) ASC"
c.execute(query5)
rows5 = c.fetchall()
#for row in rows5:
#    print(row)
c.execute(query5)
rows_5_many = c.fetchmany(5)

print("The first five results for the 5th query in SQLite are:")
print_results_5(rows_5_many)

print("----")

# mongodb
result5 = flights_collection.aggregate(
    [
        {"$lookup": {
            "from": "airports",
            "localField": "IATA_CODE.ORIGIN_AIRPORT",
            "foreignField": "IATA_CODE",
            "as": "airport",
        }},
        {"$lookup": {
            "from": "airlines",
            "localField": "AIRLINE_CODE",
            "foreignField": "AIRLINE_CODE",
            "as": "airline",
        }},
        {"$group": {
            "_id": {"origin airport": "$airport.AIRPORT", "airline": "$airline.AIRLINE"},
            "average delays": {"$avg": "$DELAYS.ARRIVAL_DELAY"}
        }}, 
        {"$sort": {"average delays": 1}}
    ]
)

#print_docs(result5)
print("The first five results for the 5th query in MongoDB are: ")
print_results_5(result5)

print("----")

################# update one value ##################

# sqlite3
query6 = "UPDATE AIRPORTS SET LATITUDE = 40 WHERE IATA_CODE = 'ABE'"
c.execute(query6)
rows6 = c.fetchall()
#for row in c.execute("SELECT * FROM Airports LIMIT 1"):
#    print(row)

# mongodb
result6 = {"IATA_CODE": "ABE"}
oldvalue = airports_collection.find({"IATA_CODE": "ABE"})

for x in oldvalue:
    old_longitude_value = x["COORDINATES"][0]

newvalue = {"$set": {"COORDINATES.0": old_longitude_value, "COORDINATES.1": 40}}

x = airports_collection.update_one(result6, newvalue)
#print(x.modified_count, "documents updated.")


################# insert many values ###################

# sqlite3
query7 = "INSERT INTO AIRLINES VALUES \
            ('RA', 'Ronaldo Airlines'), \
            ('BA', 'Bases de dados Airlines'), \
            ('PA', 'Portugal Airlines')"
c.execute(query7)
rows7 = c.fetchall()
# for row in c.execute("SELECT * FROM AIRLINES"):
#    print(row)

query8 = "INSERT INTO FLIGHTS \
            (DAY, DAY_OF_THE_WEEK, AIRLINE_CODE, FLIGHT_NUMBER, TAIL_NUMBER, DISTANCE, \
            ORIGIN_AIRPORT, DESTINATION_AIRPORT, DEPARTURE_DELAY, ARRIVAL_DELAY) \
            VALUES (1, 2, 'RA', 90, 'N405AS', 1000, 'ANC', 'SEA', 10, -8)"
c.execute(query8)
rows8 = c.fetchall()
#for row in c.execute("SELECT * FROM FLIGHTS"):
#    print(row)

# mongodb
fileinsert = open('airlinesinsert.json')
datainsert = json.load(fileinsert)
result7 = airlines_collection.insert_many(datainsert)
#print(f"Airlines automatic IDs: {result7.inserted_ids}")

newflight = {
      "DAY":1,
      "DAY_OF_WEEK":2,
      "AIRLINE_CODE":"RA",
      "FLIGHT_NUMBER":90,
      "TAIL_NUMBER":"N405AS",
      "DISTANCE":1000,
      "IATA_CODE":{
        "ORIGIN_AIRPORT":"ANC",
        "DESTINATION_AIRPORT":"SEA"
      },
      "DELAYS":{
        "DEPARTURE_DELAY":10.0,
        "ARRIVAL_DELAY":-8.0
      }
    }
result8 = flights_collection.insert_one(newflight)
#print(f"Flights automatic ID: {result8.inserted_id}")

# CLOSE CONNECTIONS
client.close()
conn.close()