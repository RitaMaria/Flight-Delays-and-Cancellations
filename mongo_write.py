import json
from pymongo import MongoClient

# open and load json
airlines = open('Airlines.json')
airlines_data = json.load(airlines)
flights = open('Flights.json')
flights_data = json.load(flights)
airports = open('Airports2.json')
airports_data = json.load(airports)

# print('airlines', airlines_data)
# print('flihgts', flights_data)
# print('airports', airports_data)

#connection
client = MongoClient()

#create a database named project with no index
db = client.group5_mongo_no_index

# create collections
airlines_collection = db.create_collection("airlines")
flights_collection = db.create_collection("flights")
airports_collection = db.create_collection("airports")

# insert the data into the mongoDB
airlines_result = airlines_collection.insert_many(airlines_data)
flights_result = flights_collection.insert_many(flights_data)
airports_result = airports_collection.insert_many(airports_data)

#print(f"Airlines: {airlines_result.inserted_ids}")
#print(f"Flights: {flights_result.inserted_ids}")
#print(f"Airports: {airports_result.inserted_ids}")



#create a database named project with index
db = client.group5_mongo_index

# create collections
airlines_collection = db.create_collection("airlines")
flights_collection = db.create_collection("flights")
airports_collection = db.create_collection("airports")

# insert the data into the mongoDB
airlines_result = airlines_collection.insert_many(airlines_data)
flights_result = flights_collection.insert_many(flights_data)
airports_result = airports_collection.insert_many(airports_data)

#print(f"Airlines: {airlines_result.inserted_ids}")
#print(f"Flights: {flights_result.inserted_ids}")
#print(f"Airports: {airports_result.inserted_ids}")