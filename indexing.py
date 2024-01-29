import time
import sqlite3
from pymongo import MongoClient
import numpy as np
import matplotlib.pyplot as plt
plt.style.use('ggplot')

################### Pymongo ###############

# connection 1
client1 = MongoClient()
# open the database with no index
db1 = client1.group5_mongo_no_index
# collections
airlines_collection_1 = db1['airlines']
airports_collection_1 = db1['airports']
flights_collection_1 = db1['flights']


# connection 2
client2 = MongoClient()
# open the database with index
db2 = client2.group5_mongo_index
# collections
airlines_collection_2 = db2['airlines']
airports_collection_2 = db2['airports']
flights_collection_2 = db2['flights']


############### CREATION OF INDEXS ########### 
"""

# create hash index on flight_number
resp = flights_collection_2.create_index([("FLIGHT_NUMBER", 'hashed')])
print("index response:", resp)

# create a compound text index on airport and city
resp2 = airports_collection_2.create_index([("AIRPORT", 'text'), ("CITY", 'text')])
print("index response:", resp2)

# create a geospatial index for coordinates
resp3 = airports_collection_2.create_index([("COORDINATES", '2dsphere')])
print("index response:", resp3)
"""
############# Performance testing #############

labels = ['Query1', 'Query2', 'Query3']
no_index_times = []
index_times = []

time_i = time.time()
query = flights_collection_1.find({"FLIGHT_NUMBER": {"$gt":7000}}, {"FLIGHT_NUMBER":1})
time_f = time.time()
no_index_times.append(time_f-time_i)
print('time expended with no index', time_f-time_i)

time_i = time.time()
query = flights_collection_2.find({"FLIGHT_NUMBER": {"$gt":7000}}, {"FLIGHT_NUMBER":1})
time_f = time.time()
index_times.append(time_f-time_i)
print('time expended with index', time_f-time_i)

time_i = time.time()
query = airports_collection_1.find({}, {"AIRPORT": 1, "CITY": 1})
time_f = time.time()
no_index_times.append(time_f-time_i)
print('time expended with no index', time_f-time_i)

time_i = time.time()
query = airports_collection_2.find({}, {"AIRPORT": 1, "CITY": 1})
time_f = time.time()
index_times.append(time_f-time_i)
print('time expended with index', time_f-time_i)

time_i = time.time()
query = airports_collection_1.find({"COORDINATES.LATITUDE": {"$gt": 30, "$lt": 50}}, 
{"AIRPORT":1, "COORDINATES.LATITUDE": 1, "COORDINATES.LONGITUDE": 1})
time_f = time.time()
no_index_times.append(time_f-time_i)
print('time expended with no index', time_f-time_i)

time_i = time.time()
query = airports_collection_2.find({"COORDINATES.LATITUDE": {"$gt": 30, "$lt": 50}}, 
{"AIRPORT":1, "COORDINATES.LATITUDE": 1, "COORDINATES.LONGITUDE": 1})
time_f = time.time()
index_times.append(time_f-time_i)
print('time expended with index', time_f-time_i)

# plot

x = np.arange(len(labels))
width = 0.35

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, no_index_times, width, label = 'No index')
rects2 = ax.bar(x + width/2, index_times, width, label = 'Index')

ax.set_ylabel('Time (seconds)')
ax.set_title('Comparison of execution times in MongoDB with and without indexes')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding = 3)
ax.bar_label(rects2, padding = 3)

# plt.show()

client1.close()
client2.close()

################### SQLite ###############

# connection 1 (database with no index)
conn1 = sqlite3.connect('group5_sql_no_index.db')
c1 = conn1.cursor()

# connection 2 (databse with index)
conn2 = sqlite3.connect('group5_sql_index.db')
c2 = conn2.cursor()

############### CREATION OF INDEXS ########### 
"""
# create index on flight_number
createIndex = "CREATE INDEX index_flights ON FLIGHTS(FLIGHT_NUMBER)"
c2.execute(createIndex)

# create a compound text index on airport and city
multi_col = "CREATE INDEX air_city ON AIRPORTS (AIRPORT, CITY);"
c2.execute(multi_col)

# create a compound index for coordinates
multi_col = "CREATE INDEX COORDINATES ON AIRPORTS (LATITUDE, LONGITUDE);"
c2.execute(multi_col)
"""

############# Performance testing #############

labels = ['Query1', 'Query2', 'Query3']
no_index_times = []
index_times = []

query1 = "SELECT FLIGHT_NUMBER FROM Flights WHERE FLIGHT_NUMBER > 7000"
query2 = "SELECT AIRPORT, CITY FROM Airports"
query3 = "SELECT AIRPORT, LATITUDE, LONGITUDE FROM Airports WHERE LATITUDE BETWEEN 30 AND 50"

time_i = time.time()
c1.execute(query1)
records_query1 = c1.fetchall()
time_f = time.time()
no_index_times.append(time_f-time_i)
print('time with no index: ', time_f-time_i)

time_i = time.time()
c2.execute(query1)
records_query2 = c2.fetchall()
time_f = time.time()
index_times.append(time_f-time_i)
print('time with index: ', time_f-time_i)

time_i = time.time()
c1.execute(query2)
records_query1 = c1.fetchall()
time_f = time.time()
no_index_times.append(time_f-time_i)
print('time with no index: ', time_f-time_i)

time_i = time.time()
c2.execute(query2)
records_query2 = c2.fetchall()
time_f = time.time()
index_times.append(time_f-time_i)
print('time with index: ', time_f-time_i)

time_i = time.time()
c1.execute(query3)
records_query1 = c1.fetchall()
time_f = time.time()
no_index_times.append(time_f-time_i)
print('time with no index: ', time_f-time_i)

time_i = time.time()
c2.execute(query3)
records_query2 = c2.fetchall()
time_f = time.time()
index_times.append(time_f-time_i)
print('time with index: ', time_f-time_i)

# plot

x = np.arange(len(labels))
width = 0.35

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, no_index_times, width, label = 'No index')
rects2 = ax.bar(x + width/2, index_times, width, label = 'Index')

ax.set_ylabel('Time (seconds)')
ax.set_title('Comparison of execution times in SQLite with and without indexes')
ax.set_xticks(x, labels)
ax.legend()

ax.bar_label(rects1, padding = 3)
ax.bar_label(rects2, padding = 3)

plt.show()

conn1.close()
conn2.close()