import pandas as pd

############# AIRLINES #############

# read the dataset into a pandas dataframe
df_airlines = pd.read_csv("airlines.csv")

# rename column to correspond to schema 
df_airlines.rename(columns = {"IATA_CODE":"AIRLINE_CODE"}, inplace = True)

# write to a json file
df_airlines.to_json("Airlines.json",
orient = "records", date_format = "epoch", 
double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

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

# create nested dict of airports
df_flights["IATA_CODE"] = df_flights[["ORIGIN_AIRPORT", "DESTINATION_AIRPORT"]].apply(
    lambda s: s.to_dict(), axis = 1
)

# crete nested dict of delays
df_flights["DELAYS"] = df_flights[["DEPARTURE_DELAY", "ARRIVAL_DELAY"]].apply(
    lambda s: s.to_dict(), axis = 1
)

# write to a json file
df_flights.drop(["ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DEPARTURE_DELAY", "ARRIVAL_DELAY"], axis = 1).to_json("Flights.json",
orient = "records", date_format = "epoch", 
double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

"""

############# AIRPORTS #############

# read the dataset into a pandas dataframe
df_airports = pd.read_csv("airports.csv")

# create nested dict of coordinates
df_airports["COORDINATES"] = df_airports[["LONGITUDE", "LATITUDE"]].apply(
    lambda s: s.to_dict(), axis = 1
)

# write to a json file      
df_airports.drop(["LONGITUDE", "LATITUDE"], axis = 1).to_json("Airports.json",
orient = "records", date_format = "epoch", 
double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

"""

########### AIRPORTS MODIFIED ###########

# read the dataset into a pandas dataframe
df_airports2 = pd.read_csv("airports.csv")

# choose rows where latitude and longitude aren't null
df_airports2 = df_airports2[df_airports2["LONGITUDE"].notna()]
df_airports2 = df_airports2[df_airports2["LATITUDE"].notna()]

# create nested list of coordinates
df_airports2["COORDINATES"] = df_airports2[["LONGITUDE", "LATITUDE"]].apply(
    lambda s: s.to_list(), axis = 1
)

# write to a json file
df_airports2.drop(["LONGITUDE", "LATITUDE"], axis = 1).to_json("Airports2.json",
orient = "records", date_format = "epoch", 
double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)