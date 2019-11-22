import pandas as pd
import numpy as np
import datetime


cancellationFiles = ["data/Delay&Cancellation/" + x for x in ["2009.csv",
															  "2010.csv",
															  "2011.csv",
															  "2012.csv",
															  "2013.csv",
															  "2014.csv",
															  "2015.csv",
															  "2016.csv",
															  "2017.csv",
															  "2018.csv"]]
weatherFile = "data/Weather/weather_description.csv"

# cancellationFiles = ["data/Delay&Cancellation/" + x for x in ["2017.csv"]]

def topDelayAirlines(verbose=False):
	'''
	Provide the top 3 airlines with an average difference in expected arrival and actual arrival and their standard deviation

	Input: 
		None
	Output:
		dict: Mapping airline to stddev of arrival delay

	Approach:
		Keep track of arrival differences through a dict; transform to dataframe and get metrics
	'''

	airline_dict = {}
	for filename in cancellationFiles:
		if verbose: print("Reading data from " + filename)
		chunk_read = pd.read_csv(filename, chunksize=1000000)
		for chunk in chunk_read:
			# For each airline: get a dataframe, append delay in dict 
			for airline in chunk['OP_CARRIER'].unique():
				if airline not in airline_dict:
					airline_dict[airline] = []

				#Extract Arrival delay from respective airline
				airline_dict[airline].extend(chunk[chunk['OP_CARRIER'] == airline]['ARR_DELAY'].dropna().values)

	#Convert lists to pandas Series so we don't run into pandas issues when casting
	for airline in airline_dict:
		airline_dict[airline] = pd.Series(airline_dict[airline])
	delay_df = pd.DataFrame(airline_dict)

	#Get mean & stddev of airlines
	results_df = pd.DataFrame()
	results_df['mean'] = delay_df.mean(axis=0)
	results_df['stddev'] = delay_df.std(axis=0)

	# Return top 3 based on mean
	return results_df.sort_values(by="mean", ascending=False).head(3)

	'''
		mean     stddev
	VX  11.149536  42.773900
	B6  10.977184  51.785661
	EV   8.027348  62.579603
	'''



def averageRainDelay(city, airport, verbose=False):
	'''
	Given a city and airport code provide the average delay time on days where there is any type of rain
	
	Input: 
		city: str
		airport: str

	Output:
		averageRainDelay: float

	Approach: 
		Get days that rained in a city, filter airline data by these days/airport, extract delay data
	'''
	
	#Load our data, in particular the weather for our city
	weather_df = pd.read_csv(weatherFile)
	
	try:
		city_df = weather_df[['datetime',city]]
	except:
		print("No city found!")
		return

	#Clean our data somewhat
	city_df = city_df.dropna()

	# Limit to just rainy days
	city_df = city_df[city_df[city].str.contains("rain")]

	# Get the unique days where it rained
	city_df['datetime'] = pd.to_datetime(city_df['datetime'])
	city_df['datetime'] = city_df['datetime'].dt.date
	rainy_days = city_df['datetime'].unique()

	# Go through our data and keep relevant data

	rainy_delays = pd.DataFrame()

	for filename in cancellationFiles:
		if verbose: print("Reading data from " + filename)
		chunk_read = pd.read_csv(filename, chunksize=1000000)
		for chunk in chunk_read:

			#Filter based on airport code
			data_df = chunk[chunk['ORIGIN'] == airport]

			#Format datetime correctly
			data_df['FL_DATE'] = pd.to_datetime(data_df['FL_DATE'])

			#Filter based on days
			data_df = data_df[data_df['FL_DATE'].isin(rainy_days)]

			#Collect relevant delay data
			rainy_delays = pd.concat([rainy_delays, data_df['DEP_DELAY'].dropna()], ignore_index=True)
			

	return rainy_delays[0].mean()

	# Atlanta, ATL: 12.51768244458


def worstTravelDays(city, airport, verbose=False):
	'''
	Given a city and airport code what are that aiport's worst days to travel
	Note: Interpreting 'worst days to travel' as largest delay time
		  Interpreting 'days' as days of the week

	Input:
		city: str
		airport: str

	Output:
		Sorted list of days of the week and highest average delay

	Approach: 
		Keep structure to track delays of each day of week, extract metrics after all data has been tracked
	'''

	# Create storage for delay times for days of the week
	day_dfs = [pd.DataFrame() for x in range(7)]

	# Load our data
	for filename in cancellationFiles:
		if verbose: print("Reading data from " + filename)
		chunk_read = pd.read_csv(filename, chunksize=1000000)
		for chunk in chunk_read:

			#Filter based on airport code
			data_df = chunk[chunk['ORIGIN'] == airport]

			#Format datetime as days of the week
			data_df['FL_DATE'] = pd.to_datetime(data_df['FL_DATE'])
			data_df['FL_DATE'] = data_df['FL_DATE'].dt.weekday

			#Add the relevant info for our respective weekdays
			for day in [0,1,2,3,4,5,6]:
				day_df = data_df[data_df['FL_DATE'] == day]['DEP_DELAY']
				day_dfs[day] = pd.concat([day_dfs[day], day_df.dropna()], ignore_index=True)

	day_delays = [x.mean() for x in day_dfs]
	return np.argmax(day_delays), day_delays
	'''
	Atlanta, ATL:
		0
		[   11.012215
		    8.355434
		    8.745895
		    10.206612
		    10.935639
		    7.176773
		    9.189768
		]
	'''
			

def cancellationProbability(city, airport, verbose=False):
	'''
	Given a city and airport code create a function that provides a probability of a flight getting cancelled

	Input:
		city: str
		airport: str

	Output:
		float: probability of flight getting cancelled

	Approach: 
		Filter flight data on airport, calculate ratio of cancelled to total flights
	'''

	# Calculate the ratio of cancelled to uncancelled flights for a certain airport
	cancelled_flights, total_flights = 0, 0

	for filename in cancellationFiles:
		if verbose: print("Reading data from " + filename)
		chunk_read = pd.read_csv(filename, chunksize=1000000)
		for chunk in chunk_read:

			#Filter based on airport code
			data_df = chunk[chunk['ORIGIN'] == airport]

			#Drop data that we shouldn't be counting
			data_df = data_df.dropna(subset=['CANCELLED'])

			#Get number of cancelled and valid
			cancelled_flights += len(data_df[data_df['CANCELLED'] == 1.0])
			total_flights += len(data_df)

	return float(cancelled_flights/total_flights)

	# Atlanta, ATL: 0.011344666128994242


def delayProbability(city, airport, weather_desc, verbose = False):
	'''
	Given an airport code, city and weather description create a function to predict the probability of departure
	flight delay from the airport
	Input:
		city: str
		airport: str
		weather_desc: str

	Output:
		float: probability of flight getting delayed

	Approach:
		Get all days that experienced weather, filter flight data by these dates/airport, keep count of delayed flights
	'''

	delayed_flights, total_flights = 0, 0

	# Load our data
	weather_df = pd.read_csv(weatherFile)
	
	try:
		city_df = weather_df[['datetime', city]]
	except:
		print("No city found!")
		return

	# Basic Data Cleaning
	city_df = city_df.dropna()

	# Get all days where city experienced weather_desc
	city_df = city_df[city_df[city].str.contains(weather_desc)]
	city_df['datetime'] = pd.to_datetime(city_df['datetime'])
	city_df['datetime'] = city_df['datetime'].dt.date
	weather_days = city_df['datetime'].unique()

	# Go through our data, filter by weather_days, and keep count of delayed vs. non-delayed flights
	for filename in cancellationFiles:
		if verbose: print("Reading data from " + filename)
		chunk_read = pd.read_csv(filename, chunksize=1000000)
		for chunk in chunk_read:

			#Filter based on airport code
			data_df = chunk[chunk['ORIGIN'] == airport]

			#Format datetime correctly
			data_df['FL_DATE'] = pd.to_datetime(data_df['FL_DATE'])

			#Filter based on weather
			data_df = data_df[data_df['FL_DATE'].isin(weather_days)]

			#Get number of delayed
			delayed_flights += len(data_df[data_df['DEP_DELAY'] > 0.0])
			total_flights += len(data_df)

	return delayed_flights / total_flights

	#Atlanta, ATL, "sky is clear": 0.35335226279868004


if __name__ == "__main__":
	# print(averageRainDelay("Atlanta", "ATL", verbose=True))
	# print(worstTravelDays("Atlanta", "ATL", verbose=True))
	# print(cancellationProbability("Atlanta", "ATL", verbose=True))
	# print(delayProbability("Atlanta", "ATL", "sky is clear", verbose=True))
	# print(topDelayAirlines(verbose=True))
	pass
