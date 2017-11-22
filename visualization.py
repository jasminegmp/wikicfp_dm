# Jasmine Kim - Global conference data visualization
# I used three different libraries to help me to do the visualization
# 1. Pandas (to utilize Data Frame to organize my data)
# 2. Geopy (to get actual Longitutde and Latitude of geographical locations of my conference locations)
# 3. BaseMap (to get the neat global map)
# I referenced the following blogs to help me understand how to use these:
# https://geopy.readthedocs.io/en/1.10.0/
# https://peak5390.wordpress.com/2012/12/08/matplotlib-basemap-tutorial-plotting-points-on-a-simple-map/

import mpl_toolkits
import string
from pandas import DataFrame
from matplotlib import pyplot as plt
from geopy.geocoders import Nominatim
from mpl_toolkits.basemap import Basemap
import pandas as pd

# This is to later use to get actual geographical long/lat
geolocator = Nominatim()

# Read in TSV file and store into pandas data frame
# Referenced some knowledge from here: 
# https://stackoverflow.com/questions/37553298/pandas-column-name-assignment-read-csv
column = ['loc','year', 'freq']
df = pd.read_csv("results_4.txt", sep='\t', names = column)

# Sort data frame by year
result =  df.sort_values(by ='year', ascending =True)
curr_yr = "2011"

curr_yr = 2011
year = []
locations = []

# Go through each row in data frame, and placing it in a year list
# So the data is sorted by year where each location is calculated a long/lat using geopy
# Also it is given a size and color to later plot
for index, row in result.iterrows():
	print row['loc']
 	if row['freq'] > 5:
		color = 'ro'
		size = 8
	elif row['freq'] > 1:
		color = 'co'
		size = 5
	else:
		color = 'bo'
		size = 3
	printable = set(string.printable)
	# Was having some issues with non-ascii character values
	# Referenced this:
	# https://stackoverflow.com/questions/8689795/how-can-i-remove-non-ascii-characters-but-leave-periods-and-spaces-using-python
	y = filter(lambda x: x in printable, str(row['loc']))
	location = geolocator.geocode(y, timeout=30)

	if location != None:
		locations.append([location.latitude, location.longitude, color, size])

	if row['year'] != curr_yr:
		print row['year']
		year.append(locations)
		curr_yr = row['year']
		locations = []
year.append(locations)

count = 0;
yr_count = 2011;

# Now go through each year plotting using basemap
for eachyr in year:
	fig = plt.figure()
	map = Basemap()
	map.drawcoastlines()
	map.drawcountries()
	for location in eachyr:
		x,y = map(location[1],location[0])
		map.plot(x, y, location[2], markersize=location[3], alpha = 0.5)
		count += 1;
	plt.title(str(yr_count))


	name_fig = str(yr_count) + ".png"
	plt.savefig(name_fig)
	yr_count += 1

