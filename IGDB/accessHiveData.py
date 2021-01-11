from pyhive import hive
import pandas
import sys
import ssl
import thrift
import thrift_sasl
import json
from datetime import datetime as dt
import numpy as np
import matplotlib.pyplot as plt

#instantiate connection to hive via server2, using pyhive library
#use connection to read hive table and store as pandas dataframe

connection = hive.Connection(host="localhost", port=10000, username='fieldemployee')
dataframe = pandas.read_sql("SELECT * FROM igdbtables.topgames", connection)

#convert dataframe to usuable dictionary format, for processing

games = []
genreDataSet = []
dataframe.to_dict()
limit = 500 

for i in range(0, limit):
    game = {}
    game['id'] = int(dataframe['topgames.id'][i])
    game['name'] = dataframe['topgames.name'][i]
    theseGenres = dataframe['topgames.genres'][i]
    theseGenres = theseGenres.strip('[]')
    try:
        Glist = theseGenres.split(',')
        Glist2 = []
        for g in Glist:
            Glist2.append(int(g))
        game['genres'] = Glist2
    except:
        game['genres'] = int([theseGenres])
    
    game['first_release_date'] = int(dataframe['topgames.first_release_date'][i])
    game['rating'] = float(dataframe['topgames.rating'][i])
    try:
        game['hypes'] = int(dataframe['topgames.id'][i])
    except:
        game['hypes'] = 0
    try:
        game['follows'] = int(dataframe['topgames.follows'][i])
    except:
        game['follows'] = 0
    game['parent'] = dataframe['topgames.parent_game'][i]
    games.append(game)


genrekey = {}
with open("genrekey.txt", "r") as thisfile:
    for line in thisfile:
        split = line.split(',')
        genrekey[int(split[0])] = str(split[1].strip("\n"))

#iterate through genres, if game belongs to genre, conduct calculations. Append genre info to dataset.

for key in genrekey:
    data = {}
    thisgenre = genrekey[key];
    frequency = 0
    rating_sum = 0.0
    hype_sum = 0.0
    follows = 0
    for game in games:
        if key in game['genres']:
            frequency +=1
            rating_sum += game['rating']
            try:
                hype_sum += game['hypes']
            except:
                pass
            finally:
                try:
                    follows += game['follows']
                except:
                    pass

    if not frequency == 0:
        avg_rating = rating_sum/frequency
        avg_hypes = hype_sum/frequency
        data['genre'] = key
        data['name'] = thisgenre
        data['avg_rating'] = avg_rating
        data['avg_hypes'] = avg_hypes
        data['follows'] = follows
        popularity = frequency/limit
        data['frequency'] = popularity*100
        genreDataSet.append(data)

genreAxis = []
ratingAxis = []
hypeAxis = []
followAxis = []
frequencyAxis = []

for Gdict in genreDataSet:
    genreAxis.append(Gdict['name'])
    ratingAxis.append(Gdict['avg_rating'])
    hypeAxis.append(Gdict['avg_hypes'])
    followAxis.append(Gdict['follows'])
    frequencyAxis.append(Gdict['frequency'])

largest_follows = 0
for f in followAxis:
    if f > largest_follows:
        largest_follows = f

followPercentAxis = []
for follow in followAxis:
    followPercentAxis.append((follow/largest_follows)*100)

largest_hypes = 0
for h in hypeAxis:
    if h > largest_hypes:
        largest_hypes = h

hypePercentAxis = []
for hype in hypeAxis:
    hypePercentAxis.append((hype/largest_hypes)*100)

# data to plot
n_groups = len(genreAxis)

# create plot
fig, ax = plt.subplots()
index = np.arange(n_groups)
bar_width = 0.2
opacity = 0.8

rects1 = plt.bar(index, ratingAxis, bar_width, alpha=opacity, color='b', label='Rating')

rects2 = plt.bar(index + 1.1*bar_width, followPercentAxis, bar_width, alpha=opacity, color='g', label='Follows as %')

rects3 = plt.bar(index + 2.2*bar_width, hypePercentAxis, bar_width, alpha=opacity, color='r', label='Hypes as %')

rects4 = plt.bar(index + 3.3*bar_width, frequencyAxis, bar_width, alpha=opacity, color='g', label='Frequency')

plt.xlabel('Genre')
plt.ylabel('Values (out of 100)')
plt.title('Rating, follows, hypes, and frequency by genre')
plt.xticks(index + bar_width, genreAxis)
plt.legend()

plt.tight_layout()
plt.show()

