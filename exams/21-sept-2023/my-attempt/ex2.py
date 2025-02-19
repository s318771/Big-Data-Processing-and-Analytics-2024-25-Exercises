import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

custWatchedPath = "data/CustomerWatched.txt"
espisodesPath = "data/Episodes.txt"
tvSeriesPath = "data/TVSeries.txt"

# Useless for this program
# customersPath = "data/Customers.txt"

outputPath1 = "outPart1/"
outputPath2 = "outPart2/"

# cid, startTimestamp, sid, seasonN, epN
custWatchedRDD = sc.textFile(custWatchedPath)
# sid, seasonN, epN, title, OriginalAirDate
espisodesRDD = sc.textFile(espisodesPath).cache()
# sid, title, genre
tvSeriesRDD = sc.textFile(tvSeriesPath)

# Part 1 - Average number of episodes per season for each comedy TV series

# we start by filtering only comedy tv series
comedyTvSeriesRDD = tvSeriesRDD.filter(lambda line: line.split(',')[2] == 'Comedy')\
    .map(lambda line: (line.split(',')[0], None))  # (sid, None)

# from espisodesRDD we extract ((sid, seasonN), 1)
# and we compute ((sid, seasonN), nEpisodes) --> the number of episodes of each season of sid
# we proceed by mapping (sid, (nEpisodes, 1)) to keep track of the number of seasons
# we compute the total number of episodes for every series sid and the number of seasons
# what we obtain: 
# (sid, (tot_nEpisodes, nSeasons))
# we map to (sid, avgEpisodesPerSeason)
def sidSeasonOneMap(line):
    # input: sid, seasonN, epN, title, OriginalAirDate
    # output: ((sid, seasonN), 1)
    fields = line.split(',')
    sid = fields[0]
    seasonN = fields[1]
    return ((sid, seasonN), 1)

sidAvgEpsPerSeasonRDD = espisodesRDD\
    .map(sidSeasonOneMap)\
    .reduceByKey(lambda v1, v2: v1 + v2)\
    .map(lambda p: (p[0][0], (p[1], 1)))\
    .reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))\
    .mapValues(lambda v: v[0] / v[1])  # (sid, avgEpisodesPerSeason)

# last step: keep only comedy series
# in order to do so -> join between sidAvgEpsPerSeasonRDD and comedyTvSeriesRDD
# join result: (sid, (avgEpisodesPerSeason, None))
# final map: (sid, avgEpisodesPerSeason)
res1RDD = sidAvgEpsPerSeasonRDD.join(comedyTvSeriesRDD)\
    .map(lambda p: (p[0], p[1][0]))

# save result part 1
res1RDD.saveAsTextFile(outputPath1)

# Part 2 - for each sid --> customers who watched at leas 1 episode of each season

# from episodesRDD i'm getting pairs (sid, 1) to count the number of seasons for each serie:
# result (sid, nSeries)
seriesPerSeasonRDD = espisodesRDD\
    .map(lambda line: (line.split(',')[0], 1))\
    .reduceByKey(lambda v1, v2: v1 + v2)  # (sid, nSeries)

# from custWatchedRDD i get (cid, sid, seasonN) and take only distinct elements from this
# then i map to ((cid, sid), 1) to count the number of distinct seasons for which he watched at least one episode
# result here: ((cid, sid), nSeasonsC)
# then I map this to (sid, (cid, nSeasonsC))
# now I can join with seriesPerSeasonRDD
# join result: (sid, ((cid, nSeasonsC), nSeasons))
# I proceed filtering only pairs where nSeasonC == nSeasons
# and map to final output format (sid, cid)
def cidSidSeasonNMap(line):
    # cid, startTimestamp, sid, seasonN, epN
    # wanted output ((cid, sid, seasonN), None)
    fields = line.split(',')
    cid = fields[0]
    sid = fields[1]
    seasonN = fields[2]
    return((cid, sid, seasonN), None)

res2RDD = custWatchedRDD.map(cidSidSeasonNMap)\
    .distinct()\
    .map(lambda p: ((p[0][0], p[0][1]), 1))\
    .reduceByKey(lambda v1, v2: v1 + v2)\
    .map (lambda p: (p[0][1], (p[0][0], p[1])))\
    .join(seriesPerSeasonRDD)\
    .filter(lambda p: p[1][0][1] == p[1][1])\
    .map(lambda p: (p[0], p[0][0][0]))

res2RDD.saveAsTextFile(outputPath2)