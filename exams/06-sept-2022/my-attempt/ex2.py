import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# companiesPath = "data/Companies.txt" # Useless for this program
dataCenterPath = "data/DataCenters.txt"
dailyPwrConsPath = "data/DailyPowerConsumption.txt"

outputPath1 = "outPart1/"
outputPath2 = "outPart2/"

# Define the rdds associated with the used input files
# CodDC,CodC,City,Country,Continent
dataCenterRDD = sc.textFile(dataCenterPath).cache()
# CodDC,Date,kWh
pwrConsRDD = sc.textFile(dailyPwrConsPath).cache()


# Part 1 -- Dates with high power consumption in many data centers

# let's retrieve from pwrConsRDD to (date, (countHigh, countTot))
# and then map to (date, countHigh/countTot) to have the percentage
# we then filter only on those with percentage > 90
# keep only keys, since we're interested only in dates

def dateCountsFilter(line):
    # CodDC,Date,kWh
    fields = line.split(',')
    date = fields[1]
    cons = float(fields[2])
    if cons >= 1000:
        return (date, (1, 1))
    else:
        return (date, (0, 1))
    
res1RDD = pwrConsRDD\
    .map(dateCountsFilter)\
    .reduceByKey(lambda v1, v2: (v1[0] +v2[0], v1[1] + v2[1]))\
    .mapValues(lambda v: v[0] / v[1])\
    .filter(lambda p: p[1] > 0.9)\
    .keys()

# save result
res1RDD.saveAsTextFile(outputPath1)

# Part 2 -- continent(s) with highest average power consumption per data center in the year 2021
# and the highest number of data centers

# first thing -> filter only year 2021:
pwrCons21RDD = pwrConsRDD.filter(lambda line: line.split(',')[1].startswith('2021'))

# we then proceed to compute the annual consumption for each data center:
dcConsRDD = pwrCons21RDD\
    .map(lambda line: (line.split(',')[0], line.split(',')[2]))\
    .reduceByKey(lambda v1, v2: v1 + v2)  # (codDC, consumption)

# we now extract from dataCenterRDD (codDC, continent) tuples
# we perform a join with dcConsRDD 
# join res: (codDC, (continent, consumption))
# we map this result into (continent, (consumption, 1))
# to then obtain (continent, (avg, nDC))

def codDCContintentMap(line):
    # CodDC,CodC,City,Country,Continent
    fields = line.split(',')
    codDC = fields[0]
    continent = fields[4]
    return (codDC, continent)

continentAvgNDCRDD = dataCenterRDD\
    .map(codDCContintentMap)\
    .join(dcConsRDD)\
    .map(lambda p: (p[1][0], (p[1][1], 1)))\
    .reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))\
    .map(lambda p: (p[0], (p[1][0] / p[1][1], p[1][1]))).cache()  # (continent, (avgConsumption, nDC))

# we compute max of avgConsumption and max of number of data centers
maxAvgConsumption = continentAvgNDCRDD\
    .map(lambda p: p[1][0])\
    .max()

maxNDc = continentAvgNDCRDD\
    .map(lambda p: p[1][1])\
    .max()

# final step -> select only continents whose avgConsumption = maxAvgConsumption and nDC = maxNDc
res2RDD = continentAvgNDCRDD\
    .filter(lambda p: p[1][0] == maxAvgConsumption and p[1][1] == maxNDc)\
    .keys()

# save results
res2RDD.saveAsTextFile(outputPath2)