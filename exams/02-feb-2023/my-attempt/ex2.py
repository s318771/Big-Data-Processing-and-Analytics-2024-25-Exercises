import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# houseId, city, country, sizeSQM
housesPath = "exam_ex2_data/houses.txt"
# houseId, date, kWh
consumptionPath = "exam_ex2_data/daylyPowerConsumption.txt"

outputPath1 = "/out1"
outputPath2 = "/out2"

housesRDD = sc.textFile(housesPath).cache()
consumptionsRDD = sc.textFile(consumptionPath).cache()

# Part 1

# filter only consumptions in 2022
consumptions22RDD = consumptionsRDD.filter(lambda line: line.split(',')[1].starswith('2022'))

# we have to calculate the average daily consumption
# fistly we map to (houseId, (sum, count))
# and we can obtain (houseId, avg = sum/count)

def houseSumCountMap(line):
    # input format houseId, date, kWh
    fields = line.split(',')
    houseId = fields[0]
    consumption = float(fields[2])
    return (houseId, (consumption, 0))

houseAverageConsumptionRDD = consumptions22RDD.map(houseSumCountMap)\
    .reduceByKey(lambda v1, v2: (v1[0]+v2[0], v1[1] + v2[1]))\
    .map(lambda p: (p[0], p[1][0] / p[1][1]))  # (houseId, avgConsumption)

# we find houses with high consumption in 2022
highConsumptionRDD = houseAverageConsumptionRDD.filter(lambda p: p[1] > 30)  # (houseId, highAvgConsumption)

# let's retrieve (houseId, country) from housesRDD
def houseCountryMap(line):
    # houseId, city, country, sizeSQM
    fields = line.split(',')
    houseId = fields[0]
    country = fields[2]
    return (houseId, country)

houseCountryRDD = housesRDD.map(houseCountryMap).cache()  # (houseId, country)

# keep an RDD with coutries with at least one high consumption house
# to do so -> join houseCountryRDD with highConsumptionRDD
# join result: (houseId, (country, avg))
# map to country

highConsumptionCountries = houseCountryRDD.join(highConsumptionRDD)\
    .map(lambda p: p[1][0])  # country with high consumption

# retrieve an RDD with all the countries and subtract those with a high consumption
# then remove duplicates and we're done

res1RDD = houseCountryRDD.map(lambda p: p[1])\
    .subtract(highConsumptionCountries)\
    .distinct()

res1RDD.saveAsTextFile(outputPath1)

# Part 2

#only consumption in 2021
consumptions21RDD = consumptionsRDD.filter(lambda line: line.split(',')[1].starswith('2021'))

# for each country -> number of cities with a high annual consumption
# (houseId, high_annual_consumption)

def houseIdConsumptionMap(line):
    # houseId, date, kWh
    fields = line.split(',')
    houseId = fields[0]
    counsumption = fields[2]
    return (houseId, counsumption)

annualConsumptionsRDD = consumptions21RDD.map(houseIdConsumptionMap)\
    .reduceByKey(lambda v1, v2: v1 + v2)\
    .filter(lambda p: p[1] > 10000)  # houseId, highAnnualConsumption

# let's retrieve (houseId, (country, city)) from housesRDD
# join with annualConsumptionsRDD and obtain (houseId, ((country, city), high_annual_consumption))
# map to (country, 1)
# (country, n_cities) -- if n_cities > 500 -> (country, n_cities) else (country, 0)
def houseIdCountryCityMap(line):
    # houseId, city, country, sizeSQM
    fields = line.split(',')
    houseId = fields[0]
    country = fields[2]
    city = fields[1]
    return (houseId, (country, city))

countryNHighConsumptionCitiesRDD = housesRDD.map(houseIdCountryCityMap)\
    .join(annualConsumptionsRDD)\
    .map(lambda p: (p[1][0][0], 1))\
    .reduceByKey(lambda v1, v2: v1 + v2)  # (country, n_cities) with high annual consumption

## PROBLEMA DI QUESTA SOLUZIONE:
# Se c'è qualche paese che non ha mai  avuto una high annual consumption non viene preso in cosiderazione 
# e non ci sarà nella soluzione finale: not good.

def resultFilter(p):
    country = p[0]
    n_cities = p[1]
    if n_cities > 50:
        return (country, n_cities)
    else:
        return (country, 0)

res2RDD = countryNHighConsumptionCitiesRDD.filter(resultFilter)

res2RDD.saveAsTextFile(outputPath2)