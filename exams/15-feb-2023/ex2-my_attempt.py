from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Exam 15 feb 2023')
sc = SparkContext(conf = conf)

housePath = "data/Houses.txt"
consumptionPath = "data/MonthlyWaterConsumption.txt"

outputPath1 = "outPart1/"
outputPath2 = "outPart2/"

# retrieve input RDDs
# Houses format: hid, city, country, year_built
housesRDD = sc.textFile(housePath)
# Consumption format: hid, month, m3
consumptionsRDD = sc.textFile(consumptionPath)

# PART 1 - houses with increasing consumption in at least 3 trimesters of the year 2022
# compared with the corresponding trimesters of year 2021
# output format "hid, City"

# filter only years 2021 and 2022
def filter2122(line):
    # Consumption format: hid, month, m3
    fields = line.split(',')
    month = fields[1]
    return month.startswith('2021') or month.startswith('2022')

consumptions2122 = consumptionsRDD.filter(filter2122)  # hid, month, m3

# map to ((hid, trimester_id, year), water_consumption)
# reduceByKey and obtain ((hid, trimester_id, year), total_water_consumption)
def hidTrimesterM3Map(line):
    # hid, month, m3
    fields = line.split(',')
    hid = fields[0]
    year_month = fields[1].split('/')
    year = int(year_month[1])
    trimester_id = (int(year_month[2]) - 1) // 3 + 1
    water_consumption = float(fields[2])
    return ((hid, trimester_id, year), water_consumption)

hidTrimesterYearTotM3RDD = consumptions2122.map(hidTrimesterM3Map)\
    .reduceByKey(lambda v1, v2: v1 + v2)  # ((hid, trimester_id, year), total_water_consumption)

# obtain ((hid, trimester), (consumption_21, consumption_22))
def consumption2122(tuple):
    # ((hid, trimester_id, year), total_water_consumption)
    hid = tuple[0][0]
    trimester = tuple[0][1]
    year = tuple[0][2]
    water_consumption = tuple[1]
    if year == 2021:
        return ((hid, trimester), (water_consumption, 0.))
    else:
        return((hid, trimester), (0., water_consumption))

hidTrimesterConsumption2122RDD = hidTrimesterYearTotM3RDD.map(consumption2122)\
    .reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))  # ((hid, trimester), (consumption_21, consumption_22))

hosesWithIncreasingConsumptionRDD = hidTrimesterConsumption2122RDD.filter(lambda p: p[1][1] > p[1][0])\
    .map(lambda p: (p[0], 1))\
    .reduceByKey(lambda v1, v2: v1 + v2)\
    .filter(lambda p: p[1] >= 3)  # (hid, count)

# from housesRDD map (hid, city)
def hidCityMap(line):
    # Houses format: hid, city, country, year_built
    fields = line.split(',')
    hid = fields[0]
    city = fields[1]
    return (hid, city)

hidCityRDD = housesRDD.map(hidCityMap)

# join hidCityRDD with hosesWithIncreasingConsumptionRDD
# result (hid, (city, count))
# map to string "hid, city"
res1RDD = hidCityRDD.join(hosesWithIncreasingConsumptionRDD)\
    .map(lambda p: f"{p[0], p[1][0]}")

res1RDD.saveAsTextFile(outputPath1)