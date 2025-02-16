from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Exam 12 Sept 2024')
sc = SparkContext(conf = conf)

purchasePath = "data/Purchases.txt"
# usersPath = "data/Users.txt"
cataloguePath = "data/Catalogue.txt"

outputPath1 = "outPart1/"
outputPath2 = "outPart2/"

# Purchases format: sale_timestamp, user_id, item_id, sale_price
purchasesRDD = sc.textFile(purchasePath)
# Catalogue format: item_id, name, category, still_in_prod
catalogueRDD = sc.textFile(cataloguePath)

# PART 1 - Users with highest number of purchases in 2022 or 2023

# filter only purchases in 2022 and 2023
def filter2223(line):
    return (line.startswith('2022') or line.startswith('2023'))

purchases2223RDD = purchasesRDD.filter(filter2223).cache()

# create pairs (user_id, (count_22, count_23)) to count purchases is 2022 and 2023 of each user
def count2223Map(line):
    # Purchases format: sale_timestamp, user_id, item_id, sale_price
    if line.startswith('2022'):
        return (line.split(',')[1], (1, 0))
    else:
        return (line.split(',')[1], (0, 1))

# res pairs --> (user_id, (count_22, count_23))
userIdCount2223RDD = purchases2223RDD.map(count2223Map)\
    .reduceByKey(lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1]))

# find maximum for 2022 and maximum for 2023
max2223 = userIdCount2223RDD.values().reduce(lambda v1, v2: (max(v1[0], v2[0]), max[v1[1], v2[1]]))

max22 = max2223[0]
max23 = max2223[1]

# filter only users with count22 = max22 or count23 = max23
# res pairs --> (user_id, (count_22, count_23))
# we are interested only in the keys (user_id)
userIdmax22Ormax23RDD = userIdCount2223RDD.filter(lambda p: p[1][0] == max22 or p[1][1] == max23)\
    .keys()

userIdmax22Ormax23RDD.saveAsTextFile(outputPath1)


# PART 2 - for each category -> the items purchased by the largest amount of users in years 2022 and 2023

# 1 - ((item_id, user_id), #purchases) for years 2022, 2023 of course
def itemUserMap(line):
    # Purchases format: sale_timestamp, user_id, item_id, sale_price
    fields = line.split(',')
    user_id = fields[2]
    item_id = fields[1]
    return ((item_id, user_id), 1)

purchasesItemIdperUserIdRDD = purchases2223RDD.map(itemUserMap).distinct()\
    .reduceByKey(lambda v1, v2: v1 + v2)  # ((item_id, user_id), #purchases)

# 2 - retrieve categories for each item
# (item_id, category)
def itemCategory(line):
    # Catalogue format: item_id, name, category, still_in_prod
    fields = line.split(',')
    item_id = fields[0]
    category = fields[1]
    return (item_id, category)

itemCategoryRDD = catalogueRDD.map(itemCategory).cache()

# 3 - ((item_id, user_id), #purchases) -> map to (item_id, #purchasesPerUser)
# join (item_id, category) with (item_id, #purchasesPerUser)
# res : (item_id, (category, #purchasesPerUser))

itemIdCategoryPurchasesPerUser = purchasesItemIdperUserIdRDD.map(lambda p: (p[0][0], p[1]))\
    .join(itemCategoryRDD).cache()

# 4 - find starting from (category, #purchasesPerUser) -> (category, max#purchasesPerUser)
categoryMaxPurchasePerUserRDD = itemIdCategoryPurchasesPerUser.map(lambda p: (p[1][0], p[1][1]))\
    .reduceByKey(lambda v1, v2: max(v1, v2))  # (category, max#purchasesPerUser)

# 5 - map itemIdCategoryPurchasesPerUser -- (item_id, (category, #purchasesPerUser)) into (category, item_id)
# join with categoryMaxPurchasePerUserRDD -- (category, max#purchasesPerUser)
# res: (category, (item_id, max#purchasesPerUser))
# map to (category, item_id)

res2partialRDD = itemIdCategoryPurchasesPerUser.map(lambda p: (p[1][0], p[0]))\
    .join(categoryMaxPurchasePerUserRDD)\
    .map(lambda p: (p[0], p[1][0]))

# 6 - retrieve category with no purchases
# from itemCategoryRDD.values() subtract res2partialRDD.keys()
# res: (category) of categories with no purchases
# map to (category, "No purchase")

categoryNoPurchasesRDD = itemCategoryRDD.values.subtract(res2partialRDD.keys())\
    .map(lambda cat: (cat, "NoPurchase"))

res2RDD = res2partialRDD.union(categoryNoPurchasesRDD)

res2RDD.saveAsTextFile(outputPath2)



