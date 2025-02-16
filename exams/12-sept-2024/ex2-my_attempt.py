from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Exam 12 Sept 2024')
sc = SparkContext(conf = conf)

products_path = 'data/Products.txt'
prices_path = 'data/Prices.txt'
sales_path = 'data/Sales.txt'

# Sales format: product_id, date, number_sold_products
salesRDD = sc.textFile(sales_path).cache()


# PART 1 - products that decreased their total sales in 2021 with respect to sales in 2019


# filter only sales for 2019 and 2021
def filter1921(line):
    # Sales format: product_id, date, number_sold_products
    date = line.split(',')[1]
    return date.startswith('2019') or date.startswith('2021')

sales1921RDD = salesRDD.filter(filter1921)

# from this let's create pairs (product_id, (n_sales19, n_sales21))
def pidSales1921Map(line):
    # Sales format: product_id, date, number_sold_products
    fields = line.split(',')
    pid = fields[0]
    date = fields[1]
    numSales = int(fields[2])
    if date.startswith('2019'):
        return (pid, (numSales, 0))
    else:
        return (pid, (0, numSales))
    
pidNsales1921RDD = sales1921RDD.map(pidSales1921Map)\
    .reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))  # (product_id, (n_sales19, n_sales21))

res1RDD = pidNsales1921RDD.filter(lambda p: p[1][1] < p[1][0]).keys()

res1RDD.saveAsTextFile('/out1')

# PART 2 - Most sold products for each year --> (year, product_id)

# map ((year, product_id), number_sold_products)
# reduceByKey ((year, product_id), total_sold_products) yearPidTotSoldProdsRDD
# NOOOO map (product_id, total_sold_products) SBAGLIATO -> QUESTO è MAX PER PROD_ID, IO VOGLIO MAX PER YEAR
# GIUSTO : map (year, total_sold_products)
# reduceByKey (year, max(total_sold_products)) yearMaxTotSoldRDD
def yearPidSoldMap(line):
    # Sales format: product_id, date, number_sold_products
    fields = line.split(',')
    year = fields[1].split('/')[0]
    pid = fields[0]
    n_sales = fields[2]
    return((year, pid), n_sales)

yearPidSoldRDD = salesRDD.map(yearPidSoldMap).cache()  # ((year, product_id), number_sold_products)

yearMaxTotSold = yearPidSoldRDD.reduceByKey(lambda v1, v2: v1 + v2)\
    .map(lambda p: (p[0][0], p[1]))\
    .reduceByKey(lambda v1, v2: max(v1, v2))  # (year, max(total_sold_products))

# map yearPidSoldRDD to ((year, numer_sold_products), pid)
# map yearMaxTotSold to ((year, max(total_sold_products)), None)
# join them
# res: ((year, max(total_sold_products)), (pid, None))
# map (year, pid)

res2RDD = yearPidSoldRDD.map(lambda p: ((p[0][0], p[1]), p[0][1]))\
    .join(yearMaxTotSold.map(lambda p: ((p[0], p[1]), None)))\
    .map(lambda p: (p[0][0], p[1][0]))

res2RDD.saveAsTextFile('/out2')