from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Exam 20 june 2024')
sc = SparkContext(conf = conf)

jobContractsPath = "sample_data/JobContracts.txt"
jobOffersPath = "sample_data/JobOffers.txt"
jobPostingsPath = "sample_data/JobPostings.txt"

outputPath1 = "outSpark1/"
outputPath2 = "outSpark2/"

# Job Postings format: job_id, job_title, country
jobPostingsRDD = sc.textFile(jobPostingsPath)
# Job Offers format: offer_id, job_id, salary, status, SSN
jobOffersRDD = sc.textFile(jobOffersPath)
# Job Contracts format: contract_id, offer_id, contract_date, contract_type
jobContractsRDD = sc.textFile(jobContractsPath)


# PART 1 - Top 3 countries with the highest average salary
# for each country -> compute avg_sal considering only accepted job offers
# select top 3 countries
# final output -> (country, avg_sal)

# let's retrieve (job_id, (country, job_title)) from jobPostingsRDD
def jobIdCountryTitleMap(line):
    # Job Postings format: job_id, job_title, country
    fields = line.split(',')
    job_id = fields[0]
    job_title = fields[1]
    country = fields[2]
    return (job_id, (country, job_title))
jobIdCountryTitleRDD = jobPostingsRDD.map(jobIdCountryTitleMap)  # (job_id, (country, job_title))

# filter to obtain only accepted offers:
acceptedOffersRDD = jobOffersRDD.filter(lambda line: line.find('Accepted') >= 0)

# retrieve (job_id, (salary, offer_id)) from the acceptedOffersRDD
def acceptedOffersMap(line):
    # Job Offers format: offer_id, job_id, salary, status, SSN
    fields = line.split(',')
    job_id = fields[1]
    salary = float(fields[2])
    offer_id = fields[0]
    return (job_id, (salary, offer_id))

jobIdSalaryOfferIdRDD = acceptedOffersRDD.map(acceptedOffersMap)  # (job_id, (salary, offer_id))

# join between jobIdCountryTitleRDD and jobIdSalaryOfferIdRDD
# res:  (job_id, ((country, job_title), (salary, offer_id)))
jobIdCountryTitleSalaryOfferIdRDD = jobIdCountryTitleRDD.join(jobIdSalaryOfferIdRDD).cache() # JOIN

# map (country, (salary, 1))
# reduceByKey (country, (sum_sal, n_sal))
# map (country, sum_sal / n_sal)
countryAvgSalRDD = jobIdCountryTitleSalaryOfferIdRDD.map(lambda pair: (pair[1][0][0], (pair[1][1][0])))\
    .reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))\
    .map(lambda p: (p[0], p[1][0] / p[1][1]))

res1 = countryAvgSalRDD.top(3, key=lambda p: p[1])

res1RDD = sc.parallelize(res1)

res1RDD.saveAsTextFile(outputPath1)



# PART 2 - Select most popular job title in each country among job postings that resulted in job contracts
# i.e. the job title with the highest number of job contracts in a given country
# final output: (country, most_common_title, contract_count)

# retrieve (offer_id, contract_id) from jobContractsRDD
# Job Contracts format: contract_id, offer_id, contract_date, contract_type
offerIdContractId = jobContractsRDD.map(lambda line: (line.split(',')[1], line.split(',')[0]))

# from jobIdCountryTitleSalaryOfferIdRDD:
# map: (offer_id, (country, job_title))
offerIdTitleCountryRDD = jobIdCountryTitleSalaryOfferIdRDD.map(lambda pair: (pair[1][1][1], (pair[1][0][0], pair[1][0][1])))

# join with offerIdContractId:
# res: (offer_id, ((country, title), contract_id))
# map ((country, title), 1)
# reduceByKey ((country, title), n_contracts)
countryTitleContractsCountRDD = offerIdTitleCountryRDD.join(offerIdContractId)\
    .map(lambda pair: (((pair[1][0][0]), pair[1][0][1]), 1))\
    .reduceByKey(lambda v1, v2: v1 + v2).cache()

# retrieve from countryTitleContractsCountRDD (country, n_contracts) 
# reduceByKey (country, max_n_contracts)
# join with map countryTitleContractsCountRDD to (country, (title, n_contracts))
# res: (country, (max_n_contracts, (title, n_contracts)))
# filter max_n_contracts == n_contracts
# map to string "country, job_title, n_contracts"
res2RDD = countryTitleContractsCountRDD.map(lambda pair: (pair[0][0], pair[1]))\
    .reduceByKey(lambda v1, v2: max(v1, v2))\
    .join(countryTitleContractsCountRDD.map(lambda pair: (pair[0][0], (pair[0][1], pair[1]))))\
    .filter(lambda pair: pair[1][0] == pair[1][1][1])\
    .map(lambda pair: f"{pair[0], pair[1][1][0], pair[1][1][0]}")

res2RDD.saveAsTextFile(outputPath2)