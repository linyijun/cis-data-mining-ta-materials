import json
import time
from pyspark import SparkContext

# Initialize variables
reviewJson = '../datasets/review.json'
businessJson = '../datasets/business.json'
outputJson = '../datasets/user_business.csv'

# Read Input
sc = SparkContext()
sc.setLogLevel("ERROR")
reviewRdd = sc.textFile(reviewJson).map(lambda review: json.loads(review))
businessRdd = sc.textFile(businessJson).map(lambda business: json.loads(business)).filter(lambda business: business['state'] == 'NV')

# Prepare business_id to stars and categories pairs
businessRdd = businessRdd.map(lambda business: (business["business_id"], 1))
reviewRdd = reviewRdd.map(lambda review: (review["business_id"], review["user_id"]))

# Join 2 rdds with key business_id
userBusinessPairs = businessRdd.join(reviewRdd).map(lambda p: (p[1][1], p[0])).collect()

# Generate output
f = open(outputJson, "w")
f.write('user_id,business_id\n')

for pair in userBusinessPairs:
    f.write(pair[0] + ',' + pair[1] + '\n')
f.close()
