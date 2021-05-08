import os
import json
import csv
from pyspark import SparkContext

os.environ['JAVA_HOME'] = "/home/buyi/app/jdk"
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

sc = SparkContext("local[*]", 'task2')
business_path = "/home/buyi/data/hw2/business.json"
review_path = "/home/buyi/data/hw2/review.json"
output_file = "user_business.csv"

rdd_business = sc.textFile(business_path).map(json.loads)
rdd_review = sc.textFile(review_path).map(json.loads)
rdd_business = rdd_business.filter(lambda x: x['state'] == 'NV').map(lambda x: (x['business_id'], x['state']))
rdd_review = rdd_review.map(lambda x: (x['business_id'], x['user_id']))
rdd = rdd_business.join(rdd_review).map(lambda x: (x[1][1], x[0]))

with open(output_file, 'w') as f:
    f.truncate()
    writer = csv.writer(f)
    writer.writerow(('user_id', 'business_id'))
    writer.writerows(rdd.collect())
