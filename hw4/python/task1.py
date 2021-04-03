import os
import sys
import time
from itertools import combinations

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from graphframes import *


if __name__ == '__main__':
    start_time = time.time()
    os.environ['JAVA_HOME'] = "/home/buyi/app/jdk"
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

    filter_threshold, input_file_path, community_output_file_path = '7', '/home/buyi/data/hw4/ub_sample_data.csv', 'task1_ans'
    # filter_threshold, input_file_path, community_output_file_path = sys.argv[1:]

    # os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
    spark = SparkSession.builder.appName("task1").master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.option("header",True).csv(input_file_path)
    df1 = df.groupby("user_id").agg(f.collect_list("business_id").alias("business_list"))

    user_profile = dict()
    for row in df1.collect():
        user_profile[row['user_id']] = list(set(row['business_list']))
    pairs = list(combinations(user_profile.keys(), 2))
    nodes = list()
    links = list()
    for pair in pairs:
        if len(set(user_profile[pair[0]]).intersection(user_profile[pair[1]])) >= int(filter_threshold):
            nodes.append(pair[0])
            nodes.append(pair[1])
            links.append(pair)

    vertices = spark.createDataFrame([(x,) for x in set(nodes)],['id'])

    edges = spark.createDataFrame(links,['src', 'dst'])
    g = GraphFrame(vertices, edges)
    result = g.labelPropagation(maxIter=5)

    communities_df = result.groupby('label').agg(f.collect_list("id").alias("users"))
    communities = [sorted(["'"+x+"'" for x in row['users']]) for row in communities_df.collect()]
    communities = sorted(communities, key=lambda x:x[0])
    communities = sorted(communities, key=lambda x: len(x))
    # communities = result.rdd.map(lambda x:(x['label'], x['id'])).groupByKey().mapValues(lambda x: sorted(["'"+i+"'" for i in set(x)]))\
    #     .map(lambda x:x[1]).sortBy(lambda x:x[0]).sortBy(lambda x:len(x)).collect()

    with open(community_output_file_path, 'w') as f:
        for row in communities:
            f.write(','.join(row)+"\n")
    print("time: {}".format(time.time() - start_time))

