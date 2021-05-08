import os
import sys
import json
import time
import math
from pyspark import SparkContext

if __name__ == '__main__':
    start_time = time.time()
    os.environ['JAVA_HOME'] = '/home/buyi/app/jdk'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    sc = SparkContext('local[*]','task2_predict')

    test_file, model_file, output_file = "/home/buyi/data/hw3/test_review.json", "task2.model", "task2.predict"
    # test_file, model_file, output_file = sys.argv[1:]

    rdd_model = sc.textFile(model_file).map(json.loads)
    rdd_business = rdd_model.filter(lambda x:x['type'] == 'b').map(lambda x:(x['business_id'], x['business_profile']))
    rdd_user = rdd_model.filter(lambda x:x['type'] == 'u').map(lambda x:(x['user_id'], x['user_profile']))
    business_dict, user_dict = dict(rdd_business.collect()), dict(rdd_user.collect())
    # print(business_dict):
    rdd_test = sc.textFile(test_file).map(json.loads).map(lambda x: (x['business_id'], x['user_id']))
    predicted_pairs = rdd_test.collect()
    with open(output_file, 'w') as f:
        for pair in predicted_pairs:
            business, user = business_dict.get(pair[0]), user_dict.get(pair[1])
            if business is not None and user is not None:
                sim = len(set(business).intersection(user))/(math.sqrt(len(business))*math.sqrt(len(user)))
                if sim >= 0.01:
                    f.write(json.dumps({"user_id":pair[1], "business_id":pair[0], "sim":sim})+"\n")

    print("time: {}".format(time.time() - start_time))
