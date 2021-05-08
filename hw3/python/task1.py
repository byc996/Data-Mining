import os
import sys
import json
import time
import random
from itertools import combinations

from pyspark import SparkContext


def hash_func(x, a, b, m):
    return (a * x + b) % m

def find_prime(x):
    isPrime = False
    while not isPrime:
        x = x + 1
        for i in range(2, x):
            if x % i == 0:
                break
        else:
            isPrime = True
    return x

def generate_hash_parameters(n_features, num_rows):
    m = find_prime(n_features)
    random_a = list()
    random_b = list()
    while len(random_a) != num_rows:
        a = random.randint(1, 500)
        b = random.randint(10, 50000)
        if a % m != 0 and a not in random_a:
            random_a.append(a)
            random_b.append(b)
    return random_a, random_b, m


def signature(users, a_list, b_list, m):
    signature = list()
    for i in range(len(a_list)):
        min_signature = min(list(map(lambda x: hash_func(x, a_list[i], b_list[i], m), users)))
        signature.append(min_signature)
    return signature


def split_band(pair, r, b):
    business, signatures = pair[0], pair[1]
    for i in range(b):
        band = signatures[r*i: r*(i+1)]
        yield "{}_{}".format(i, "_".join([str(i) for i in band])), business

def jaccard_similarity(x, business_users):
    s1, s2 = business_users[x[0]], business_users[x[1]]

    similarity = len(set(s1).intersection(s2))/ len(set(s1).union(s2))
    return x, similarity

if __name__ == '__main__':
    start_time = time.time()
    os.environ['JAVA_HOME'] = '/home/buyi/app/jdk'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    #input_file, output_file = '/home/buyi/data/hw3/train_review.json', 'task1.res'
    input_file, output_file = sys.argv[1:]
    num_rows = 40
    # num_bands = 20
    r = 1
    sc = SparkContext('local', 'task1')

    rdd_review = sc.textFile(input_file).map(json.loads).map(lambda x:(x['business_id'],x['user_id'])).persist()
    rdd_users = rdd_review.map(lambda x: x[1]).distinct().sortBy(lambda x: x)#.map(lambda x:(x[1],x[0]))
    user_index_dict = dict(rdd_users.zipWithIndex().collect())
    # print("the number of users: {}".format(len(user_index_dict)))

    a_list, b_list, m = generate_hash_parameters(len(user_index_dict), num_rows)

    # print("m: {}".format(m))
    rdd = rdd_review.groupByKey().mapValues(lambda x:[user_index_dict[i] for i in x])
    # print("characteristic matrix: {}".format(rdd.take(5)))
    # print("time: {}".format(time.time() - start_time))
    rdd1 = rdd.mapValues(lambda x:signature(x, a_list, b_list, m))
    # print("signatures matrix: {}".format(rdd1.take(5)))

    rdd2 = rdd1.flatMap(lambda x:split_band(x, r, int(num_rows/r)))

    rdd3 = rdd2.groupByKey().map(lambda x:list(x[1])).filter(lambda x:len(x) > 1).flatMap(lambda x:list(combinations(x,2))).map(lambda x:tuple(sorted(x))).distinct()
    # print("candidate pairs: {}".format(rdd3.count()))
    business_users = dict(rdd.collect())
    result = rdd3.map(lambda x:jaccard_similarity(x, business_users)).filter(lambda x:x[1] >= 0.05)
    data = result.collect()
    # print(result.take(10))
    with open(output_file, 'w') as f:
        for item in data:
            tmp = {'b1':item[0][0], 'b2': item[0][1], 'sim': item[1]}
            f.write(json.dumps(tmp)+"\n")
    print("time: {}".format(time.time() - start_time))

