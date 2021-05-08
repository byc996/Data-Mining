import os
import sys
import math
import json
import time
from itertools import combinations
import random

from pyspark import SparkContext


def calculate_pearson(pair, profile_dict):
    i1, i2 = pair[0], pair[1]
    i1_profile_rate_dict, i2_profile_rate_dict = dict(profile_dict[i1]), dict(profile_dict[i2])
    i1_profile, i2_profile = list(i1_profile_rate_dict.keys()), list(i2_profile_rate_dict.keys())
    intersect_items = set(i1_profile).intersection(i2_profile)
    if len(intersect_items) >= 3:
        # print(b1_profile_dict)
        i1_stars = [i1_profile_rate_dict[b] for b in intersect_items]
        i2_stars = [i2_profile_rate_dict[b] for b in intersect_items]
        i1_mean, i2_mean = sum(i1_stars)/len(i1_stars), sum(i2_stars)/len(i2_stars)
        # i1_mean, i2_mean = sum(i1_profile_rate_dict.values())/len(i1_profile_rate_dict.values()), sum(i2_profile_rate_dict.values())/len(i1_profile_rate_dict.values())
        numerator = sum([(i1_profile_rate_dict[i] - i1_mean)*(i2_profile_rate_dict[i] - i2_mean) for i in intersect_items])
        denominator = math.sqrt(sum([math.pow(i1_profile_rate_dict[i] - i1_mean, 2) for i in intersect_items])) * \
                                  math.sqrt(sum([math.pow(i2_profile_rate_dict[i] - i2_mean, 2) for i in intersect_items]))
        # print(numerator, denominator)
        sim = numerator / denominator if denominator != 0 else 0
        return pair, sim


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
    # print(a_list)
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

def jaccard_similarity(x, users_business):
    s1, s2 = users_business[x[0]], users_business[x[1]]
    similarity = len(set(s1).intersection(s2)) / len(set(s1).union(s2))
    return x, similarity

if __name__ == '__main__':
    start_time = time.time()
    os.environ['JAVA_HOME'] = '/home/buyi/app/jdk'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    train_file, model_file, cf_type = '/home/buyi/data/hw3/train_review.json', 'task3user.model', 'user_based' # task3item.model item_based  task3user.model user_based
    # train_file, model_file, cf_type = sys.argv[1:]

    sc = SparkContext('local[*]', 'task3')
    rdd = sc.textFile(train_file).map(json.loads).persist()
    business_index_dict = dict(rdd.map(lambda x:x['business_id']).distinct().zipWithIndex().collect())
    index_business_dict = dict(rdd.map(lambda x:x['business_id']).distinct().zipWithIndex().map(lambda x:(x[1], x[0])).collect())
    # print(len(business_index_dict))
    user_index_dict = dict(rdd.map(lambda x: x['user_id']).distinct().zipWithIndex().collect())
    if cf_type == 'item_based':
        rdd1 = rdd.map(lambda x:(x['business_id'], (user_index_dict[x['user_id']], x['stars']))).groupByKey().mapValues(lambda x:list(x))
        business_profile_dict = dict(rdd1.collect())
        business = rdd1.map(lambda x:x[0])

        rdd2 = business.cartesian(business).filter(lambda x:x[0]<x[1]).map(lambda x:calculate_pearson(x, business_profile_dict)).filter(lambda x:x is not None).filter(lambda x:x[1]>0)
        rdd2.collect()
        with open(model_file, 'w') as f:
            for pair_score in rdd2.collect():
                b1, b2 = pair_score[0][0], pair_score[0][1]
                f.write(json.dumps({"b1":b1, "b2":b2, "sim":pair_score[1]})+"\n")
        print("time: {}".format(time.time() - start_time))
    else:
        total_rows, band_rows = 40, 1
        rdd_user = rdd.map(lambda x:(x['user_id'], (business_index_dict[x['business_id']], x['stars']))).groupByKey().mapValues(lambda x:list(x))
        user_profile_dict = dict(rdd_user.collect())
        rdd1 = rdd_user.mapValues(lambda x:[i[0] for i in x])
        # print(rdd1.take(10))
        a_list, b_list, m = generate_hash_parameters(len(user_index_dict), total_rows)
        rdd2 = rdd1.mapValues(lambda x:signature(x, a_list, b_list, m))
        rdd3 = rdd2.flatMap(lambda x: split_band(x, band_rows, int(total_rows / band_rows)))
        rdd4 = rdd3.groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(
            lambda x: list(combinations(x, 2))).map(lambda x: tuple(sorted(x))).distinct()
        users_business = dict(rdd1.collect())
        candidate_pairs = rdd4.map(lambda x: jaccard_similarity(x, users_business)).filter(lambda x: x[1] >= 0.01).map(lambda x:x[0])
        # candidate_pairs.map(lambda x:calculate_pearson(x, business_profile_dict)).filter(lambda x:x is not None)
        result = candidate_pairs.map(lambda x: calculate_pearson(x, user_profile_dict)).filter(lambda x: x is not None).filter(lambda x:x[1]>0)
        with open(model_file, 'w') as f:
            for pair_score in result.collect():
                u1, u2 = pair_score[0][0], pair_score[0][1]
                f.write(json.dumps({"u1": u1, "u2": u2, "sim": pair_score[1]}) + "\n")

        # print(result.count())
        print("time: {}".format(time.time() - start_time))


