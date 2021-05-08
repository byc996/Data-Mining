import math
import os
import sys
import json
import time
from pyspark import SparkContext

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False

def remove_words(w_list, stop_words):
    new_words = list()
    for w in w_list:
        if w not in stop_words and not is_number(w):
            new_words.append(w)
    return new_words

def filter_rare(w_list, no_rare_words_index):
    new_words = list()
    # print(w_list)
    for w in w_list:
        index = no_rare_words_index.get(w)
        if index is not None:
            new_words.append(index)
    return new_words

def calculate_tf_idf(x, idf_dict):
    business, word_list = x[0], list(x[1])
    new_words_dict = dict()
    for w in word_list:
        new_words_dict[w] = new_words_dict.get(w, 0) + 1
    # calculate tf-idf
    max_f = 0
    for k,v in new_words_dict.items():
        max_f = v if v > max_f else max_f
    for k in new_words_dict.keys():
        new_words_dict[k] = new_words_dict[k]/max_f * idf_dict[k]
    a = sorted(new_words_dict.items(), key=lambda x: x[1], reverse=True)
    words_dict = dict(a[:200]) if len(a) > 200 else dict(a)
    return business, list(words_dict.keys())

def aggregate_item(business_ids, business_vector_dict):
    tmp = list()
    for id in business_ids:
        tmp += business_vector_dict[id]
    return list(set(tmp))

if __name__ == '__main__':

    start_time = time.time()
    os.environ['JAVA_HOME'] = '/home/buyi/app/jdk'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    train_file, model_file, stopwords = '/home/buyi/data/hw3/train_review.json', 'task2.model', '/home/buyi/data/hw3/stopwords'
    # train_file, model_file, stopwords = sys.argv[1:]

    sc = SparkContext('local[*]', 'task2')
    rdd_review = sc.textFile(train_file).map(json.loads).map(lambda x: (x['business_id'], x['text']))
    # 1. concatenate all reviews for a business
    rdd = rdd_review.groupByKey().mapValues(lambda x:" ".join(list(x))).mapValues(lambda x: [i.strip().lower() for i in x.split(' ')])
    punctuations = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"]
    stop_words = sc.textFile(stopwords).collect()
    stop_words.append("")
    for p in punctuations:
        stop_words.append(p)
    # print(rdd.take(1))
    # print("the number of stop words: {}".format(len(stop_words)))
    rdd1 = rdd.mapValues(lambda x: remove_words(x, stop_words))

    total_num_words = rdd1.map(lambda x: len(x[1])).reduce(lambda x, y: x + y)
    threshold = total_num_words * 0.000001
    no_rare_words_index = dict(rdd1.flatMap(lambda x: [(i, 1) for i in x[1]]).reduceByKey(lambda x, y: x + y).filter(
        lambda x: x[1] >= threshold).map(lambda x: x[0]).zipWithIndex().collect())
    print("the number of not rare words: {}".format(len(no_rare_words_index)))
    rdd2 = rdd1.mapValues(lambda x: filter_rare(x, no_rare_words_index))

    n_business = rdd.count()
    print("number of business: {}".format(n_business))
    rdd_idf = rdd2.flatMap(lambda x:set(x[1])).map(lambda x:(x, 1)).groupByKey().mapValues(lambda x:sum(x)).map(lambda x: (x[0], math.log(n_business/ x[1], 10)))
    idf_dict = dict(rdd_idf.collect())
    print("time: {}".format(time.time() - start_time))

    rdd_tf_idf = rdd2.map(lambda x: calculate_tf_idf(x, idf_dict))
    business_profile_dict = dict(rdd_tf_idf.collect())
    print("time: {}".format(time.time() - start_time))

    rdd3 = sc.textFile(train_file).map(json.loads).map(lambda x: (x['user_id'], x['business_id']))
    rdd_user_profile = rdd3.groupByKey().mapValues(lambda x:aggregate_item(x,business_profile_dict))
    user_profile_dict = dict(rdd_user_profile.collect())
    # print(user_profile_dict)


    with open(model_file, 'w') as f:
        f.truncate()
        for k,v in business_profile_dict.items():
            f.write(json.dumps({"type":"b", "business_id":k, "business_profile": v})+'\n')
        for k,v in user_profile_dict.items():
            f.write(json.dumps({"type":"u", "user_id":k, "user_profile":v})+'\n')

    print("time: {}".format(time.time() - start_time))