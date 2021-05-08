import os
import time
import sys

from pyspark import SparkContext

def phase1_map(partition, threshold, length, n_buckets):
    baskets = list(partition)
    sub_threshold = len(baskets) / length * threshold
    frequent_itemsets = pcy(baskets, sub_threshold, n_buckets)
    for i in frequent_itemsets:
        yield i, 1


def phase2_map(partition, candidate):
    baskets = list(partition)
    candidate_dict = dict()
    for c in candidate:
        candidate_dict[c] = 0
    for b in baskets:
        for c in candidate_dict.keys():
            tmp = {c} if isinstance(c, str) else set(c)
            if tmp.issubset(set(b)):
                candidate_dict[c] += 1
    for k, v in candidate_dict.items():
        yield k, v


def hash_pcy(key, n_buckets):
    return sum([int(i) for i in key]) % n_buckets


def pcy(baskets, support, n_buckets):
    baskets = list(baskets)
    # Pass 1
    candidate_dict = dict()
    hash_table = dict()
    bitmap = [0] * n_buckets
    frequent_list = list()
    frequent_itemset = list()
    singleton = list()
    for b in baskets:
        for i in range(len(b)):
            candidate_dict[(b[i],)] = candidate_dict.get((b[i],), 0) + 1
            for j in range(i + 1, len(b)):
                key = hash_pcy((b[i], b[j]), n_buckets)
                hash_table[key] = hash_table.get(key, 0) + 1
    for k, v in candidate_dict.items():
        if v >= support:
            frequent_list.append(k)
            singleton.append(k[0])
    for k, v in hash_table.items():
        if v >= support:
            bitmap[k] = 1
    frequent_list.sort()
    # print("frequent_list: ", frequent_list)

    frequent_itemset.extend(frequent_list)

    # Pass 2
    candidates_dict = dict()
    for i in range(len(frequent_list)):
        for j in range(i + 1, len(frequent_list)):
            pair = tuple(sorted((frequent_list[i][0], frequent_list[j][0])))
            if bitmap[hash_pcy(pair, n_buckets)] == 1:
                candidates_dict[pair] = 0
    for b in baskets:
        for c in candidates_dict.keys():
            if set(c).issubset(b):
                candidates_dict[c] += 1

    frequent_list = list()
    for k in candidates_dict.keys():
        if candidates_dict[k] >= support:
            frequent_list.append(tuple(sorted(k)))
    # print("frequent_list: ", len(frequent_list), frequent_list)
    frequent_itemset.extend(frequent_list)

    # Pass 3,4,...
    size = 3
    while len(frequent_list) > 0:
        candidates_dict = dict()
        for i in range(len(frequent_list)-1):
            for j in range(i+1, len(frequent_list)):
                union = set(frequent_list[i]).union(frequent_list[j])
                if len(union) == size:
                    candidates_dict[tuple(sorted(union))] = 0

        for b in baskets:
            for c in candidates_dict.keys():
                if set(c).issubset(b):
                    candidates_dict[c] += 1
        frequent_list = list()
        for k in candidates_dict.keys():
            if candidates_dict[k] >= support:
                frequent_list.append(k)
        size += 1
        # print("frequent_list: ", len(frequent_list), frequent_list)
        frequent_itemset.extend(frequent_list)

    return frequent_itemset


def output(output_path, l, title):
    result_dict = dict()

    for i in l:
        val = '(\'' + i[0] + '\')' if len(i) == 1 else i
        if len(i) in result_dict.keys():
            result_dict[len(i)].append(val)
        else:
            result_dict[len(i)] = [val]

    with open(output_path, 'a') as f:

        f.write(title + '\n\n')
        keys = sorted(result_dict.keys())
        sum = 0
        for k in keys:
            sum += len(result_dict[k])
            f.write(','.join(sorted([str(item) for item in result_dict[k]])))
            f.write('\n\n')
        # print(sum)


if __name__ == '__main__':
    # os.environ['JAVA_HOME'] = "/home/buyi/app/jdk"
    # os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    start_time = time.time()
    sc = SparkContext("local[*]", 'task1')
    # case_number, support, input_file_path, output_file_path = "1", "4", "/home/buyi/data/hw2/small2.csv", "task1_1_4_ans"
    # case_number, support, input_file_path, output_file_path = "2", "9", "/home/buyi/data/hw2/small2.csv", "task1_2_9_ans"
    #
    case_number, support, input_file_path, output_file_path = sys.argv[1:]
    n_partitions, n_buckets = 3, 300
    rdd = sc.textFile(input_file_path, n_partitions)
    first = rdd.first()
    rdd1 = rdd.filter(lambda x: x != first).map(
        lambda x: (x.split(',')[0].strip(), x.split(',')[1].strip()))
    if int(case_number) == 1:
        rdd_basket = rdd1.groupBy(lambda x: x[0]).mapValues(lambda x: list(set([i[1] for i in x])))
    else:
        rdd_basket = rdd1.groupBy(lambda x: x[1]).mapValues(lambda x: list(set([i[0] for i in x])))
    length = rdd_basket.count()
    # print(rdd_basket.glom().map(len).collect())

    # Phase1
    with open(output_file_path, 'w') as f:
        f.truncate()
    r = rdd_basket.map(lambda x: x[1]).mapPartitions(
        lambda par: phase1_map(par, int(support), length, n_buckets)).reduceByKey(
        lambda x, y: 1)
    # Phase2
    output(output_file_path, r.map(lambda x: x[0]).collect(), "Candidates: ")
    candidates = r.map(lambda x: x[0]).collect()
    r_result = rdd_basket.map(lambda x: x[1]).mapPartitions(lambda par: phase2_map(par, candidates)).reduceByKey(
        lambda x, y: x + y).filter(lambda x: x[1] >= int(support))
    output(output_file_path, r_result.map(lambda x: x[0]).collect(), "Frequent Itemsets: ")
    print("Duration: {}".format(round(time.time() - start_time, 2)))
