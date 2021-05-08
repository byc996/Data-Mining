import os
import sys
import csv
import time
import json
import random
import binascii
from pyspark import SparkContext

def create_random_hash_funcs(n_bits, n_hash_func):
    m = n_bits
    random_a = list()
    random_b = list()
    while len(random_a) != n_hash_func:
        a = random.randint(1, 5000)
        b = random.randint(1000, 5000000)
        if a % m != 0 and a not in random_a:
            random_a.append(a)
            random_b.append(b)
    return random_a, random_b, m

def hash_city(a_list, b_list, m, city):
    hashed_list = list()
    # print(city)
    if city.strip() == "":
        return [-1]
    x = int(binascii.hexlify(city.encode('utf8')),16)
    for i in range(len(a_list)):
        h_v = (a_list[i] * x + b_list[i]) % m
        hashed_list.append(h_v)
    return hashed_list


def is_exists(index_list, bit_list):
    if index_list[0] == -1:
        return "0"
    else:
        exist = "1"
        for index in index_list:
            if bit_list[index] == 0:
                exist = "0"
        return exist

if __name__ == '__main__':
    start_time = time.time()
    os.environ['JAVA_HOME'] = '/home/buyi/app/jdk'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

    sc = SparkContext('local', 'task1')
    first_json_path, second_json_path, output_file_path = "/home/buyi/data/hw6/business_first.json", "/home/buyi/data/hw6/business_second.json", "task1_ans.csv"
    # first_json_path, second_json_path, output_file_path = sys.argv[1:]

    rdd_first = sc.textFile(first_json_path).map(json.loads).map(lambda x:x['city'])
    rdd_second = sc.textFile(second_json_path).map(json.loads).map(lambda x: x['city'])
    # m = rdd_first.count()  # num of elements inserted
    n_bits = 10000  # num of bits in array
    n_hash_func = 3
    random_a, random_b, m = create_random_hash_funcs(n_bits, n_hash_func)
    rdd_indices = rdd_first.filter(lambda x:x.strip()!="").flatMap(lambda x: hash_city(random_a, random_b, m, x)).distinct()
    indices = rdd_indices.collect()
    bit_list = [0]*n_bits
    for index in indices:
        bit_list[index] = 1
    result = rdd_second.map(lambda x:hash_city(random_a, random_b, m, x)).map(lambda x: is_exists(x, bit_list)).collect()

    # print(result.count())
    # print(result.filter(lambda x:x==0).count())
    print(len(result), rdd_second.count())

    with open(output_file_path, 'w') as f:
        # f.write(" ".join(result))
        writer = csv.writer(f, delimiter=' ')
        writer.writerow(result)
    print("time: {}s".format(time.time() - start_time))