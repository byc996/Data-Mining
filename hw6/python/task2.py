import os
import sys
import csv
import time
import datetime
import json
import random
import binascii
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def my_print(ground_truth, estimation, output_file_path):
    t = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    with open(output_file_path, 'a') as f:
        f.write(",".join([t, str(ground_truth), str(estimation)])+'\n')
        # writer = csv.writer(f, delimiter=',')
        # print("{},{},{}".format(t, ground_truth, estimation))
        # writer.writerow([t, ground_truth, estimation])


def predict_num_distinct_value(rdd, random_a, random_b, m, n_group, output_file_path):
    city_list = rdd.collect()
    long_tail_list = list()
    for i in range(len(random_a)):
        a, b = random_a[i], random_b[i]
        long_tail = 0
        for city in city_list:
            x = int(binascii.hexlify(city.encode('utf8')), 16)
            hashed_city = (a * x + b) % m
            bit_str = format(hashed_city, 'b')
            tail = 0
            for e in bit_str[::-1]:
                if e == '0': tail += 1
                else: break
            if tail > long_tail:
                long_tail = tail
        long_tail_list.append(long_tail)

    long_tail_list.sort()
    group_avg_list = list()
    for i in range(int(len(long_tail_list)/n_group)):
        est = [2**r for r in long_tail_list[n_group*i:n_group*(i+1)]]
        group_avg_list.append(sum(est)/len(est))
    group_avg_list.sort()
    estimation = int(group_avg_list[int(len(group_avg_list) / 2)])

    ground_truth = len(set(city_list))
    print(estimation, ground_truth)
    my_print(ground_truth, estimation, output_file_path)

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

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = '/home/buyi/app/jdk'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

    sc = SparkContext(appName='task1')
    sc.setLogLevel("OFF")
    # port, output_file_path = sys.argv[1:]
    port, output_file_path = "9999", "task2_ans_non_csv"

    window_length, sliding_interval, batch_interval  = 30, 10, 5
    n_hash_func, n_group = 49, 7
    m = find_prime(10000)
    random_a  = random.sample(range(10, 50000), n_hash_func)
    random_b  = random.sample(range(1000, 10000000), n_hash_func)
    with open(output_file_path, 'w') as f:
        f.truncate()
        f.write(",".join(['Time', 'Ground Truth', 'Estimation']) + '\n')
        # writer = csv.writer(f, delimiter=',')
        # writer.writerow(['Time', 'Ground Truth', 'Estimation'])

    ssc = StreamingContext(sc, batch_interval)
    streaming = ssc.socketTextStream('localhost', int(port))

    streaming.window(window_length, sliding_interval).map(json.loads).map(lambda x: x['city'])\
        .filter(lambda x:x.strip()!="").foreachRDD(lambda x:predict_num_distinct_value(x, random_a, random_b, m, n_group, output_file_path))

    ssc.start()
    ssc.awaitTermination()