import os
import time
import sys
from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth


if __name__ == '__main__':
    os.environ['JAVA_HOME'] = "/home/buyi/app/jdk"
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    start_time = time.time()
    sc = SparkContext("local[*]", 'task3')
    # filter_threshold, support, input_file_path, output_file_path = sys.argv[1:]
    filter_threshold, support, input_file_path, output_file_path = "70", "50", "user_business.csv", "task3_ans"

    rdd = sc.textFile(input_file_path).cache()
    first = rdd.first()
    if str(first).find("user_id") != -1:
        rdd = rdd.filter(lambda x: x != first)
    rdd = rdd.map(lambda x: (x.split(',')[0].strip(), x.split(',')[1].strip()))
    rdd_basket = rdd.groupBy(lambda x: x[0]).mapValues(lambda x: list(set([i[1] for i in x]))) \
        .map(lambda x: x[1]).filter(lambda x: len(x) > int(filter_threshold))

    count = rdd_basket.count()
    n_p = rdd_basket.getNumPartitions()
    print(n_p)
    model = FPGrowth.train(rdd_basket, int(support)/count, n_p)
    tmp_frequents = model.freqItemsets().map(lambda x:x[0]).collect()
    frequents = list()
    tmp_dict = dict()
    for item in tmp_frequents:
        tmp = tuple(sorted(item))
        if len(item) == 1:
            tmp = (item[0],)
            tmp_dict[1] = tmp_dict.get(1,0) + 1
        else:
            tmp_dict[len(item)] = tmp_dict.get(len(item), 0) + 1
        frequents.append(str(tmp))
    # print(tmp_dict)
    frequents_task2 = list()
    with open("frequents_task2", 'r') as f:
        for item in f.readlines():
            frequents_task2.append(item[:-1])

    intersect = set(frequents).intersection(frequents_task2)
    with open(output_file_path, 'w') as f:
        f.truncate()
        f.write("Task2,{}\n".format(len(frequents_task2)))
        f.write("Task3,{}\n".format(len(frequents)))
        f.write("Intersection,{}\n".format(len(intersect)))

    print("Duration: {}".format(round(time.time() - start_time, 2)))
