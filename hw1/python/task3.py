import sys
import json
from pyspark import SparkContext

if __name__ == '__main__':

    # input_file, output_file, partition_type, n_partitions, n = "review.json", "default_ans", "customized", 20, 50
    input_file, output_file, partition_type, n_partitions, n = sys.argv[1:]
    n_partitions, n = int(n_partitions), int(n)
    sc = SparkContext("local[*]", 'task3')
    rdd = sc.textFile(input_file).map(json.loads).map(lambda x: [x['business_id'], 1])
    if partition_type.strip() == 'customized':
        rdd = rdd.partitionBy(n_partitions, hash)
    rdd1 = rdd.reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] > n).map(list)
    result = dict()
    result['n_partitions'] = rdd.getNumPartitions()
    result['n_items'] = rdd.glom().map(len).collect()
    result['result'] = rdd1.collect()
    with open(output_file, 'w') as f:
        json.dump(result, f)