import sys
import json
from pyspark import SparkContext

def test(rdd):
    print(rdd.take(4))

def t1_a(rdd):
    return rdd.count()


def t1_b(rdd, y):
    rdd1 = rdd.filter(lambda x: y.strip() in x['date'])
    # return rdd1.take(4)
    return rdd1.count()

def t1_c(rdd):
    rdd1 = rdd.map(lambda x: x['user_id']).distinct()
    return rdd1.count()

def t1_d(rdd, m):
    rdd1 = rdd.map(lambda x: (x['user_id'], 1)).reduceByKey(lambda a, b: a+b).map(lambda x: list(x))
    rdd2 = rdd1.sortBy(lambda x: x[0]).sortBy(lambda x: x[1], ascending=False)
    return rdd2.take(m)

def t1_e(rdd, stop_words, n):
    punctuations = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"]
    stop_words.append("")
    for p in punctuations:
        stop_words.append(p)
    rdd1 = rdd.flatMap(lambda x: x['text'].split(' ')).map(lambda x: x.lower().strip()).filter(lambda x: x not in stop_words)
    rdd2 = rdd1.map(lambda x: [x, 1]).reduceByKey(lambda a, b: a+b)
    rdd3 = rdd2.sortBy(lambda x: x[0]).sortBy(lambda x: x[1], ascending=False).map(lambda x:x[0])
    return rdd3.take(n)

if __name__ == '__main__':

    # input_file, output_file, stopwords, y, m, n = "review.json", "task1_ans", "stopwords", "2018", 10, 10
    input_file, output_file, stopwords, y, m, n = sys.argv[1:]
    m, n = int(m), int(n)
    sc = SparkContext("local[*]", 'task1')
    rdd_review = sc.textFile(input_file).map(json.loads)

    stop_words = sc.textFile(stopwords).collect()
    # print(t1_e(rdd_review, stop_words, n))
    result = dict()
    result['A'] = t1_a(rdd_review)
    result['B'] = t1_b(rdd_review, y)
    result['C'] = t1_c(rdd_review)
    result['D'] = t1_d(rdd_review, m)
    result['E'] = t1_e(rdd_review, stop_words, n)
    with open(output_file, 'w') as f:
        json.dump(result, f)
    # test(rdd_review)


