import sys
import json
from pyspark import SparkContext

def test(sc):
    rdd = sc.textFile('review.json').map(json.loads)
    print(rdd.take(3))

def t2_spark(review_file, business_file, n):
    sc = SparkContext("local[*]", 'task2')
    rdd_review = sc.textFile(review_file).map(json.loads).map(lambda x: [x['business_id'], x['stars']])
    rdd_review_1 = rdd_review.filter(lambda x: x[1] is not None).groupByKey().mapValues(list)
    rdd_business = sc.textFile(business_file).map(json.loads).map(lambda x: (x['business_id'], x['categories']))
    rdd_business_1 = rdd_business.filter(lambda x: x[1] is not None).filter(lambda x: x[1].strip() != "")
    rdd = rdd_business_1.join(rdd_review_1).map(lambda x:(x[1][0],x[1][1])).flatMap(lambda x: [[c.strip(), x[1]] for c in x[0].split(',')])
    rdd2 = rdd.reduceByKey(lambda x, y: x + y).mapValues(lambda x:sum(x)/len(x))
    rdd3 = rdd2.sortBy(lambda x: x[0]).sortBy(lambda x: x[1], ascending=False)

    return rdd3.map(list).take(n)

def t2_no_spark(review_file, business_file, n):
    review_dict = dict()
    with open(review_file, 'r') as f:
        line = f.readline()
        while line:
            item = json.loads(line)
            if item['stars'] is not None:
                review_dict.setdefault(item['business_id'], []).append(item['stars'])
            line = f.readline()
    business_dict = dict()
    with open(business_file, 'r') as f:
        line = f.readline()
        while line:
            item = json.loads(line)
            if item['categories'] is not None and item['categories'].strip() != '':
                business_dict[item['business_id']] = item['categories']
            line = f.readline()

    join_dict = dict()
    for k in business_dict.keys():
        if k in review_dict.keys():
            categories = business_dict[k].strip().split(',')
            for c in categories:
                for s in review_dict[k]:
                    join_dict.setdefault(c.strip(), []).append(s)
    avg = list(map(lambda x: [x[0], sum(x[1])/len(x[1])], join_dict.items()))
    tmp = sorted(avg, key=lambda x:x[0])
    result = sorted(tmp, key=lambda x:x[1], reverse=True)[:n]
    return result


if __name__ == '__main__':

    # review_file, business_file, output_file, if_spark, n = "review.json", "business.json", 'task2_spark_ans', 'spark', 10
    review_file, business_file, output_file, if_spark, n = sys.argv[1:]
    n = int(n)
    if if_spark.strip() == 'spark':
        result = t2_spark(review_file, business_file, n)
    elif if_spark.strip() == 'no_spark':
        result = t2_no_spark(review_file, business_file, n)

    with open(output_file, 'w') as f:
        json.dump({"result": result}, f)



