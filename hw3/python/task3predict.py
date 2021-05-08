import os
import sys
import json
import time


from pyspark import SparkContext


def predict_item(x, model_dict, business_index, user_index, user_business_dict, business_avg_rate, user_avg_rate, N):
    # x (user, business)
    user, business = user_index.get(x[0]), business_index.get(x[1])

    if business is None:
        p = user_avg_rate.get(user)
        return x, p
    if user is None or model_dict.get(business) is None:
        p = business_avg_rate.get(business)
        return x, p

    # if user_business_dict.get(user) is None or model_dict.get(business) is None:
    #     p = business_avg_rate.get(business)
    #     return x, p
    business_rate_dict = dict(user_business_dict[user])
    business_sim_dict = sorted(model_dict[business], key=lambda x: x[1], reverse=True)
    candidate_items = list()

    i = 0
    while len(candidate_items) < N and i < len(business_sim_dict):
        pair = business_sim_dict[i]
        if pair[0] in business_rate_dict.keys():
            candidate_items.append((pair[0], pair[1], business_rate_dict[pair[0]]))
        i += 1

    denominator = sum([abs(x[1]) for x in candidate_items])
    if denominator == 0:
        p = business_avg_rate.get(business) if business_avg_rate.get(business) is not None else user_avg_rate.get(user)
    else:
        p = sum([x[1] * x[2] for x in candidate_items]) / denominator

    return x, p


def predict_user(x, model_dict, business_index, user_index, business_user_dict, business_avg_rate, user_avg_rate, N):
    user, business = user_index.get(x[0]), business_index.get(x[1])
    r_u = user_avg_rate.get(user)

    # if user is None or business is None or business_user_dict.get(business) is None or model_dict.get(user) is None:
    #     p = user_avg_rate.get(user) if user_avg_rate.get(user) is not None else business_avg_rate.get(business)
    #     return x, p

    if user is None:
        p = business_avg_rate.get(business)
        return x, p
    if business is None or model_dict.get(user) is None:
        p = user_avg_rate.get(user)
        return x, p
    user_rate_dict = dict(business_user_dict.get(business))
    user_sim_dict = sorted(model_dict[user], key=lambda x: x[1], reverse=True)
    candidate_users = list()
    i = 0
    while len(candidate_users) < N and i < len(user_sim_dict):
        pair = user_sim_dict[i]
        if pair[0] in user_rate_dict.keys():
            candidate_users.append((pair[0], pair[1], user_rate_dict[pair[0]]))

        i += 1

    numerator = sum((x[2] - user_avg_rate[x[0]]) * x[1] for x in candidate_users)
    denominator = sum([abs(x[1]) for x in candidate_users])
    if denominator == 0:
        p = user_avg_rate[user] if user_avg_rate.get(user) is not None else business_avg_rate.get(business)
    else:
        p = r_u + numerator / denominator
    return x, p

if __name__ == '__main__':
    start_time = time.time()
    os.environ['JAVA_HOME'] = '/home/buyi/app/jdk'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    # task3item.model task3item.predict item_based task3user.model task3user.predict user_based
    # train_file, test_file, model_file, output_file, cf_type = '/home/buyi/data/hw3/train_review.json', '/home/buyi/data/hw3/test_review.json', 'task3user.model', 'task3user.predict', "user_based"  # user_based
    train_file, test_file, model_file, output_file, cf_type = sys.argv[1:]
    sc = SparkContext('local[*]', 'task3')
    rdd_train = sc.textFile(train_file).map(json.loads)
    business_index = rdd_train.map(lambda x: x['business_id']).distinct().zipWithIndex().collectAsMap()
    user_index = rdd_train.map(lambda x: x['user_id']).distinct().zipWithIndex().collectAsMap()
    # business_index_dict = dict(rdd_train.map(lambda x: x['business_id']).distinct().zipWithIndex().collect())
    rdd_user = rdd_train.map(lambda x: (user_index[x['user_id']], x['stars'])).groupByKey().mapValues(lambda x: list(x))
    user_avg_rate = dict(rdd_user.map(lambda x: (x[0], sum(x[1]) / len(x[1]))).collect())
    rdd_business = rdd_train.map(lambda x: (business_index[x['business_id']], x['stars'])).groupByKey().mapValues(lambda x: list(x))
    business_avg_rate = dict(rdd_business.map(lambda x: (x[0], sum(x[1]) / len(x[1]))).collect())
    rdd_test = sc.textFile(test_file).map(json.loads).map(lambda x:(x["user_id"], x["business_id"])).filter(lambda x:x[0] in user_index.keys() or x[1] in business_index.keys())
    rdd_model = sc.textFile(model_file).map(json.loads)

    N = 5

    if cf_type == "item_based":
        user_business_dict = rdd_train.map(lambda x: (user_index[x['user_id']], (business_index[x['business_id']], x['stars']))).groupByKey().mapValues(lambda x: list(x)).collectAsMap()
        rdd_model_1 = rdd_model.map(lambda x: (business_index[x['b1']], business_index[x['b2']], x['sim'])).flatMap(lambda x: [(x[0], (x[1], x[2])), (x[1], (x[0], x[2]))]).groupByKey().mapValues(lambda x: list(x))
        model_dict = rdd_model_1.collectAsMap() # .filter(lambda x:x[1] in business_index.keys())
        result = rdd_test.map(
            lambda x: predict_item(x, model_dict, business_index, user_index, user_business_dict, business_avg_rate, user_avg_rate, N)).filter(lambda x:x[1] is not None).collect()

    else:
        business_user_dict = rdd_train.map(lambda x: (business_index[x['business_id']], (user_index[x['user_id']], x['stars']))).groupByKey().mapValues(
                lambda x: list(x)).collectAsMap()
        rdd_model_1 = rdd_model.map(lambda x: (user_index[x['u1']], user_index[x['u2']], x['sim'])).flatMap(
            lambda x: [(x[0], (x[1], x[2])), (x[1], (x[0], x[2]))]).groupByKey().mapValues(lambda x: list(x))

        model_dict = dict(rdd_model_1.collect())
        # print(model_dict) .filter(lambda x:x[0] in user_index.keys())
        result = rdd_test.map(
            lambda x: predict_user(x, model_dict, business_index, user_index, business_user_dict, business_avg_rate, user_avg_rate, N)).filter(lambda x:x[1] is not None).collect()
    with open(output_file, 'w') as f:
        for pair_rate in result:
            f.write(json.dumps(
                {"user_id": pair_rate[0][0], "business_id": pair_rate[0][1], "stars": pair_rate[1]}) + "\n")
    print("time: {}".format(time.time() - start_time))
