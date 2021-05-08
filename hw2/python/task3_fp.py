import os
import time
import sys
from itertools import combinations
from pyspark import SparkContext


class Node:
    def __init__(self, value, count):
        self.value = value
        self.count = count
        self.link = None
        self.parent = None
        self.children = []

    def increment(self, num):
        self.count += num

    def __str__(self):
        return "{}:{}".format(self.value, self.count)



def generate_item_link_table(frequent_items):
    item_link_table = dict()
    for pair in frequent_items:
        item_link_table[pair[0]] = []
    return item_link_table


def reformat_baskets(basket, frequent_items):
    item_dict =dict()
    frequent_items_dict = dict()
    for pair in frequent_items:
        frequent_items_dict[pair[0]] = pair[1]
    for item in list(basket):
        if item in frequent_items_dict.keys():
            item_dict[item] = frequent_items_dict[item]
    b_list = list()
    for k,v in item_dict.items():
        b_list.append((k,v))
    sorted_list = sorted(b_list, key=lambda x:x[1], reverse=True)
    return [i[0] for i in sorted_list]

def insert_basket(tree, basket, item_link_table):
    basket = list(basket)
    # print(basket)
    if len(basket) > 0:
        isChild = False
        for child in tree.children:
            if basket[0] == child.value:
                isChild = True
                child.count += 1
                insert_basket(child, basket[1:],item_link_table)
                break
        if isChild == False:
            child = Node(basket[0], 1)
            child.parent = tree
            tree.children.append(child)
            item_link_table[basket[0]].append(child)
            insert_basket(child, basket[1:], item_link_table)
    else:
        return

def generate_conditional_pattern_base(item_link_table):
    conditional_pattern = dict()
    for k in item_link_table.keys():
        for node in item_link_table[k]:
            temp_list = []
            count = node.count
            while node.parent.value != 'root':
                node = node.parent
                temp_list.insert(0, node.value)
            conditional_pattern.setdefault(k, []).append((temp_list, count))
    return conditional_pattern



def get_candidate_count(pair_list, k, support):
    items_count = dict()
    frequent_list = list()
    for item_pair in pair_list:
        item_list, count = item_pair
        for item in item_list:
            items_count[item] = items_count.get(item, 0) + count
    for key in items_count.keys():
        if items_count[key] >= support:
            frequent_list.append(key)
    itemset_count = dict()
    for item_pair in pair_list:
        new_list = []
        item_list, count = item_pair
        for item in item_list:
            if item in frequent_list:
                new_list.append(item)
        for i in range(1, len(new_list) + 1):
            for candidate in combinations(new_list, i):
                itemset_count[candidate] = itemset_count.get(candidate, 0) + count
    frequent_itemset = dict()
    for key, value in itemset_count.items():
        if value >= support:
            tmp_key = list(key)
            tmp_key.append(k)
            frequent_itemset[tuple(sorted(tmp_key))] = value
    return [(k,v) for k,v in frequent_itemset.items()]

if __name__ == '__main__':
    # os.environ['JAVA_HOME'] = "/home/buyi/app/jdk"
    # os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    start_time = time.time()

    sc = SparkContext("local[*]", 'task3_fp')
    filter_threshold, support, input_file_path, output_file_path = sys.argv[1:]
    # filter_threshold, support, input_file_path, output_file_path = "70", "50", "user_business.csv", "task3_my_ans"

    rdd = sc.textFile(input_file_path).cache()
    first = rdd.first()
    if str(first).find("user_id") != -1:
        rdd = rdd.filter(lambda x: x != first)
    rdd = rdd.map(lambda x: (x.split(',')[0].strip(), x.split(',')[1].strip()))
    rdd_basket = rdd.groupBy(lambda x: x[0]).mapValues(lambda x: list(set([i[1] for i in x]))) \
        .map(lambda x: x[1]).filter(lambda x: len(x) > int(filter_threshold)).persist()

    rdd = rdd_basket.flatMap(lambda x: [(i, 1) for i in x]).reduceByKey(lambda x,y: x+y).filter(lambda x:x[1]>=int(support)).sortBy(lambda x:x[1], ascending=False)

    frequent_items = rdd.collect()
    rdd1 = rdd_basket.map(lambda x: reformat_baskets(x, frequent_items))
    item_link_table = generate_item_link_table(frequent_items)
    # print(item_link_table)
    root = Node("root", 0)
    for b in rdd1.collect():
        insert_basket(root, b, item_link_table)
    # print(item_link_table)
    conditional_pattern = generate_conditional_pattern_base(item_link_table)

    pattern_table = [(k,v) for k,v in conditional_pattern.items()]
    rdd = sc.parallelize(pattern_table).flatMap(lambda x:get_candidate_count(x[1], x[0], int(support)))

    frequents = list()
    for item in item_link_table.keys():
        frequents.append(str((item,)))
    for pair in rdd.collect():
        frequents.append(str(tuple(sorted(pair[0]))))
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