import os
import sys
import time
import copy
from itertools import combinations

from pyspark import SparkContext

class Node:
    def __init__(self, name):
        self.name = name
        self.value = 1
        self.parents = set()
        self.children = set()

def get_communities(nodes, edges):
    visited_nodes = set()
    communities = list()
    for n in nodes:
        if len(visited_nodes) == len(nodes):
            break
        elif n in visited_nodes:
            continue
        else:
            community = set()
            candidate_nodes = {n}
            while len(candidate_nodes) > 0:
                for nn in copy.deepcopy(candidate_nodes):
                    for e in copy.deepcopy(edges):
                        if nn in e:
                            if e[0] not in visited_nodes:
                                candidate_nodes.add(e[0])
                                visited_nodes.add(e[0])
                                community.add(e[0])
                            if e[1] not in visited_nodes:
                                candidate_nodes.add(e[1])
                                visited_nodes.add(e[1])
                                community.add(e[1])
                            edges.remove(e)
                    candidate_nodes.remove(nn)
            if len(community) == 0: communities.append({n})
            else: communities.append(community)
    return communities


def calculate_graph_betweenness(communities, edges):
    edge_betweenness = dict()
    for c in communities:
        edges_dict = dict()
        c_edges = set()
        for edge in copy.deepcopy(edges):
            if edge[0] in c and edge[1] in c:
                c_edges.add(edge)
                edges.remove(edge)
        for e in c_edges:
            edges_dict.setdefault(e[0], []).append(e[1])
            edges_dict.setdefault(e[1], []).append(e[0])

        for node in c:
            betweenness = get_tree_betweenness(node, copy.deepcopy(c_edges), copy.deepcopy(edges_dict))
            for item in betweenness:
                edge_betweenness[item[0]] = edge_betweenness.setdefault(item[0], 0) + item[1]
    return list(map(lambda x: (x[0], x[1] / 2), edge_betweenness.items()))

def get_tree_betweenness(root, edges, edges_dict):
    root_node = Node(root)
    level_dict = dict()
    level = 0
    level_dict[level] = {root_node}
    node_dict = {root_node.name: root_node}
    while len(level_dict[level]) > 0:
        level += 1
        level_dict[level] = set()
        for node in level_dict[level - 1]:
            children = set([(node_dict[x] if x in node_dict.keys() else Node(x)) for x in edges_dict[node.name]])
            for c in children:
                node_dict[c.name] = c
                edges_dict[node.name].remove(c.name)
                edges_dict[c.name].remove(node.name)
                if c in level_dict[level - 1]:
                    continue
                node.children.add(c)
                c.parents.add(node)
                level_dict[level].add(c)

    edges_betweenness = tree_betweenness(edges, level_dict)
    return list(edges_betweenness.items())

def tree_betweenness(edges, level_dict):
    nodes_betweeness = dict()
    edges_betweenness = dict(map(lambda x:(x, 0), edges))
    levels = sorted(level_dict.items(), key=lambda x:x[0], reverse=True)
    for level in levels:
        for node in level[1]:
            if len(node.children) == 0:
                nodes_betweeness[node.name] = 1
            else:
                children = node.children
                children_links = [tuple(sorted([node.name, child.name])) for child in children]
                nodes_betweeness[node.name] = 1 + sum([edges_betweenness[link] for link in children_links])
            if len(node.parents) != 0:
                if len(list(node.parents)[0].parents) > 0:
                    weights_dict = dict([(n, len(n.parents)) for n in node.parents])
                else:
                    weights_dict = dict([(n, 1) for n in node.parents])
                score = 1 / sum(weights_dict.values()) * nodes_betweeness[node.name]
                for p in node.parents:
                    link = tuple(sorted([node.name, p.name]))
                    edges_betweenness[link] += score * weights_dict[p]
    return edges_betweenness

def generate_tree(root_node, edges_dict):
    level_dict = dict()
    level = 0
    level_dict[level] = {root_node}
    node_dict = {root_node.name: root_node}
    while len(level_dict[level]) > 0:
        level += 1
        level_dict[level] = set()
        for node in level_dict[level-1]:
            children = set([(node_dict[x] if x in node_dict.keys() else Node(x)) for x in edges_dict[node.name]])
            for c in children:
                node_dict[c.name] = c
                edges_dict[node.name].remove(c.name)
                edges_dict[c.name].remove(node.name)
                if c in level_dict[level-1]:
                    continue
                node.children.add(c)
                c.parents.add(node)
                level_dict[level].add(c)
    return level_dict


def calculate_modularity(commuities, m, nodes_degree, adjacent_dict):
    modularity = 0
    for c in commuities:
        tmp_mod = 0

        for pair in combinations(c, 2):
            pair = tuple(sorted(pair))
            aij = adjacent_dict[pair]
            tmp_mod += aij - nodes_degree[pair[0]] * nodes_degree[pair[1]] / 2 / m
        modularity += tmp_mod
    return modularity / 2 / m

if __name__ == '__main__':
    start_time = time.time()
    os.environ['JAVA_HOME'] = "/home/buyi/app/jdk"
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

    filter_threshold, input_file_path, betweenness_output_file_path, community_output_file_path = '7', '/home/buyi/data/hw4/ub_sample_data.csv', 'task2_1_ans', 'task2_2_ans'
    # filter_threshold, input_file_path, betweenness_output_file_path, community_output_file_path = sys.argv[1:]
    sc = SparkContext("local[3]", "task2")

    rdd = sc.textFile(input_file_path)
    header = rdd.first()
    rdd1 = rdd.filter(lambda x:x != header).map(lambda x:x.split(','))
    user_index = rdd1.map(lambda x:x[0]).distinct().zipWithIndex().collectAsMap()
    index_user = dict([(x[1], x[0]) for x in user_index.items()])
    business_index = rdd1.map(lambda x:x[1]).distinct().zipWithIndex().collectAsMap()
    rdd_user_profile = rdd1.map(lambda x:(user_index[x[0]],business_index[x[1]])).groupByKey().mapValues(lambda x:list(x))
    user_profile_dict = rdd_user_profile.collectAsMap()
    unique_user = user_index.values()
    pairs = combinations(unique_user, 2)
    nodes, edges = set(), set()
    for p in pairs:
        if len(set(user_profile_dict[p[0]]).intersection(user_profile_dict[p[1]])) >= int(filter_threshold):
            nodes.add(p[0])
            nodes.add(p[1])
            edges.add(tuple(sorted(p)))
    # [{38, 2, 213, 6},{584, 1060}, ...]
    communities = get_communities(nodes, copy.deepcopy(edges))

    betweenness_edges = calculate_graph_betweenness(communities, copy.deepcopy(edges))
    sorted_betweenness_edges = sorted(betweenness_edges, key=lambda x:x[1], reverse=True)

    with open(betweenness_output_file_path, 'w') as f:
        betweenness_edges = list(map(lambda x:(tuple(sorted([index_user[x[0][0]], index_user[x[0][1]]])), x[1]), betweenness_edges))
        for pair_betweenness in sorted(sorted(betweenness_edges, key=lambda x:x[0]), key=lambda x:x[1], reverse=True):
            f.write("('{}', '{}'), {}\n".format(pair_betweenness[0][0], pair_betweenness[0][1], pair_betweenness[1]))

    nodes_degree = dict()
    for e in edges:
        nodes_degree[e[0]] = nodes_degree.setdefault(e[0], 0) + 1
        nodes_degree[e[1]] = nodes_degree.setdefault(e[1], 0) + 1
    adjacent_dict = dict()
    for pair in combinations(nodes_degree.keys(), 2):
        pair = tuple(sorted(pair))
        adjacent_dict[pair] = 1 if pair in edges else 0
    modularity, max_modularity = -1, -1
    m = len(sorted_betweenness_edges)
    pre_communities = copy.deepcopy(communities)
    while modularity >= max_modularity:
        pre_communities = copy.deepcopy(communities)
        max_modularity = modularity
        highest_betweenness = sorted_betweenness_edges[0][1]
        for e in copy.deepcopy(sorted_betweenness_edges):
            if e[1] == highest_betweenness:
                sorted_betweenness_edges.remove(e)
        edges = list(map(lambda x: x[0], sorted_betweenness_edges))
        communities = get_communities(nodes, copy.deepcopy(edges))

        modularity = calculate_modularity(communities, m, nodes_degree, adjacent_dict)
        betweenness_edges = calculate_graph_betweenness(communities, edges)
        sorted_betweenness_edges = sorted(betweenness_edges, key=lambda x:x[1], reverse=True)
    sorted_communities = sorted(sorted(list(map(lambda x:sorted([index_user[i] for i in x]), pre_communities))), key=lambda x:len(x))
    # print(sorted_communities)
    with open(community_output_file_path, 'w') as f:
        for c in sorted_communities:
            f.write(','.join(["'"+x+"'" for x in c])+'\n')
    print("time: {}".format(time.time() - start_time))






