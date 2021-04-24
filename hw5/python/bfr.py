import os
import sys
import copy
import time
import json
import random
from pyspark import SparkContext


class Cluster:
    def __init__(self, id):
        self.id = id
        self.N = 0
        self.SUM = list()
        self.SUM_SQ = list()
        self.points = list()


    def get_statistics(self):
        return self.N, self.SUM, self.SUM_SQ

    def get_centroid(self):
        return [self.SUM[i]/self.N for i in range(len(self.SUM))]


    def add_point(self, point):
        self.N += 1
        if self.N == 1:
            self.SUM = point[1]
            self.SUM_SQ = [x ** 2 for x in point[1]]
        else:
            d = len(point[1])
            self.SUM = [self.SUM[i]+point[1][i] for i in range(d)]
            self.SUM_SQ = [self.SUM_SQ[i]+point[1][i]**2 for i in range(d)]
        self.points.append(point[0])

    def extend_point_list(self, point_list):
        if len(point_list) > 0:
            d = len(point_list[0][1])
            if self.N == 0:
                self.N, self.SUM, self.SUM_SQ = 0, [0]*d, [0]*d
            for p in point_list:
                self.N += 1
                self.SUM = [self.SUM[i] + p[1][i] for i in range(d)]
                self.SUM_SQ = [self.SUM_SQ[i] + p[1][i] ** 2 for i in range(d)]
                self.points.append(p[0])

    def merge_cluster(self, cluster):
        d = len(cluster.SUM)
        self.N += cluster.N
        self.SUM = [self.SUM[i] + cluster.SUM[i] for i in range(d)]
        self.SUM_SQ = [self.SUM_SQ[i] + cluster.SUM_SQ[i] for i in range(d)]
        self.points.extend(cluster.points)

    def __str__(self):
        return "ID: {}, # of points: {}".format(self.id, len(self.points))


def new_cluster():
    global c_id
    cluster = Cluster(c_id)
    c_id += 1
    return cluster

def euclidean_distance(p1, p2):
    if len(p1) != len(p2):
        return None
    else:
        squared_sum = 0
        for i in range(len(p1)):
            squared_sum += (p1[i] - p2[i]) ** 2
        return squared_sum ** 0.5

def mahalanobis_distance(v, c):
    N, SUM, SUM_SQ = c.get_statistics()
    centroid = c.get_centroid()
    # print(len(SUM), len(v))
    std = [(SUM_SQ[i] / N - (SUM[i] / N)**2) ** 0.5 for i in range(len(v))]
    std = [v if v !=0 else 1 for v in std]

    dist = sum([((v[i] - centroid[i]) / std[i]) ** 2 for i in range(len(v))]) ** 0.5
    return dist

def initialize(points_dict, k):
    centroids = []

    # centroids.append(points_dict[random.choice(list(points_dict.keys()))])
    centroids.append(random.choice(list(points_dict.keys())))

    for c_id in range(k-1):
        dist_index = dict()
        for i in points_dict.keys():
            if i not in centroids:
                point = points_dict[i]
                d = sys.maxsize
                for j in range(len(centroids)):
                    temp_dist = euclidean_distance(point, points_dict[centroids[j]])
                    d = min(d, temp_dist)
                dist_index[d] = i
        next_centroid = dist_index[max(dist_index.keys())]
        centroids.append(next_centroid)
    return centroids

def k_means(points_dict, k, k_means_threshold):
    clusters = dict()
    cluster_dict = dict()
    if len(points_dict) == 0:
        return clusters
    elif len(points_dict) <= k:
        for i,p in points_dict.items():
            c = new_cluster()
            c.add_point((i, p))
            clusters[c.id] = c
        return clusters

    points_index = list(points_dict.keys())
    # new_centroids = [points_dict[points_index[index]] for index in random.sample(range(0, len(points_index)), k)]
    centroids_index = initialize(points_dict, k)
    new_centroids = [points_dict[index] for index in centroids_index]
    difference = sys.maxsize

    while True:
        # assign points to clusters
        centroids = copy.deepcopy(new_centroids)
        for index in points_index:
            min_distance, closest_centroid = sys.maxsize, centroids[0]
            for centroid in centroids:
                distance = euclidean_distance(points_dict[index], centroid)
                if distance < min_distance:
                    min_distance, closest_centroid = distance, centroid
            cluster_dict.setdefault(closest_centroid, []).append(index)

        if difference <= k_means_threshold:
            break
        # update centroids
        difference = 0
        new_centroids = list()
        for point_index_tuple in cluster_dict.values():
            new_centroid, n_points = points_dict[point_index_tuple[0]], len(point_index_tuple)
            for p_index in point_index_tuple[1:]:
                new_centroid = [new_centroid[i] + points_dict[p_index][i] for i in range(len(new_centroid))]
            new_centroids.append(tuple([x / n_points for x in new_centroid]))
        for i in range(len(new_centroids)):
            difference += euclidean_distance(centroids[i], new_centroids[i])
        # print(difference)
        cluster_dict = dict()

    for v in cluster_dict.values():
        c = new_cluster()
        c.extend_point_list([(index, points_dict[index]) for index in v])
        clusters[c.id] = c
    return clusters

def mergeCS(CS, mahalanobis_threshold):
    new_CS = dict()
    visited_cluster_ids = list()
    for cs1 in CS.values():
        if cs1.id not in visited_cluster_ids:
            visited_cluster_ids.append(cs1.id)
            for cs2 in CS.values():
                if cs2.id not in visited_cluster_ids:
                    dist = mahalanobis_distance(cs2.get_centroid(), cs1)
                    if dist < mahalanobis_threshold:
                        cs1.merge_cluster(cs2)
                        visited_cluster_ids.append(cs2.id)
            new_CS[cs1.id] = cs1
    return new_CS




if __name__ == '__main__':
    start_time = time.time()
    os.environ['JAVA_HOME'] = '/home/buyi/app/jdk'
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

    input_path, n_cluster, out_file1, out_file2 = "/home/buyi/data/hw5/test1", "10", "cluster1", "intermediate1"
    # input_path, n_cluster, out_file1, out_file2 = "/home/buyi/data/hw5/test2", "10", "cluster2", "intermediate2"
    # input_path, n_cluster, out_file1, out_file2 = "/home/buyi/data/hw5/test3", "5", "cluster3", "intermediate3"
    # input_path, n_cluster, out_file1, out_file2 = "/home/buyi/data/hw5/test4", "8", "cluster4", "intermediate4"
    # input_path, n_cluster, out_file1, out_file2 = "/home/buyi/data/hw5/test5", "15", "cluster5", "intermediate5"
    #
    # input_path, n_cluster, out_file1, out_file2 = sys.argv[1:]
    sc = SparkContext('local', 'BFR')

    with open(out_file2, 'w') as f:
        f.write(
            "round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained\n")
    filenames = os.listdir(input_path)
    file_paths = [os.path.join(input_path, filename) for filename in filenames]
    K_MEANS_THRESHOLD = 30
    c_id = 0
    DS, CS, RS = dict(), dict(), dict()
    for r, file_path in enumerate(file_paths):
        '''
        1. Load the data points from one file.
        '''
        rdd = sc.textFile(file_path).map(lambda x: x.split(',')).map(lambda x: (x[0], tuple([float(i) for i in x[1:]])))
        dimension = len(rdd.take(1)[0][1])
        MAHALANOBIS_THRESHOLD = 2 * (dimension ** 0.5)
        points_dict = dict(rdd.collect())
        if r == 0:
            '''
            2. Run K-Means on the data points or a random subset of the data points. For the implementation, you
            will apply K-means on a subset since you cannot handle all data points in the first file. Initialize the
            algorithm with a large number of centroids (e.g., 3 or 5 times of K) and use the Euclidean distance as
            the similarity measurement.
            '''
            subset_index = random.sample(list(points_dict.keys()), int(len(points_dict) * 0.6))
            subset_dict = dict([(i, points_dict[i]) for i in subset_index])
            clusters = k_means(subset_dict, int(n_cluster)*3 , K_MEANS_THRESHOLD)
            # print(clusters)
            '''
            3. Among the result clusters from step 2, move all the clusters that contain only one or very few points
            (you define “very few”) to RS as the outliers. You will now have two groups of data points: the outlier
            data points and inlier data points.
            '''
            for c in clusters.values():
                if len(c.points) == 1:
                    key = c.points[0]
                    RS[key] = points_dict[key]
                    points_dict.pop(key)
            '''
            4. Run the clustering algorithm again to cluster the inlier data points into K clusters. Use these K clusters
            as your DS. Discard these points and generate the DS statistics.
            '''
            DS = k_means(points_dict, int(n_cluster), K_MEANS_THRESHOLD)
            # for c in clusters.values():
            #     DS[c.id] = c.get_statistics()
            '''
            5. Run the clustering algorithm again to cluster the outlier data points using a large number of clusters
            (e.g., 3 or 5 times of K). Generate CS and their statistics from the clusters with more than one data
            point and use the remaining as your new RS.
            '''
            clusters = k_means(RS, int(n_cluster) * 3, K_MEANS_THRESHOLD)
            p_dict = copy.deepcopy(RS)
            RS = dict()
            for c in clusters.values():
                if c.N == 1:
                    index = c.points[0]
                    RS[index] = p_dict[index]
                else:
                    CS[c.id] = c
        else:
            '''
            8. For the new data points, compare them to each DS using the Mahalanobis Distance and assign them
            to the nearest DS clusters if the distance is < 𝛼√𝑑 (e.g., 2√𝑑).
            '''
            for k, v in points_dict.items():
                min_dist, min_index = sys.maxsize, 0
                for i, ds in DS.items():
                    dist = mahalanobis_distance(v, ds)
                    if dist < min_dist:
                        min_dist = dist
                        min_index = i
                if min_dist < MAHALANOBIS_THRESHOLD:
                    DS[min_index].add_point((k,v))
                else:
                    '''
                    9. For the new data points which are not assigned to any DS cluster, compare them to each of the CS
                    using the Mahalanobis Distance and assign them to the nearest CS clusters if the distance is < 𝛼√𝑑
                    (e.g., 2√𝑑).
                    '''
                    min_dist, min_index = sys.maxsize, 0
                    for i, cs in CS.items():
                        # centroid = [x/ds[0] for x in ds[1]]
                        dist = mahalanobis_distance(v, cs)
                        if dist < min_dist:
                            min_dist = dist
                            min_index = i
                    if min_dist < MAHALANOBIS_THRESHOLD:
                        CS[min_index].add_point((k,v))
                    else:
                        '''
                        10. For the new data points which are not assigned to any DS or CS cluster, add them to your RS.
                        '''
                        RS[k] = v
            '''
            11. Run the clustering algorithm on the RS with a large number of centroids (e.g., 3 or 5 time of K).
            Generate CS and their statistics from the clusters with more than one data point and add them to
            your existing CS list. Use the remaining points as your new RS.
            '''
            clusters = k_means(RS, int(n_cluster) * 3, K_MEANS_THRESHOLD)
            p_dict = copy.deepcopy(RS)
            RS = dict()
            for c in clusters.values():
                if c.N == 1:
                    key = c.points[0]
                    RS[key] =  p_dict[key]
                else:
                    CS[c.id] = c
            '''
            12. Merge CS clusters that have a Mahalanobis Distance < 𝛼√𝑑 (e.g., 2√𝑑).
            '''
            CS = mergeCS(CS, MAHALANOBIS_THRESHOLD)
        '''
        If this is the last run (after the last chunk of data), merge your CS and RS clusters into the closest DS clusters.
        '''
        if r == (len(file_paths) - 1):
            for k, cs in CS.items():
                min_dist, min_index = sys.maxsize, 0
                for j, ds in DS.items():
                    # print(len(cs.get_centroid()), len(ds.SUM), len(ds.SUM_SQ))
                    dist = mahalanobis_distance(cs.get_centroid(), ds)
                    if dist < min_dist:
                        min_dist = dist
                        min_index = j
                DS[min_index].merge_cluster(cs)
            CS = dict()

        nu_point_ds, nu_point_cs = 0, 0
        for ds in DS.values():
            nu_point_ds += ds.N
        for cs in CS.values():
            nu_point_cs += cs.N
        with open(out_file2, 'a') as f:
            s = "{},{},{},{},{},{}\n".format(r,len(DS),nu_point_ds, len(CS), nu_point_cs, len(RS))
            print(s)
            f.write(s)

    points_memberships = dict()
    for i in RS.keys():
        points_memberships[int(i)] = -1
    c_i = 0
    for i, ds in DS.items():
        for p_i in ds.points:
            points_memberships[int(p_i)] = c_i
        c_i += 1
    memberships_list = sorted(points_memberships.items(), key=lambda x: x[1])
    memberships_list = sorted(memberships_list, key=lambda x: x[0])

    with open(out_file1, 'w') as f:
        memberships_dict = dict()
        for m in memberships_list:
            memberships_dict[str(m[0])] = m[1]

        f.write(json.dumps(memberships_dict))

    print("time: {}".format(time.time() - start_time))