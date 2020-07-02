#!/usr/bin/env python

from __future__ import division
import argparse
import json
import math
from os.path import dirname, realpath
from pyspark import SparkContext
import time
import os

VIRTUAL_COUNT = 10
PRIOR_CORRELATION = 0.0
THRESHOLD = 0.5


##### Metric Functions ############################################################################
def correlation(n, sum_x, sum_y, sum_xx, sum_yy, sum_xy):
    # http://en.wikipedia.org/wiki/Correlation_and_dependence
    numerator = n * sum_xy - sum_x * sum_y
    denominator = math.sqrt(n * sum_xx - sum_x * sum_x) * math.sqrt(n * sum_yy - sum_y * sum_y)
    if denominator == 0:
        return 0.0
    return numerator / denominator

def regularized_correlation(n, sum_x, sum_y, sum_xx, sum_yy, sum_xy, virtual_count, prior_correlation):
    unregularized_correlation_value = correlation(n, sum_x, sum_y, sum_xx, sum_yy, sum_xy)
    weight = n / (n + virtual_count)
    return weight * unregularized_correlation_value + (1 - weight) * prior_correlation

def cosine_similarity(sum_xx, sum_yy, sum_xy):
    # http://en.wikipedia.org/wiki/Cosine_similarity
    numerator = sum_xy
    denominator = (math.sqrt(sum_xx) * math.sqrt(sum_yy))
    if denominator == 0:
        return 0.0
    return numerator / denominator

def jaccard_similarity(n_common, n1, n2):
    # http://en.wikipedia.org/wiki/Jaccard_index
    numerator = n_common
    denominator = n1 + n2 - n_common
    if denominator == 0:
        return 0.0
    return numerator / denominator
#####################################################################################################

##### util ##########################################################################################
def combinations(iterable, r):
    # http://docs.python.org/2/library/itertools.html#itertools.combinations
    # combinations('ABCD', 2) --> AB AC AD BC BD CD
    # combinations(range(4), 3) --> 012 013 023 123
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = list(range(r))
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(list(range(r))):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1, r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)
#####################################################################################################


def parse_args():
    parser = argparse.ArgumentParser(description='MapReduce similarities')
    parser.add_argument('-d', help='path to data directory', default='../data/mapreduce/recommendations/small/')
    parser.add_argument('-u', help='your gcp username, only set this when you submit a job to a cluster', default='')
    parser.add_argument('-n', help='number of data slices', default=128)
    parser.add_argument('-o', help='path to output JSON', default="output")
    return parser.parse_args()

# Feel free to create more mappers and reducers.
def mapper0(record):
    # TODO
    pass

def reducer(a, b):
    # TODO
    pass

def mapper1(record):
    # Hint: 
    # INPUT:
    #   record: (key, values)
    #     where -
    #       key: movie_id
    #       values: a list of values in the line
    # OUTPUT:
    #   [(key, value), (key, value), ...]
    #     where -
    #       key: movie_title
    #       value: [(user_id, rating)]
    #
    # TODO
    pass

def mapper2(record):
    # TODO
    pass

def mapper3(record):
    # TODO
    pass

def mapper4(record):
    # TODO
    pass

def mapper5(record):
    # TODO
    pass

def main():
    args = parse_args()
    sc = SparkContext()

    with open(args.d + '/movies.dat', 'r') as mlines:
        data = [line.rstrip() for line in mlines]
    with open(args.d + '/ratings.dat', 'r') as rlines:
        data += [line.rstrip() for line in rlines]

    # Implement your mapper and reducer function according to the following query.
    # stage1_result represents the data after it has been processed at the second
    # step of map reduce, which is after mapper1.
    stage1_result = sc.parallelize(data, args.n).map(mapper0).reduceByKey(reducer) \
                                                    .flatMap(mapper1).reduceByKey(reducer)
    if not os.path.exists(args.o):
        os.makedirs(args.o)

    # Store the stage1_output
    with open(args.o  + '/netflix_stage1_output.json', 'w') as outfile:
        json.dump(stage1_result.collect(), outfile, separators=(',', ':'))

    # TODO: continue to build the pipeline
    # Pay attention to the required format of stage2_result
    # stage2_result = stage1_result.

    # Store the stage2_output
    with open(args.o  + '/netflix_stage2_output.json', 'w') as outfile:
        json.dump(stage2_result.collect(), outfile, separators=(',', ':'))

    # TODO: continue to build the pipeline
    # final_result = stage2_result.

    with open(args.o + '/netflix_final_output.json', 'w') as outfile:
        json.dump(final_result, outfile, separators=(',', ':'))

    sc.stop()

    # Don't modify the following code
    if args.u != '':
        os.system("gsutil mv {0} gs://{1}".format(args.o + '/*.json', args.u))


if __name__ == '__main__':
    main()
