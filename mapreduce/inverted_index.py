#!/usr/bin/env python

import argparse
import json
import os
from pyspark import SparkContext

def parse_args():
    parser = argparse.ArgumentParser(description='MapReduce inverted index (Problem 1)')
    parser.add_argument('-d', help='path to data file', default='../data/mapreduce/books_small.json')
    parser.add_argument('-n', help='number of data slices', default=128)
    parser.add_argument('-o', help='path to output JSON', default='output')
    return parser.parse_args()

# Feel free to create more mappers and reducers.
def reducer1(a, b):
    # TODO
    return b

def mapper2(record):
    # TODO
    return (record[0], record[1]) 

def reducer2(a, b):
    # TODO
    list = []
    list = list + a
    list = list + b
    return list


# Do not modify
def main():
    args = parse_args()
    sc = SparkContext()

    with open(args.d, 'r') as infile:
        data = [json.loads(line) for line in infile]
        mid_result =[]
        for line in data:
            line_result = sc.parallelize(line[1].split(" "), 32) \
                            .map(lambda word: [word, line[0].split("\t")]) \
                            .reduceByKey(reducer1) \
                            .collect() \
            
            mid_result = mid_result + line_result
        ####################
        inverted_index_result = sc.parallelize(mid_result, 128) \
                                  .map(mapper2) \
                                  .reduceByKey(reducer2) \
                                  .sortByKey(True) \
                                  .collect()      
        #print(inverted_index_result)                          


    sc.stop()

    if not os.path.exists(args.o):
        os.makedirs(args.o)

    with open(args.o + '/output_inverted_index.json', 'w') as outfile:
        json.dump(inverted_index_result, outfile, indent=4)



if __name__ == '__main__':
    main()
