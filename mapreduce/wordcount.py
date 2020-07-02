#!/usr/bin/env python3
from operator import add
from pyspark import SparkContext

"""
This is a very simple sample of how to use the MapReduce framework PySpark.
In this sample, the goal is to print out the word count per word in the input
text.

Feel free to play around with this file to understand how PySpark / all the
components work together!
"""

# mapper is a function that maps its input to an associated value. In this example,
# because the goal is to count the occurence of a word, it is a good idea to map
# each occurence of a word to a count of 1.
def mapper(word):
    return (word, 1)

# reducer is a function that aggregates all the outputs from the mapper function
# together. In this example, a and b would be some count of occurences of the same word,
# and by pairwise the number of occurrences of that word until we have considered
# all the tuples with that word being the key, we will have the true count of occurences
# of that word.
def reducer(a, b):
    return a + b

def main():
    # input sentence, then turn it into a list of words
    sentence = "MapReduce Map Reduce Mapper Reducer MapReduce"
    data = sentence.split()
    # create a new instance of Spark pipeline
    sc = SparkContext()
    # Turning off info messages being displayed in spark control
    sc.setLogLevel("OFF")

    """
    - parallelize will partition the work in the data list into 128 slices
    - the pipeline will then run map, using the mapper function passed in, on all slices
    from the original list, all in parallel!
    - after being done with mapping, it will "aggregate" all the outputs from the mapper
    function using the reducer function, in parallel, as it the mapper function returns
    the result from different slices.
    - We will then sort the results by key, and collect the result, and return a list!
    """
    # making a list of (word, associated_count) using lambda functions and operators
    word_count = sc.parallelize(data, 128) \
                   .map(lambda word: (word, 1)) \
                   .reduceByKey(add) \
                   .sortByKey(True) \
                   .collect()
    print(word_count)

    # using self defined methods: mapper and reducer, which you will implement.
    word_count = sc.parallelize(data, 128) \
                   .map(mapper) \
                   .reduceByKey(reducer) \
                   .sortByKey(True) \
                   .collect()
    print(word_count)
    # Stopping the Spark pipeline
    sc.stop()

if __name__ == '__main__':
    main()
