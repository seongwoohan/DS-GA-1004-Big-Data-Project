import sys
import os
from pyspark import SparkContext
from operator import add
import csv

def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

def filter_OCC(input):
    if input[6] == '' or input[7] == '':
	return False
    else:
	return True

def map_OCC(input):
    return (input[6], input[7])

def reduce_OCC(a, b):
    return (a, b)

if __name__ == "__main__":
    if len(sys.argv) != 3:
	print "usage: inputfile outputfile"
	exit(-1)

    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1]).mapPartitions(readfile)
    crime_OCC = crime_data.filter(filter_OCC).map(map_OCC).distinct()
    crime_OCC = crime_OCC.groupByKey().filter(lambda x: len(x[1]) >1).map(lambda x: (x[0], list(x[1])))
    crime_OCC.map(lambda x: "%s\t%s" % (x[0], x[1])).saveAsTextFile(sys.argv[2])
    sc.stop()

