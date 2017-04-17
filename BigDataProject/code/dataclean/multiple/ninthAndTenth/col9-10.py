import sys
import os
from pyspark import SparkContext
from operator import add
import csv

def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

def filter_ICC(input):
    if input[8] == '' or input[9] == '':
        return False
    else:
        return True

def map_ICC(input):
    return (input[6], input[7])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: inputfile outputfile"
        exit(-1)

    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1]).mapPartitions(readfile)
    crime_OCC = crime_data.filter(filter_ICC).map(map_ICC).distinct()
    crime_OCC = crime_OCC.groupByKey().filter(lambda x: len(x[1]) >1).map(lambda x: (x[0], list(x[1])))
    crime_OCC.map(lambda x: "%s\t%s" % (x[0], x[1])).saveAsTextFile(sys.argv[2])
    sc.stop()

