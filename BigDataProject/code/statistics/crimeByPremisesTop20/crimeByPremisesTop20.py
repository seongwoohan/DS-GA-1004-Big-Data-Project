import sys
import os
from pyspark import SparkContext
from operator import add
import csv


def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: inputfile outputfile"
        exit(-1)
    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1])
    crime_date = crime_data.mapPartitions(readfile).map(lambda x: (x[16], 1) if x[16] != "" else ("999", 1))
    crime_date_count = crime_date.reduceByKey(add).sortBy(lambda x: -x[1]).map(lambda x: "%s\t%s" % (x[0], x[1])).take(20)
    sc.parallelize(crime_date_count).saveAsTextFile(sys.argv[2])
    sc.stop()

