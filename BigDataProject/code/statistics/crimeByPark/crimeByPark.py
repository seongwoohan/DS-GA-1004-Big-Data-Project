import sys
import os
from pyspark import SparkContext
from operator import add
import csv

def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

def mapper(input):
    try:
	if x[17] != "":
	    return x[17]
    except:
	return None

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: inputfile outputfile"
        exit(-1)

    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1]).mapPartitions(readfile)
    crime_park = crime_data.map(lambda x: x[17]).filter(lambda x: x != None and x != "")
    crime_park_count = crime_park.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: -x[1]).map(lambda x:"%s\t%s" % (x[0], x[1])).take(20)
    sc.parallelize(crime_park_count).saveAsTextFile(sys.argv[2])
    sc.stop()
    
