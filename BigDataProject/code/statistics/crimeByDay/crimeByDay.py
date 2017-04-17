import sys
import os
from pyspark import SparkContext
from operator import add
import csv

def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

def formatDate(input):
    try:
        split = input.split("/")
        return "%s%s%s" % (split[2], split[0], split[1])
    except:
	return "999"

if __name__ == "__main__":
    if len(sys.argv) != 3:
	print "usage: inputfile outputfile"
	exit(-1)

    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1]).mapPartitions(readfile)
    crime_date = crime_data.map(lambda x:x[1] if x[1] != "" else x[3])
    crime_date_count = crime_date.map(formatDate).map(lambda x: (x, 1)).reduceByKey(add).sortByKey().map(lambda x: "%s\t%s" % (x[0], x[1]))
    crime_date_count.saveAsTextFile(sys.argv[2])
    sc.stop()


    
