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
    crime_data = sc.textFile(sys.argv[1]).mapPartitions(readfile)
    crime_date_count = crime_date.count()
    crime_date_count.saveAsTextFile(sys.argv[2])
    sc.stop()


