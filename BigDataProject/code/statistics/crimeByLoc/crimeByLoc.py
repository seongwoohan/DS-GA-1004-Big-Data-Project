# spark-submit crimeByLoc.py /user/ly976/NYPD_Complaint_Data_Historic.csv crimeByLoc.txt                                                                                                                      
# hfs -getmerge crimeByLoc.txt crimeByLoc.txt                                                                                                                                                                 

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
    crime_data = crime_data.mapPartitions(readfile).map(lambda x : ((x[15], x[16]), 1) if (x[15] != "" or x[16] != "") else (("999", "999"), 1)).reduceByKey(add).map(lambda x : "%s\t%s\t%s" % (x[0][0], x[0][1], x[1]))
    crime_data.saveAsTextFile(sys.argv[2])
    sc.stop()
