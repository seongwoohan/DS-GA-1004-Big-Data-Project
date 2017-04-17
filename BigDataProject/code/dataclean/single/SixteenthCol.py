# spark-submit FifteenthCol.py /user/ly976/NYPD_Complaint_Data_Historic.csv FifteenthCol.txt                                                                                                                      
# hfs -getmerge FifteenthCol.txt FifteenthCol.txt                                                                                                                                                                 

import sys
import os
from pyspark import SparkContext
from operator import add
import csv


def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

def fun(input):
    if (input[15]).strip() == "":
        return "999\tText\tPremises\tNULL"
    elif (input[15] == "INSIDE" or input[15] == "OPPOSITE OF" or input[15] == "FRONT OF" or input[15] == "REAR OF" or input[15] == "OUTSIDE"):
        return "%s\tText\tPremises\tVALID" % (input[15])
    else:
        return "%s\tText\tPremises\tINVALID" % (input[15])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: inputfile outputfile"
        exit(-1)
    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1])
    crime_data = crime_data.mapPartitions(readfile).map(fun)
    valid_count = crime_data.filter(lambda x: x.split("\t")[3] == "VALID").map(lambda x : ("Total number of VALID", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = crime_data.union(valid_count)
    invalid_count = crime_data.filter(lambda x: x.split("\t")[3] == "INVALID").map(lambda x : ("Total number of INVALID", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(invalid_count)
    null_count = crime_data.filter(lambda x: x.split("\t")[3] == "NULL").map(lambda x : ("Total number of NULL",  1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(null_count)
    result.saveAsTextFile(sys.argv[2])
    sc.stop()
