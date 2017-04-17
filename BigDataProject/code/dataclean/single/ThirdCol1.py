# spark-submit ThirdCol.py /user/ly976/NYPD_Complaint_Data_Historic.csv ThirdCol.txt                                                             
# hfs -getmerge ThirdCol.txt ThirdCol.txt 

# check is it a valid, invalid or null time


import sys
import os
from pyspark import SparkContext
from operator import add
import csv
from datetime import time

def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

def isValidTime(input):
    correctTime = None
    hour1, minute1, second1 = input.split(':')
    try:
        time(int(hour1), int(minute1), int(second1))
        correctTime = True
    except ValueError:
        correctTime = False
    return correctTime

def fun(input):
    if input == "999":
        return "%s\tTime\tOccur start\tNULL" % (input)
    elif (isValidTime(input)):
        return "%s\tTime\tOccur start\tVALID" % (input)
    else:
        return "%s\tTime\tOccur start\tINVALID" % (input)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: inputfile outputfile"
        exit(-1)
    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1])
    crime_data = crime_data.mapPartitions(readfile).map(lambda x: x[2] if x[2] != "" else "999").map(fun)
    valid_count = crime_data.filter(lambda x: x.split("\t")[3] == "VALID").map(lambda x : ("Total number of VALID", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = crime_data.union(valid_count)
    invalid_count = crime_data.filter(lambda x: x.split("\t")[3] == "INVALID").map(lambda x : ("Total number of INVALID", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(invalid_count)
    null_count = crime_data.filter(lambda x: x.split("\t")[3] == "NULL").map(lambda x : ("Total number of NULL", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(null_count)
    invalid = crime_data.filter(lambda x: x.split("\t")[3] == "INVALID")
    result = result.union(invalid)
    result.saveAsTextFile(sys.argv[2])
    sc.stop()
