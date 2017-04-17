# spark-submit FirstCol.py /user/ly976/NYPD_Complaint_Data_Historic.csv FirstCol.txt                                                                                                                      
# hfs -getmerge FirstCol.txt FirstCol.txt                                                                                                                                                                 


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
    if input == "999":
        return "%s\tNUMBER\tID\tNULL\t%s" % (input[0], input[1])
    elif (input[1] == 1):
        return "%s\tNUMBER\tID\tVALID\t%s" % (input[0], input[1])
    else:
        return "%s\tNUMBER\tID\tINVALID\t%s" % (input[0], input[1])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: inputfile outputfile"
        exit(-1)
    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1])
    crime_data = crime_data.mapPartitions(readfile).map(lambda x: (x[0], 1) if x[0] != "" else ("999", 1)).reduceByKey(add).map(fun)
    valid_count = crime_data.filter(lambda x: x.split("\t")[3] == "VALID").map(lambda x : ("Total number of VALID", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = crime_data.union(valid_count)
    invalid_count = crime_data.filter(lambda x: x.split("\t")[3] == "INVALID").map(lambda x : ("Total number of INVALID", x.split("\t")[4])).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(invalid_count)
    null_count = crime_data.filter(lambda x: x.split("\t")[3] == "NULL").map(lambda x : ("Total number of NULL",  x.split("\t")[4])).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(null_count)
    result.saveAsTextFile(sys.argv[2])
    sc.stop()
