# spark-submit SecondCol1.py /user/ly976/NYPD_Complaint_Data_Historic.csv SecondCol1.txt                                                                      
# hfs -getmerge SecondCol1.txt SecondCol1.txt                                                                                                                 

# Is it valid date                                                                                                                                          


import sys
import os
from pyspark import SparkContext
from operator import add
import csv
from datetime import date

def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

def isValidDate(input):
    correctDate = None
    month, day, year = input.split("/")
    try:
        date(int(year), int(month), int(day))
        if (int(year) > 1800 and int(year) < 2018):
            correctDate = True
        else:
            correctDate = False
    except ValueError:
        correctDate = False
    return correctDate

def fun(input):
    if input == "999":
        return "%s\tDate\tOccur start\tNULL" % (input)
    elif (isValidDate(input)):
        return "%s\tDate\tOccur start\tVALID" % (input)
    else:
        return "%s\tDate\tOccur start\tINVALID" % (input)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: inputfile outputfile"
        exit(-1)
    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1])
    crime_data = crime_data.mapPartitions(readfile).map(lambda x: x[1] if x[1] != "" else "999").map(fun)
    valid_count = crime_data.filter(lambda x: x.split("\t")[3] == "VALID").map(lambda x : ("Total number of VALID", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = crime_data.union(valid_count)
    invalid_count = crime_data.filter(lambda x: x.split("\t")[3] == "INVALID").map(lambda x : ("Total number of INVALID", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(invalid_count)
    null_count = crime_data.filter(lambda x: x.split("\t")[3] == "NULL").map(lambda x : ("Total number of NULL",  1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(null_count)
    invalid = crime_data.filter(lambda x: x.split("\t")[3] == "INVALID")
    result = result.union(invalid)
    result.saveAsTextFile(sys.argv[2])
    sc.stop()
