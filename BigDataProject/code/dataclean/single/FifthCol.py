# spark-submit FifthCol.py /user/ly976/NYPD_Complaint_Data_Historic.csv FifthCol.txt                                                                                  
# hfs -getmerge FifthCol.txt FifthCol.txt                                                                                                                             

#check whether the occur end date and time is valid and after occur start date and time
import sys
import os
from pyspark import SparkContext
from operator import add
import csv
from datetime import time
import datetime

def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

def isValidDateTime(input):
    correctDate = None
    if (input[3] == "" or input[1] == "" or input[2] == ""):
        hour2, minute2, second2 = input[4].split(':') 
        try:
            time(int(hour2), int(minute2), int(second2))
            correctDate = True
        except ValueError:
            correctDate = False
    else:
        month1, day1, year1 = input[1].split('/')
        hour1, minute1, second1 = input[2].split(':')
        month2, day2, year2 = input[3].split('/')
        hour2, minute2, second2 = input[4].split(':')
        try:
            d1 = datetime.datetime(int(year1), int(month1), int(day1), int(hour1), int(minute1), int(second1))
            d2 = datetime.datetime(int(year2), int(month2), int(day2), int(hour2), int(minute2), int(second2))
            if (d1 <= d2):
                correctDate = True
            else:
                correctDate = False
        except ValueError:
            correctDate = False
    return correctDate

def fun(input):
    if input[4] == "":
        return "999\tTime\tOccur end\tNULL"
    elif (isValidDateTime(input)):
        return "%s\tTime\tOccur end\tVALID" % (input[4])
    else:
        return "%s\t%s\tDateTime\tOccur end\tINVALID%s\t%s\tDateTime\tOccur start" % (input[3], input[4], input[1], input[2])
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: inputfile outputfile"
        exit(-1)
    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1])
    crime_data = crime_data.mapPartitions(readfile).map(fun)
    valid_count = crime_data.filter(lambda x: x.split("\t")[3] == "VALID").map(lambda x : ("Total number of VALID", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = crime_data.union(valid_count)
    invalid_count = crime_data.filter(lambda x: x.split("\t")[3] == "Occur end").map(lambda x : ("Total number of INVALID", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(invalid_count)
    null_count = crime_data.filter(lambda x: x.split("\t")[3] == "NULL").map(lambda x : ("Total number of NULL", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(null_count)
    result.saveAsTextFile(sys.argv[2])
    sc.stop()
