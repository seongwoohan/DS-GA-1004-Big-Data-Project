# spark-submit FourthCol1.py /user/ly976/NYPD_Complaint_Data_Historic.csv FourthCol1.txt                                                                                  
# hfs -getmerge FourthCol1.txt FourthCol1.txt                                                                                                                             

#check whether the occur end date is valid and after occur start date
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

def isValidDateTime(input):
    correctDate = None
    if input[1] == "":
        month2, day2, year2 = input[3].split('/')
        try:
            d2 = date(int(year2), int(month2), int(day2))
            correctDate = True
        except ValueError:
            correctDate = False
    else:
        month1, day1, year1 = input[1].split('/')
        month2, day2, year2 = input[3].split('/')
        try:
            d1 = date(int(year1), int(month1), int(day1))
            d2 = date(int(year2), int(month2), int(day2))
            diff = (d2 - d1).days
            if (diff >= 0 and diff <= 73000):
                correctDate = True
            else:
                correctDate = False
        except ValueError:
            correctDate = False
    return correctDate

def fun(input):
    if input[3] == "":
        return "999\tDate\tReport\tNULL"
    elif (isValidDateTime(input)):
        return "%s\tDate\tReport\tVALID" % (input[3])
    else:
        return "%s\tDate\tReport\tINVALID\t%s\tDate\tOccur start" % (input[3], input[1])
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
    null_count = crime_data.filter(lambda x: x.split("\t")[3] == "NULL").map(lambda x : ("Total number of NULL", 1)).reduceByKey(add).map(lambda x: "%s\t%s" % (x[0], x[1]))
    result = result.union(null_count)
    invalid = crime_data.filter(lambda x: x.split("\t")[3] == "INVALID")
    result = result.union(invalid)
    result.saveAsTextFile(sys.argv[2])
    sc.stop()
