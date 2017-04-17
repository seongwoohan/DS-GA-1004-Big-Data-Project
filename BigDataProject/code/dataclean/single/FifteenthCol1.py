# spark-submit FourteenthCol1.py /user/ly976/NYPD_Complaint_Data_Historic.csv FourteenthCol1.txt                                                                                                                      
# hfs -getmerge FourteenthCol1.txt FourteenthCol1.txt                                                                                                                                                                 


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
    Manhattan_precinct = [1,5,6,7,9,10,13,14,17,18,19,20,22,23,24,25,26,28,30,32,33,34]
    Bronx_precinct = [40,41,42,43,44,45,46,47,48,49,50,52]
    Brooklyn_precinct = [60,61,62,63,66,67,68,69,70,71,72,73,75,76,77,78,79,81,83,84,88,90,94]
    Queen_precinct = range(100,116)
    StateIsland_precinct = range(120,124)
    if input[14] == "":
        return "999\tNumber\tPrecinct\tNULL"
    elif (input[13] == "MANHATTAN" and (int(input[14]) in Manhattan_precinct)):
        return "%s\tNumber\tPrecinct\tVALID" % (input[14])
    elif (input[13] == "BRONX" and (int(input[14]) in Bronx_precinct)):
        return "%s\tNumber\tPrecinct\tVALID" % (input[14])
    elif (input[13] == "BROOKLYN" and (int(input[14]) in Brooklyn_precinct)):
        return "%s\tNumber\tPrecinct\tVALID" % (input[14])
    elif (input[13] == "STATEN ISLAND" and (int(input[14]) in StateIsland_precinct)):
        return "%s\tNumber\tPrecinct\tVALID" % (input[14])
    elif (input[13] == "QUEENS" and (int(input[14]) in Queen_precinct)):
        return "%s\tNumber\tPrecinct\tVALID" % (input[14]) 
    else:
        return "%s\tNumber\tPrecinct\tINVALID\t%s\tText\tBorough" % (input[14], input[13])


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
    invalid = crime_data.filter(lambda x: x.split("\t")[3] == "INVALID")
    result = result.union(invalid)
    result.saveAsTextFile(sys.argv[2])
    sc.stop()
