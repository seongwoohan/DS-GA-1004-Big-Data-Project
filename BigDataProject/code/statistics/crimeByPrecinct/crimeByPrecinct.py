# spark-submit crimeByPrecinct.py /user/ly976/NYPD_Complaint_Data_Historic.csv crimeByPrecinct.txt
# hfs -getmerge crimeByPrecinct.txt crimeByPrecinct.txt

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
    crime_date = crime_data.mapPartitions(readfile).map(lambda x: ((x[13], x[14]), 1) if (x[13] != "" and x[14] != "") else (("999", "999"), 1))
    crime_date_count = crime_date.reduceByKey(add).sortByKey().map(lambda x: "%s\t%s\t%s" % (x[0][0], x[0][1], x[1]))
    crime_date_count.saveAsTextFile(sys.argv[2])
    sc.stop()
