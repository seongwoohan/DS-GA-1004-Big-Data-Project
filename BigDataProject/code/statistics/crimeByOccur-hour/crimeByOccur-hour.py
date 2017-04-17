# spark-submit crimeByOccur-hour.py /user/ly976/NYPD_Complaint_Data_Historic.csv crimeByOccur-hour.txt
# hfs -getmerge crimeByOccur-hour.txt crimeByOccur-hour.txt

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
    crime_date = crime_data.mapPartitions(readfile).map(lambda x: (x[2].split(":")[0], 1) if x[2] != "" else ("999", 1))
    crime_date_count = crime_date.reduceByKey(add).sortByKey(ascending=True, numPartitions=None, keyfunc = lambda x: int(x)).map(lambda x: "%s\t%s" % (x[0]+ ":00:00 - " + str(int (x[0]) + 1) + ":00:00", x[1]))
    crime_date_count.saveAsTextFile(sys.argv[2])
    sc.stop()
