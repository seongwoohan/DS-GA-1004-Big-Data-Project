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
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: inputfile outputfile"
        exit(-1)

    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1])
    crime_data_min = crime_data.mapPartitions(readfile).filter(lambda x : x[19] != "" and x[20] != "" and x[21] != "" and  x[22] != "").map(lambda x : [float(x[19]), float(x[20]), float(x[21]), float(x[22])]).min()
    crime_data_max = crime_data.mapPartitions(readfile).filter(lambda x : x[19] != "" and x[20] != "" and x[21] != "" and  x[22] != "").map(lambda x : [float(x[19]), float(x[20]), float(x[21]), float(x[22])]).max()
    crime_data = ["X_coordinates : %s - %s"  % (crime_data_min[0], crime_data_max[0]), "Y_coordinates : %s - %s" % (crime_data_min[1], crime_data_max[1]), "Latitude : %s - %s" % (crime_data_min[2], crime_data_max[2]), "Longitude : %s - %s" % (crime_data_min[3], crime_data_max[3])]
    crime_data = sc.parallelize(crime_data)
    crime_data.saveAsTextFile(sys.argv[2])
    sc.stop()

