import sys
import os
from pyspark import SparkContext
from operator import add
import csv

def readfile(input):
    csvreader = csv.reader(input)
    next(csvreader)
    return csvreader

def filter_data(input, startDate, endDate):
    # as for date, use 'from' to indicate the date, if not exist, use 'to'
    dateStr = input[1] if input[1] != "" else input[3]
    # filter empty geo data
    try:
        split = dateStr.split("/")
    	dateStr = "%s%s%s" % (split[2], split[0], split[1])
    	lati = input[21]
    	longi = input[22]
    	return dateStr >= startDate and dateStr <= endDate and lati != "" and longi != ""
    except: 
	return False


def formatDate(input):
    try:
        split = input.split("/")
        return "%s%s%s" % (split[2], split[0], split[1])
    except:
	return "999"

startDate = ""
endDate = ""

if __name__ == "__main__":
    if len(sys.argv) != 6:
	print "usage: inputfile outputfile keyword datefrom dateto" # YYYY/mm/dd
        exit(-1)

    keyword = sys.argv[3]
    startDate = sys.argv[4]
    endDate = sys.argv[5]
    sc = SparkContext()
    crime_data = sc.textFile(sys.argv[1]).mapPartitions(readfile)
    crime_data_geo = crime_data.filter(lambda x: keyword  in x[7])
    crime_data_geo = crime_data_geo.filter(lambda x: filter_data(x, startDate, endDate))
    crime_data_geo = crime_data_geo.map(lambda  x: "%s\t%s" % (x[21], x[22]))
    crime_data_geo.saveAsTextFile(sys.argv[2])
    print "valid lines:", crime_data_geo.count()
    sc.stop()
