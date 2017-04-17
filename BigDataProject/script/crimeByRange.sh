hadoop fs  -rm -r crimeByRange.txt
spark-submit code/statistics/crimeByRange/crimeByRange.py  NYPD_Complaint_Data_Historic.csv  crimeByRange.txt

hadoop fs  -getmerge crimeByRange.txt data/crimeByRange.txt
