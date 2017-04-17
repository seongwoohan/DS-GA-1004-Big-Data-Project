hadoop fs  -rm -r crimeByStatus.txt
spark-submit code/statistics/crimeByStatus/crimeByStatus.py  NYPD_Complaint_Data_Historic.csv  crimeByStatus.txt

hadoop fs  -getmerge crimeByStatus.txt data/crimeBySeverity/crimeByStatus.txt

