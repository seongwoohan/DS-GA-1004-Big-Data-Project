hadoop fs  -rm -r crimeByDay.txt
spark-submit code/statistics/crimeByDay/crimeByDay.py  NYPD_Complaint_Data_Historic.csv  crimeByDay.txt

hadoop fs  -getmerge crimeByDay.txt data/crimeBySeverity/crimeByDay.txt

