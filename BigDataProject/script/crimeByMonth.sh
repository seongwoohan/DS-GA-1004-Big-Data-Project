hadoop fs  -rm -r crimeByMonth.txt
spark-submit code/statistics/crimeByMonth/crimeByMonth.py  NYPD_Complaint_Data_Historic.csv  crimeByMonth.txt

hadoop fs  -getmerge crimeByMonth.txt data/crimeBySeverity/crimeByMonth.txt
