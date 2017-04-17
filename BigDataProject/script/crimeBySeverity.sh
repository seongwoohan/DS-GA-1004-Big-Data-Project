hadoop fs  -rm -r crimeBySeverity.txt
spark-submit code/statistics/crimeBySeverity/crimeBySeverity.py  NYPD_Complaint_Data_Historic.csv  crimeBySeverity.txt

hadoop fs  -getmerge crimeBySeverity.txt data/crimeBySeverity/crimeBySeverity.txt

