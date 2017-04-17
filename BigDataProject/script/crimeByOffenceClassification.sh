hadoop fs  -rm -r crimeByOffenceClassification.txt
spark-submit code/statistics/crimeByOffenceClassification/crimeByOffenceClassification.py  NYPD_Complaint_Data_Historic.csv  crimeByOffenceClassification.txt

hadoop fs  -getmerge crimeByOffenceClassification.txt data/crimeBySeverity/crimeByOffenceClassification.txt
