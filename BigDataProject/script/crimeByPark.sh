hadoop fs  -rm -r crimeByPark.txt
spark-submit code/statistics/crimeByPark/crimeByPark.py  NYPD_Complaint_Data_Historic.csv  crimeByPark.txt

hadoop fs  -getmerge crimeByPark.txt data//crimeByPark.txt
