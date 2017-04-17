hadoop fs  -rm -r crimeByPremises.txt
spark-submit code/statistics/crimeByPremises/crimeByPremises.py  NYPD_Complaint_Data_Historic.csv  crimeByPremises.txt

hadoop fs  -getmerge crimeByPremises.txt data/crimeByPremises.txt
