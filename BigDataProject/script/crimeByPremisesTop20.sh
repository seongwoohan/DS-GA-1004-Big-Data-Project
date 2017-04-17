hadoop fs  -rm -r crimeByPremisesTop20.txt
spark-submit code/statistics/crimeByPremisesTop20/crimeByPremisesTop20.py  NYPD_Complaint_Data_Historic.csv  crimeByPremisesTop20.txt

hadoop fs  -getmerge crimeByPremisesTop20.txt data/crimeByPremisesTop20.txt
