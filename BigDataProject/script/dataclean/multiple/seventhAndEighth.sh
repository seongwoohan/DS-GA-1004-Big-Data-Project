hadoop fs  -rm -r seventhAndEighth.txt
spark-submit code/dataclean/multiple/seventhAndEighth/col7-8.py  NYPD_Complaint_Data_Historic.csv  seventhAndEighth.txt

hadoop fs  -getmerge seventhAndEighth.txt data/dataclean/seventhAndEighth.txt
