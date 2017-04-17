hadoop fs  -rm -r ninthAndTenth.txt
spark-submit code/dataclean/multiple/ninthAndTenth/col9-10.py  NYPD_Complaint_Data_Historic.csv  ninthAndTenth.txt

hadoop fs  -getmerge ninthAndTenth.txt data/dataclean/ninthAndTenth.txt
