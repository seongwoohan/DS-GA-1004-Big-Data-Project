hadoop fs  -rm -r crimeBySeverity.txt
spark-submit code/dataclean/single/EighteenthCol.py NYPD_Complaint_Data_Historic.csv EighteenthCol.txt                                                                                                                      
hfs -getmerge EighteenthCol.txt data/EighteenthCol.txt  
