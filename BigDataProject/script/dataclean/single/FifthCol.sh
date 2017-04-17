
hadoop fs  -rm -r FifthCol.txt
spark-submit code/dataclean/single/FifthCol.py NYPD_Complaint_Data_Historic.csv FifthCol.txt                                                                                  
hfs -getmerge FifthCol.txt data/FifthCol.txt                                                                                                                             
