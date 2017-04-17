
hadoop fs  -rm -r FourteenthCol.txt
spark-submit code/dataclean/single/FourteenthCol.py NYPD_Complaint_Data_Historic.csv FourteenthCol.txt                                                                                                                      
hfs -getmerge FourteenthCol.txt data/FourteenthCol.txt  