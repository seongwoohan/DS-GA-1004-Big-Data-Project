
hadoop fs  -rm -r FourteenthCol1.txt
spark-submit code/dataclean/single/FourteenthCol1.py NYPD_Complaint_Data_Historic.csv FourteenthCol1.txt                                                                                                                      
hfs -getmerge FourteenthCol1.txt data/FourteenthCol1.txt  