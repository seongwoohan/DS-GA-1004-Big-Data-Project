
hadoop fs  -rm -r crimeBySeverity.tx
spark-submit code/dataclean/single/EleventhCol.py NYPD_Complaint_Data_Historic.csv EleventhCol.txt                                                                                                                      
hfs -getmerge EleventhCol.txt data/EleventhCol.txt  