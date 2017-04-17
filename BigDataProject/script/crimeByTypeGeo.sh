hadoop fs  -rm -r $1-geoData.txt
spark-submit code/statistics/crimeByTypeGeo/crimeByTypeGeo.py  NYPD_Complaint_Data_Historic.csv $1-geoData.txt  $1 $2 $3
# $1 indicate the key work, which is also the name of the file to be written
hadoop fs  -getmerge $1-geoData.txt data/$1-geoData-$2-$3.txt
