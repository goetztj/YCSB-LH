#!/bin/bash

echo "Preparing YCSB"

echo "Deleting tables"


docker exec -it spark-delta3 spark-sql -e "DROP namespace ycsb cascade;"
docker exec -it spark-delta3 spark-sql -e "CREATE namespace ycsb;"

echo "Creating tables"

for i in $(seq 0 31); do
  docker exec -it spark-delta3 spark-sql -e "
    CREATE TABLE IF NOT EXISTS spark_catalog.ycsb.usertable${i} (YCSB_KEY VARCHAR(255),FIELD0 String, FIELD1 String,FIELD2 String, FIELD3 String,FIELD4 String, FIELD5 String,FIELD6 String, FIELD7 String,FIELD8 String, FIELD9 String) using delta;
  "
  bin/ycsb.sh load sparkDelta -P workloads/workloada -p threadcount=1 -p spark.resultFile=./result_lb${i} -p table=usertable${i} -p spark.lakehouse=delta > ./load_result_dl_a${i}.txt
done