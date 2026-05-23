# Spark-Delta Lake Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a Spark + Lakehouse Server cluster.

## Quickstart


### 1. Create the base table
e.g.
```
CREATE NAMESPACE ycsb;
USE spark_catalog.ycsb;

CREATE TABLE usertable (YCSB_KEY VARCHAR(255),FIELD0 String, FIELD1 String,FIELD2 String, FIELD3 String,FIELD4 String, FIELD5 String,FIELD6 String, FIELD7 String,FIELD8 String, FIELD9 String) using delta;
```

### 2. Compile YCSB core and the Spark binding

```
mvn -pl site.ycsb:sparkDelta-binding -am clean package
```


### 3. Test the implementation
You can use the internal command line to test the implemented functions

```
bin/ycsb.sh shell sparkDelta
```

### 5. Load data set
You have to create the table first
```
bin/ycsb.sh load sparkDelta -P workloads/workloada -p spark.lakehouse=delta
```


### 6. Run the benchmark
This starts the benchmark defined in the referenced workload file
```
bin/ycsb.sh run sparkDelta -P workloads/workloada -p spark.lakehouse=delta
```