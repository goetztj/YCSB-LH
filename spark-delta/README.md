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

### 2.1 Setup Delta Lake Usage

Depending on the Lakehouse you want to use, you have to switch the pom.xml file in this directory.

- For Delta Lake: pom.xml
- For Iceberg: pom-iceberg.xml

Rename the specific file to pom.xml.

When executing the benchmark, you can specifiy with Lakehouse format to use. This is required for Delta Lake as the default value is iceberg. Simply, sepecify the fromat by adding it as an argument for all bin/ycsb.sh commands:

- For Delta Lake: -p spark.lakehouse=delta
- For Iceberg: -p spark.lakehouse=iceberg

### 2.2 Compile YCSB core and the Spark binding

```
mvn -pl site.ycsb:spark-delta-binding -am clean package
```


### 3. Test the implementation
You can use the internal command line to test the implemented functions

```
bin/ycsb.sh shell spark-delta
```

### 5. Load data set
You have to create the table first
```
bin/ycsb.sh load spark-delta -P workloads/workloada -p spark.lakehouse=delta
```


### 6. Run the benchmark
This starts the benchmark defined in the referenced workload file
```
bin/ycsb.sh run spark-delta -P workloads/workloada -p spark.lakehouse=delta
```