#!/bin/bash

mvn -pl site.ycsb:spark-iceberg-binding -am clean package

./reset-tables-iceberg.sh

./ycsb-no-tbl-contnetion-iceberg.sh

./reset-tables-iceberg.sh

./ycsb-low-tbl-contnetion-iceberg.sh

./reset-tables-iceberg.sh

./ycsb-medium-tbl-contnetion-iceberg.sh

./reset-tables-iceberg.sh

./ycsb-high-tbl-contnetion-iceberg.sh

./reset-tables-iceberg.sh

./ycsb-single-tbl-iceberg.sh