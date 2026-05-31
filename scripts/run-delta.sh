#!/bin/bash

mvn -pl site.ycsb:spark-delta-binding -am clean package

./reset-tables-delta.sh

./ycsb-no-tbl-contnetion-delta.sh

./reset-tables-delta.sh

./ycsb-low-tbl-contnetion-delta.sh

./reset-tables-delta.sh

./ycsb-medium-tbl-contnetion-delta.sh

./reset-tables-delta.sh

./ycsb-high-tbl-contnetion-delta.sh

./reset-tables-delta.sh

./ycsb-single-tbl-delta.sh