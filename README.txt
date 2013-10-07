Prepare:
1. Copy the data files from /data/wgsn on local to /data/wgsn directory in hdfs
hadoop fs -mkdir /data/wgsn
hadoop fs -copyFromLocal /data/wgsn/* /data/wgsn/

2. Copy LinkedIn's datafu-0.0.4.jar to /data/wgsn on local machine
https://github.com/downloads/linkedin/datafu/datafu-0.0.4.tar.gz

Execute:
pig -f scripts/product_pairs.pig 

To Do:
1. Store input files using Avro with snappy compression
2. Modify joins by copying smaller data files to Hadoop's Distributed Cache
