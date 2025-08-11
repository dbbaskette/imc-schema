source config.env && HADOOP_USER_NAME=hdfs hdfs dfs -rm -r  hdfs://$HDFS_NAMENODE_HOST:$HDFS_NAMENODE_PORT/processed_files
