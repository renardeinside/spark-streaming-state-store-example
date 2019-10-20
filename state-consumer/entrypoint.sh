#!/bin/bash

export SPARK_DIST_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")

/usr/local/spark/bin/spark-submit \
		--class "com.renarde.spark.examples.consumer.StateConsumer" \
		--master "local[2]" \
		state-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar