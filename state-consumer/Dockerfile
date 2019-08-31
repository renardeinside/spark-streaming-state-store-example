FROM debian:stretch-slim


# Spark
ARG APACHE_SPARK_VERSION=2.4.3
ARG HADOOP_VERSION=2.7
ENV SPARK_NAME=spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

ENV SPARK_DIR /opt/${SPARK_NAME}
ENV SPARK_HOME /usr/local/spark
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$SPARK_HOME/python/lib/pyspark.zip
ENV SPARK_OPTS --driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info

RUN apt-get update && \
    apt-get -y install python3-pip wget && \
    apt-get remove -y gcc-6 libgcc-6-dev perl perl-modules-5.24 && \
    apt-get autoremove -y

RUN mkdir -p /usr/share/man/man1 && \
    apt-get install -y --no-install-recommends \
      openjdk-8-jre-headless \
      ca-certificates-java \
      curl && \
    rm -rf /var/lib/apt/* && \
    curl https://archive.apache.org/dist/spark/spark-${APACHE_SPARK_VERSION}/${SPARK_NAME}.tgz | \
    tar xzf - -C /opt && \
    apt-get remove -y curl && \
    apt-get clean

# Standardize system
RUN ln -s $SPARK_DIR $SPARK_HOME && \
    ln -s /usr/bin/pip3 /usr/bin/pip && \
    ln -s /usr/bin/python3 /usr/bin/python


RUN cd /usr/local/spark/jars && wget http://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/2.4.3/spark-streaming-kafka-0-10-assembly_2.12-2.4.3.jar
RUN cd /usr/local/spark/jars && wget http://central.maven.org/maven2/org/apache/kafka/kafka-clients/2.0.0/kafka-clients-2.0.0.jar
RUN cd /usr/local/spark/jars && wget http://central.maven.org/maven2/org/apache/spark/spark-tags_2.12/2.4.3/spark-tags_2.12-2.4.3.jar
RUN cd /usr/local/spark/jars && wget http://central.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/2.4.3/spark-sql-kafka-0-10_2.12-2.4.3.jar

COPY ./target /usr/bin/app
WORKDIR /usr/bin/app

ENTRYPOINT /usr/local/spark/bin/spark-submit \
		--class "com.renarde.spark.examples.consumer.StateConsumer" \
		--master "local[2]" \
		state-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar
