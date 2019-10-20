FROM debian:stretch-slim


# System

RUN apt-get update && \
    apt-get -y install python3-pip wget && \
    apt-get remove -y gcc-6 libgcc-6-dev perl perl-modules-5.24 && \
    apt-get autoremove -y

RUN mkdir -p /usr/share/man/man1 && \
    apt-get install -y --no-install-recommends openjdk-8-jre-headless ca-certificates-java curl wget procps && \
    apt-get clean

# Spark
ARG APACHE_SPARK_VERSION=2.4.4
ARG HADOOP_VERSION=2.8.5

ENV HADOOP_NAME hadoop-${HADOOP_VERSION}
ENV SPARK_NAME spark-${APACHE_SPARK_VERSION}-bin-without-hadoop

ENV SPARK_DIR /opt/${SPARK_NAME}
ENV SPARK_HOME /usr/local/spark

ENV HADOOP_DIR /opt/${HADOOP_NAME}
ENV HADOOP_HOME /usr/local/hadoop

ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$SPARK_HOME/python/lib/pyspark.zip


RUN curl http://mirrors.ae-online.de/apache/spark/spark-${APACHE_SPARK_VERSION}/${SPARK_NAME}.tgz | \
    tar xzf - -C /opt

RUN curl http://mirrors.ae-online.de/apache/hadoop/common/${HADOOP_NAME}/${HADOOP_NAME}.tar.gz | \
    tar xzf - -C /opt

RUN apt-get update && apt-get clean

# Standardize system
RUN ln -sf $SPARK_DIR $SPARK_HOME && \
    ln -sf $HADOOP_DIR $HADOOP_HOME && \
    ln -sf /usr/bin/pip3 /usr/bin/pip && \
    ln -sf /usr/bin/python3 /usr/bin/python


# add custom jars
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/ru/yandex/clickhouse/clickhouse-jdbc/0.2/clickhouse-jdbc-0.2.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.4/spark-sql-kafka-0-10_2.11-2.4.4.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.0.0/kafka-clients-2.0.0.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/org/spark-project/spark/unused/1.0.0/unused-1.0.0.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/com/google/guava/guava/19.0/guava-19.0.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.8.5/hadoop-aws-2.8.5.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.3/httpclient-4.5.3.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.11.234/aws-java-sdk-core-1.11.234.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-kms/1.11.234/aws-java-sdk-kms-1.11.234.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.234/aws-java-sdk-1.11.234.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.11.234/aws-java-sdk-s3-1.11.234.jar
RUN cd ${SPARK_HOME}/jars && wget https://repo1.maven.org/maven2/joda-time/joda-time/2.9.9/joda-time-2.9.9.jar


ENV APPDIR /usr/bin/app
RUN mkdir -p ${APPDIR}
WORKDIR ${APPDIR}

ENV PYTHONPATH $PYTHONPATH:${APPDIR}
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre

WORKDIR ${APPDIR}

ADD ./entrypoint.sh ${APPDIR}/entrypoint.sh
RUN chmod +x ${APPDIR}/entrypoint.sh
COPY ./target ${APPDIR}

ENTRYPOINT ["./entrypoint.sh"]
