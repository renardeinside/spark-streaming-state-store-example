package com.renarde.spark.examples.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._


object StateConsumer extends App with LazyLogging {
    val appName: String = "structured-consumer-example"

    val spark: SparkSession = SparkSession.builder()
        .appName(appName)
        .config("spark.driver.memory", "5g")
        .master("local[2]")
        .getOrCreate()

    import spark.implicits._

    logger.info("Initializing Structured consumer")

    spark.sparkContext.setLogLevel("WARN")

    val inputStream = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "visits-topic")
        .option("startingOffsets", "earliest")
        .load()


    val preparedDS = inputStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val rawData = preparedDS.filter($"value".isNotNull)

    val consoleOutput = rawData.writeStream
        .outputMode("append")
        .format("console")
        .start()


    spark.streams.awaitAnyTermination()

}
