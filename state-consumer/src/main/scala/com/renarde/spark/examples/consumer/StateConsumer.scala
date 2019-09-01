package com.renarde.spark.examples.consumer

import com.renarde.spark.examples.common._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}

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

  val rawData = preparedDS.filter($"value".isNotNull).drop("key")

  val expectedSchema = new StructType()
    .add(StructField("userId", IntegerType))
    .add(StructField("pageUrl", StringType))
    .add(StructField("visitedAt", TimestampType))

  val pageVisitsTypedStream = rawData
    .select(from_json($"value", expectedSchema).as("data"))
    .select("data.*")
    .filter($"userId".isNotNull)
    .as[PageVisit]

  val userStatisticsStream: Dataset[UserStatistics] = pageVisitsTypedStream
    .groupByKey(_.userId)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updatePageVisits)


  val consoleOutput = userStatisticsStream.writeStream
    .outputMode("update")
    .format("console")
    .start()


  spark.streams.awaitAnyTermination()

  def updatePageVisits(
                        id: Int,
                        pageVisits: Iterator[PageVisit],
                        state: GroupState[UserStatistics]): UserStatistics = {
    val currentState = state.getOption.getOrElse(UserStatistics(id, 0))
    val updatedState = currentState.copy(totalVisits = currentState.totalVisits + pageVisits.length)
    updatedState
  }
}
