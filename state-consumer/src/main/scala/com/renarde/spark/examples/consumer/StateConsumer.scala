package com.renarde.spark.examples.consumer

import com.renarde.spark.examples.common._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
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

  val pageVisitsTypedStream = rawData
    .select(from_json($"value", userEventEncoder.schema).as("data"))
    .select("data.*")
    .filter($"id".isNotNull)
    .as[PageVisit]

  val userStatisticsStream = pageVisitsTypedStream
    .groupByKey(_.id)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateUserStatistics)

  val consoleOutput = userStatisticsStream.writeStream
    .outputMode(OutputMode.Update)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .foreachBatch { (batchData: Dataset[UserStatistics], _) =>
      batchData.sort("userId").show()
    }.start()


  spark.streams.awaitAnyTermination()

  def updateUserStatistics(
                           id: Int,
                           newEvents: Iterator[PageVisit],
                           oldState: GroupState[UserStatistics]): UserStatistics = {

    var state: UserStatistics = if (oldState.exists) oldState.get else UserStatistics(id, 0, Seq.empty)

    logger.info(s"Current state: $state")

    for (event <- newEvents) {
      state = state.copy(totalEvents = state.totalEvents + 1, userEvents = state.userEvents ++ Seq(event))
      logger.info(s"Updating the state with new values: $state")
      oldState.update(state)
    }
    state
  }
}
