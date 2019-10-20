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
    // generic spark params
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    // implementation params
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    // service level params
    .config("spark.hadoop.fs.s3a.endpoint", "http://storage:9000")
    .config("spark.hadoop.fs.s3a.access.key", sys.env("MINIO_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", sys.env("MINIO_SECRET_KEY"))
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
    .option("checkpointLocation", "s3a://data/checkpoints")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .foreachBatch(printBatch _).start()


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

  def printBatch(batchData: Dataset[UserStatistics], batchId: Long): Unit = {
    logger.info(s"Started working with batch id $batchId")
    batchData.sort("userId").show()
    logger.info(s"Successfully finished working with batch id $batchId")
  }
}
