package com.renarde.spark.examples.consumer

import java.nio.file.Files

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

object StateConsumer extends App with Logging {

  val appName: String = "spark-state-consumer-example"
  val checkpointLocation: String = Files.createTempDirectory(appName).toString
  var query: StreamingQuery = _

  log.info(s"Running the process in temporary directory $checkpointLocation")

  val spark = SparkSession.builder()
    // spark props
    .master("local[2]")
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  import spark.implicits._

  implicit val sqlCtx: SQLContext = spark.sqlContext

  val visitsStream = MemoryStream[PageVisit]

  val pageVisitsTypedStream: Dataset[PageVisit] = visitsStream.toDS()

  val initialBatch = Seq(
    generateEvent(1),
    generateEvent(1),
    generateEvent(1),
    generateEvent(1),
    generateEvent(2),
  )

  visitsStream.addData(initialBatch)

  val userStatisticsStream = pageVisitsTypedStream
    .groupByKey(_.id)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateUserStatistics)

  query = userStatisticsStream.writeStream
    .outputMode(OutputMode.Update())
    .option("checkpointLocation", checkpointLocation)
    .foreachBatch(printBatch _)
    .start()


  processDataWithLock(query)

  StateDatasetProvider(spark, checkpointLocation, query, query.lastProgress.batchId).dataset.sort($"userId").show()

  val additionalBatch = Seq(
    generateEvent(1),
    generateEvent(3),
    generateEvent(3),
  )

  visitsStream.addData(additionalBatch)

  processDataWithLock(query)
  StateDatasetProvider(spark, checkpointLocation, query, query.lastProgress.batchId).dataset.sort($"userId").show()


  def updateUserStatistics(
                            id: Int,
                            newEvents: Iterator[PageVisit],
                            oldState: GroupState[UserStatistics]): UserStatistics = {

    var state: UserStatistics = if (oldState.exists) oldState.get else UserStatistics(id, 0)

    for (_ <- newEvents) {
      state = state.copy(totalEvents = state.totalEvents + 1)
      oldState.update(state)
    }
    state
  }

  def printBatch(batchData: Dataset[UserStatistics], batchId: Long): Unit = {
    log.info(s"Started working with batch id $batchId")
    log.info(s"Successfully finished working with batch id $batchId, dataset size: ${batchData.count()}")
  }

  def processDataWithLock(query: StreamingQuery): Unit = {
    query.processAllAvailable()
    while (query.status.message != "Waiting for data to arrive") {
      log.info(s"Waiting for the query to finish processing, current status is ${query.status.message}")
      Thread.sleep(1)
    }
    log.info("Locking the thread for another 5 secs for query to get finished")
    Thread.sleep(5000)
  }

}

