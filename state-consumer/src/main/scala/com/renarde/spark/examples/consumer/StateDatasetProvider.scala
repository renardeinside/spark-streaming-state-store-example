package com.renarde.spark.examples.consumer

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

case class StateDatasetProvider(spark: SparkSession, checkpointLocation: String, query: StreamingQuery, storeVersion: Long) {

  private val context = spark.sqlContext

  import context.implicits._

  private val keySchema = new StructType().add(StructField("id", IntegerType))
  private val valueSchema = new StructType().add(StructField("groupState", userStatisticsEncoder.schema))

  val storeConf = StateStoreConf(spark.sessionState.conf)
  private val hadoopConf = spark.sessionState.newHadoopConf()


  val stateStoreId = StateStoreId(checkpointLocation + "/state", operatorId = 0, partitionId = 0)
  val storeProviderId = StateStoreProviderId(stateStoreId, query.runId)

  val store: StateStore = StateStore.get(storeProviderId, keySchema, valueSchema, None, storeVersion, storeConf, hadoopConf)

  val dataset: Dataset[UserStatistics] = store.iterator().map { rowPair =>
    val statisticsEncoder = ExpressionEncoder[UserGroupState].resolveAndBind()
    statisticsEncoder.fromRow(rowPair.value).groupState
  }.toSeq.toDS()

}
