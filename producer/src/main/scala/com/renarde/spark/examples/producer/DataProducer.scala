package com.renarde.spark.examples.producer

import java.util.Properties

import akka.actor.ActorSystem
import com.renarde.spark.examples.common._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.native.Serialization.write

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

object DataProducer extends App with LazyLogging {

  def provideProducer(): KafkaProducer[String, String] = {
    val props = new Properties()

    props.put("bootstrap.servers", "kafka:9092")
    props.put("client.id", "producer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("metadata.max.age.ms", "10000")

    new KafkaProducer[String, String](props)
  }

  def startGeneration(): Unit = {
    val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler

    implicit val executor: ExecutionContextExecutor = actorSystem.dispatcher
    implicit val formats: DefaultFormats = DefaultFormats

    val producer = provideProducer()
    producer.flush()

    logger.info("Kafka producer is ready!")
    var eventsCounter: Long = 0

    val task = new Runnable {
      def run(): Unit = {

        val visitEvent = generateEvent

        eventsCounter += 1

        val visitEventMessage = new ProducerRecord[String, String]("visits-topic", write(visitEvent))
        producer.send(visitEventMessage)

        logger.info(s"New visits data came, total: $eventsCounter visits")
      }
    }

    scheduler.schedule(
      initialDelay = 3.seconds,
      interval = 1.seconds,
      runnable = task
    )
  }

  startGeneration()
}
