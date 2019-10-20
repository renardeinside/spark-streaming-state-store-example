package com.renarde.spark.examples

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.{Encoder, Encoders}

package object common {

  val random = new scala.util.Random

  case class PageVisit(id: Int, url: String, timestamp: Timestamp = Timestamp.from(Instant.now()))

  case class UserStatistics(userId: Int, totalEvents: Int, userEvents: Seq[PageVisit])

  implicit val userEventEncoder: Encoder[PageVisit] = Encoders.product[PageVisit]
  implicit val userStatisticsEncoder: Encoder[UserStatistics] = Encoders.product[UserStatistics]

  def generateEvent: PageVisit = {
    PageVisit(
      id = generateBetween(0, 5),
      url = s"https://www.my-service.org/${generateBetween(100, 200)}"
    )
  }

  def generateBetween(start: Int = 0, end: Int = 100): Int = {
    start + random.nextInt((end - start) + 1)
  }

}

