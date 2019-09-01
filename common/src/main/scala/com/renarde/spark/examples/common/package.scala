package com.renarde.spark.examples

import java.sql.Timestamp

import org.apache.spark.sql.{Encoder, Encoders}

package object common {

  val random = new scala.util.Random

  case class PageVisit(userId: Int, pageUrl: String, visitedAt: Timestamp = new Timestamp(System.currentTimeMillis()))

  case class UserStatistics(pageVisits: Seq[PageVisit]) {
    val totalVisits: Int = pageVisits.length
  }

  implicit val pageVisitEncoder: Encoder[PageVisit] = Encoders.product[PageVisit]
  implicit val userStatisticsEncoder: Encoder[UserStatistics] = Encoders.product[UserStatistics]

  def generateVisit: PageVisit = {
    PageVisit(
      userId = generateBetween(),
      pageUrl = s"https://www.my-service.org/${generateBetween(100, 200)}"
    )
  }

  def generateVisits(n: Int = 10): Seq[PageVisit] = {
    (0 to n).map(_ => generateVisit)
  }

  def generateBetween(start: Int = 0, end: Int = 100): Int = {
    start + random.nextInt((end - start) + 1)
  }

}

