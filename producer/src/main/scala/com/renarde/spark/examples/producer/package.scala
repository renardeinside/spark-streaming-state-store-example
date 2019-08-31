package com.renarde.spark.examples

import java.sql.Timestamp

import scala.util.Random

package object producer {

    case class PageVisit(userId: Int, pageUrl: String, visited_at: Timestamp = new Timestamp(System.currentTimeMillis()))

    def generateVisit: PageVisit = {
        PageVisit(
            userId = new Random().nextInt(),
            pageUrl = s"https://www.my-service.org/${new Random().nextInt()}"
        )
    }

    def generateVisits(n: Int = 10): Seq[PageVisit] = {
        (0 to n).map(_ => generateVisit)
    }
}

