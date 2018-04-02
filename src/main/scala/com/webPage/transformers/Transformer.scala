package com.webPage.transformers

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.webPage.model.{LinkedPageView, PageView}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Transformer {

  val toTimeStamp = udf((date: String) => strToTimeStamp(date).getTime)
  // Transfom Date to epoch Timestamp so we can sort it easily
  def extract(df: DataFrame) =
    df.withColumn("collector_tstamp", toTimeStamp(col("collector_tstamp")))

  val zipAndSort = udf(
    (domain_userid: String,
     events: Seq[String],
     timeStamps: Seq[Long],
     paths: Seq[String]) => {
      val eventsRes = events zip timeStamps zip paths map {
        case ((a, b), c) => PageView(a, b, domain_userid, c)
      }
      eventsRes.sortBy(_.collector_tstamp)
    })

  // We do groupBy domain_userid,
  // Output Map(domain_userid, List(PageView))
  def getEventsIdByUserId(df: DataFrame)(implicit sparkSession: SparkSession) = {
    df.groupBy(col("domain_userid"))
      .agg(collect_list(col("event_id")).as("eventsIds"),
        collect_list(col("collector_tstamp")).as("timeStamps"),
        collect_list(col("page_urlpath")).as("paths"))
      .withColumn("pageViews",
        zipAndSort(col("domain_userid"),
          col("eventsIds"),
          col("timeStamps"),
          col("paths")))
      .drop("eventsIds", "timeStamps", "paths")
  }

  def window[A](l: List[A]): Iterator[List[Option[A]]] =
    (l.map(Some(_)) ::: List(None)) sliding 2

  // List(1, 2, 3 ,4).map()
  val toLL = udf((pageViews: Seq[Row]) => {

    window(pageViews.toList).toSeq.map {
      pv =>
        val head = pv.head.get
        val nextEvent =
          if (pv.last.isDefined) pv.last.get.getAs[String]("event_id") else ""
        LinkedPageView(head.getAs[String]("event_id"),
          head.getAs[Long]("collector_tstamp"),
          head.getAs[String]("domain_userid"),
          head.getAs[String]("page_urlpath"),
          nextEvent)
    }

  })

  def seqToLinkedList(df: DataFrame) =
    df.withColumn("pageViews", toLL(col("pageViews")))

  def collect(df: DataFrame) = {
    df.drop("domain_userid")
      .select(explode(col("pageViews")).as("event"))
      .select("event.event_id",
        "event.collector_tstamp",
        "event.domain_userid",
        "event.page_urlpath",
        "event.next_event_id")
  }

  def strToTimeStamp(str: String) =
    new Timestamp(
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str).getTime)

}
