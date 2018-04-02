package com.webPage.tracking

import com.webPage.transformers.Transformer._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by sanush on 30/07/2017.
  */
object Main {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Tracking Page")
    .getOrCreate

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Please set argument for the input Path & outputPath")
      System.exit(1)
    }

    val df = spark.read.option("header", true).csv(args(0))

    val res = process(df)(spark)

    res
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .csv(args(1))

    spark.stop()
  }

  def process(df: DataFrame)(sparkSession: SparkSession) = {
    val dfMap = getEventsIdByUserId(extract(df))(spark)
    val right = collect(seqToLinkedList(dfMap))
      .select("event_id", "next_event_id")
      .withColumnRenamed("event_id", "event_id_right")

    val res = right
      .join(df, right.col("event_id_right") === df.col("event_id"))
      .drop("event_id_right")
    res
      .withColumn("next_event_id_r", res.col("next_event_id"))
      .drop("next_event_id")
      .withColumnRenamed("next_event_id_r", "next_event_id")

  }

}
