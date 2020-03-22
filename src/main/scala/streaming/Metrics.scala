package streaming

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.functions.col
import csv.CsvUtils
import data.eventEncoder
import org.apache.spark.sql.SaveMode


/**
  * DStream and RDD pipelines.
  */
object Metrics {
  // TODO: move into the ConfigManager or args
  val gcpStorageDumpPath = "gs://my-very-own-bucket-1/results/"
  val gcpBigQueryDataset = "dataset"
  val gcpBigQueryTable   = "test"

  def process(input: DStream[String], windowLength: Int, slidingInterval: Int): Unit = {
    input.window(Seconds(windowLength), Seconds(slidingInterval)).foreachRDD(rdd => {
      val topMedalsByYear = CsvUtils.datasetFromCSV(rdd, eventEncoder).filter(_.year != null).filter(_.medal != null)
        .groupBy(col("year")).count().sort(-col("count")).limit(10)
      topMedalsByYear.write.csv(s"${gcpStorageDumpPath}${rdd.id}.csv");
      topMedalsByYear.write.format("com.google.cloud.spark.bigquery")
        .option("table",s"${gcpBigQueryDataset}.${gcpBigQueryTable}")
        .mode(SaveMode.Append)
        .save()
      println(topMedalsByYear.show(10))
    })
  }
}
