package streaming

import java.sql.Timestamp

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.functions.{col, lit}
import csv.CsvUtils
import data._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.text.SimpleDateFormat

import emulator.ConfigManager
import org.apache.spark.storage.StorageLevel


/**
 * DStream and RDD pipelines.
 */
object Metrics {
  case class Metric(localFn: Dataset[FullEvent] => DataFrame, globalFn: DataFrame => DataFrame,
                    id: String, var storage: DataFrame)

  val metrics: Array[Metric] = Array(
    Metric(
      (_: Dataset[FullEvent]).filter(_.year != null).filter(_.medal != null)
                             .groupBy(col("year")).count(),
      (_: DataFrame).groupBy(col("year")).sum("count")
                    .withColumnRenamed("sum(count)", "count"),
      "medals_by_year",
      null
    ),
    Metric(
      (_: Dataset[FullEvent]).filter(_.city != null).filter(_.medal != null)
                             .groupBy(col("city")).count().sort(-col("count")),
      (_: DataFrame).groupBy(col("city")).sum("count")
                    .withColumnRenamed("sum(count)", "count").limit(10),
      "top_10_cities",
      null
    ),
    Metric(
      (_: Dataset[FullEvent]).filter(_.year != null).filter(_.region != null).filter(_.medal != null)
                             .filter(_.medal.toLowerCase.equals("gold"))
                             .groupBy(col("year"), col("region")).count(),
      (frame: DataFrame) => {
        frame.createOrReplaceTempView("frame")
        SparkSession.builder().getOrCreate().sql(
          """
            |SELECT *
            |FROM (
            |    SELECT year,
            |           region,
            |           count,
            |           ROW_NUMBER() OVER (PARTITION BY year ORDER BY count DESC) as region_rank
            |    FROM frame) ranks
            |WHERE region_rank <= 10
          """.stripMargin)
      },
      "top_10_countries_per_year",
      null
    )
  )

  def forcePersist(frame: DataFrame): DataFrame =
    SparkSession.builder().getOrCreate()
      .createDataFrame(frame.rdd, frame.schema)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

  def process(input: DStream[String], windowLength: Int, slidingInterval: Int): Unit = {
    val configManager = ConfigManager.getInstance()
    val gcpStorageDumpPath = configManager.getGcpDumpPath
    val gcpBigQueryDataset = configManager.getBigQueryDataset

    input.window(Seconds(windowLength), Seconds(slidingInterval)).foreachRDD{rdd =>
      if (!rdd.isEmpty()) {
        val timestamp = new Timestamp(System.currentTimeMillis)
        val timestampString = new SimpleDateFormat("dd:MM:yyyy_HH:mm:ss").format(timestamp)
        val directory = timestampString

        val dataset = CsvUtils.datasetFromCSV(rdd, FullEventEncoder)
        for (i <- metrics.indices) {
          val metric = metrics(i)

          val plainDataframe = metric.localFn(dataset)
          val stampedDataframe = plainDataframe.withColumn("timestamp", lit(timestamp))

          plainDataframe.write
            .csv(s"$gcpStorageDumpPath/$directory/${metric.id}")
          stampedDataframe.write
            .format("com.google.cloud.spark.bigquery")
            .option("table", s"$gcpBigQueryDataset.${metric.id}")
            .mode("append")
            .save()

          if (metric.storage == null) {
            metric.storage = forcePersist(plainDataframe)
          } else {
            val previousStorage = metric.storage
            val updatedStorage = forcePersist(previousStorage.union(plainDataframe))
            previousStorage.unpersist()

            val stampedGlobalDataframe = metric.globalFn(updatedStorage)
              .withColumn("timestamp", lit(timestamp))
            stampedGlobalDataframe.write
              .format("com.google.cloud.spark.bigquery")
              .option("table", s"$gcpBigQueryDataset.${metric.id}_global")
              .mode("overwrite")
              .save()

            metric.storage = updatedStorage
          }

          metrics(i) = metric
        }
      }
    }
  }
}
