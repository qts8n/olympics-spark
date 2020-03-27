package streaming

import java.sql.Timestamp

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.functions.{col, lit}
import csv.CsvUtils
import data._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import java.text.SimpleDateFormat

import emulator.ConfigManager
import org.apache.spark.storage.StorageLevel


/**
 * DStream and RDD pipelines.
 */
object Metrics {
  val getMedalsByYear: Dataset[FullEvent] => DataFrame =
    _.filter(_.year != null).filter(_.medal != null)
      .groupBy(col("year")).count()

  val getTopCitiesByMedalCount: Dataset[FullEvent] => DataFrame =
    _.filter(_.city != null).filter(_.medal != null)
      .groupBy(col("city")).count().sort(-col("count"))

  val getTopCountriesByGoldMedalCountPerYear: Dataset[FullEvent] => DataFrame = { dataset =>
    dataset
      .filter(_.year != null).filter(_.region != null).filter(_.medal != null).filter(_.medal.toLowerCase.equals("gold"))
      .groupBy(col("year"), col("region")).count()
  }

  case class MetricInfo(metric: Dataset[FullEvent] => DataFrame, id: String)

  val metrics: Seq[MetricInfo] = Seq(
    MetricInfo(getMedalsByYear, "medals_by_year"),
    MetricInfo(getTopCitiesByMedalCount, "top_10_cities"),
    MetricInfo(getTopCountriesByGoldMedalCountPerYear, "top_10_countries_per_year")
  )

  val dfStorage: Array[DataFrame] = Array.fill(3){null}

  val globalMetrics:Seq[DataFrame => DataFrame] = Seq(
    (_:DataFrame).groupBy(col("year")).sum("count").withColumnRenamed("sum(count)", "count"),
    (_:DataFrame).groupBy(col("city")).sum("count").withColumnRenamed("sum(count)", "count").limit(10),
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
    }
  )

  val forcePersist: DataFrame => DataFrame = frame =>
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
        for ((MetricInfo(metric, name), i) <- metrics.zipWithIndex) {
          val plainDataframe = metric(dataset)
          val stampedDataframe = plainDataframe
            .withColumn("timestamp", lit(timestamp))

          plainDataframe.write
            .csv(s"$gcpStorageDumpPath/$directory/$name")
          stampedDataframe.write
            .format("com.google.cloud.spark.bigquery")
            .option("table", s"$gcpBigQueryDataset.$name")
            .mode("append")
            .save()

          if (dfStorage(i) == null) {
            dfStorage(i) = forcePersist(plainDataframe)
          } else {
            val original = dfStorage(i)
            val persisted = forcePersist(original.union(plainDataframe))
            original.unpersist()

            val df = globalMetrics(i)(persisted)
            val stampedGlobalDataframe = df
              .withColumn("timestamp", lit(timestamp))
            stampedGlobalDataframe.write
              .format("com.google.cloud.spark.bigquery")
              .option("table", s"$gcpBigQueryDataset.${name}_global")
              .mode("overwrite")
              .save()

            dfStorage(i) = persisted
          }
        }
      }
    }
  }
}
