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
     .limit(10)

  val getTopCountriesByGoldMedalCountPerYear: Dataset[FullEvent] => DataFrame = { dataset =>
    val countsByYearAndCountry = dataset
      .filter(_.year != null).filter(_.region != null).filter(_.medal != null).filter(_.medal.toLowerCase.equals("gold"))
      .groupBy(col("year"), col("region")).count()
    countsByYearAndCountry.createOrReplaceTempView("countsByYearAndCountry")
    SparkSession.builder().getOrCreate().sql(
      """
        |SELECT ranks.year AS year,
        |       ranks.region AS region,
        |       ranks.count as count
        |FROM (
        |    SELECT year,
        |           region,
        |           count,
        |           row_number() over (partition by year order by count desc) as region_rank
        |    FROM countsByYearAndCountry) ranks
        |where region_rank <= 10
      """.stripMargin)
  }

  case class MetricInfo(metric: Dataset[FullEvent] => DataFrame, description: String, id: String)

  val metrics: Seq[MetricInfo] = Seq(
    MetricInfo(getMedalsByYear, "Medals by year", "medals_by_year"),
    MetricInfo(getTopCitiesByMedalCount, "Top 10 cities by number of medals", "top_10_cities"),
    MetricInfo(getTopCountriesByGoldMedalCountPerYear, "Top 10 countries by number of gold medals per year", "top_10_countries_per_year")
  )

  def process(input: DStream[String], windowLength: Int, slidingInterval: Int): Unit = {
    val configManager = ConfigManager.getInstance()
    val gcpStorageDumpPath = configManager.getGcpDumpPath
    val gcpBigQueryDataset = configManager.getBigQueryDataset
    input.window(Seconds(windowLength), Seconds(slidingInterval)).foreachRDD{rdd =>
      if (rdd.isEmpty()) {
        println(s"Current RDD (#${rdd.id}) is empty.")
      } else {
        val rddSize = rdd.count()
        println(s"The size of the current RDD (#${rdd.id}) is $rddSize.")

        val timestamp = new Timestamp(System.currentTimeMillis)
        val timestampString = new SimpleDateFormat("dd:MM:yyyy_HH:mm:ss").format(timestamp)
        val directory = f"${rdd.id}%05d_$timestampString"

        val dataset = CsvUtils.datasetFromCSV(rdd, FullEventEncoder)
        for (MetricInfo(metric, description, name) <- metrics) {
          val plainDataframe = metric(dataset)
          val stampedDataframe = plainDataframe
            .withColumn("rdd", lit(rdd.id))
            .withColumn("timestamp", lit(timestamp))

          plainDataframe.write
            .csv(s"$gcpStorageDumpPath/$directory/$name")
          stampedDataframe.write
            .format("com.google.cloud.spark.bigquery")
            .option("table", s"$gcpBigQueryDataset.$name")
            .mode("append")
            .save()

          println(s"${description}:")
          println(stampedDataframe.show(10))
        }
      }
    }
  }
}
