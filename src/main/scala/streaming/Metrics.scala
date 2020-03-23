package streaming

import java.sql.Timestamp

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.functions.{col, lit}
import csv.CsvUtils
import data.{Event, eventEncoder}
import org.apache.spark.sql.{DataFrame, Dataset}
import java.text.SimpleDateFormat


/**
  * DStream and RDD pipelines.
  */
object Metrics {
  // TODO: move into the ConfigManager or args
  val GcpStorageDumpPath = "gs://my-very-own-bucket-1/results"
  val GcpBigQueryDataset = "dataset"

  val getMedalsByYear: Dataset[Event] => DataFrame =
    _.filter(_.year != null).filter(_.medal != null)
     .groupBy(col("year")).count()

  val getTopCitiesByMedalCount: Dataset[Event] => DataFrame =
    _.filter(_.city != null).filter(_.medal != null)
     .groupBy(col("city")).count().sort(-col("count"))
     .limit(10)

  val getTopCountriesByGoldMedalCount: Dataset[Event] => DataFrame =
    _.filter(_.team != null).filter(_.medal != null).filter(_.medal.toLowerCase.equals("gold"))
     .groupBy(col("team")).count().sort(-col("count"))
     .limit(10)

  case class MetricInfo(metric: Dataset[Event] => DataFrame, description: String, id: String)

  val metrics: Seq[MetricInfo] = Seq(
    MetricInfo(getMedalsByYear, "Medals by year", "medals_by_year"),
    MetricInfo(getTopCitiesByMedalCount, "Top 10 cities by number of medals", "top_10_cities"),
    MetricInfo(getTopCountriesByGoldMedalCount, "Top 10 countries by number of gold medals", "top_10_countries")
  )

  def process(input: DStream[String], windowLength: Int, slidingInterval: Int): Unit = {
    input.window(Seconds(windowLength), Seconds(slidingInterval)).foreachRDD{rdd =>
      if (rdd.isEmpty()) {
        println(s"Current RDD (#${rdd.id}) is empty.")
      } else {
        val rddSize = rdd.count()
        println(s"The size of the current RDD (#${rdd.id}) is $rddSize.")

        val timestamp = new Timestamp(System.currentTimeMillis)
        val timestampString = new SimpleDateFormat("dd:MM:yyyy_HH:mm:ss").format(timestamp)
        val directory = f"${rdd.id}%05d_$timestampString"

        val dataset = CsvUtils.datasetFromCSV(rdd, eventEncoder)
        for (MetricInfo(metric, description, name) <- metrics) {
          val plainDataframe = metric(dataset)
          val stampedDataframe = plainDataframe
            .withColumn("rdd", lit(rdd.id))
            .withColumn("timestamp", lit(timestamp))

          plainDataframe.write
            .csv(s"$GcpStorageDumpPath/$directory/$name")
          stampedDataframe.write
            .format("com.google.cloud.spark.bigquery")
            .option("table", s"$GcpBigQueryDataset.$name")
            .mode("append")
            .save()

          println(s"${description}:")
          println(stampedDataframe.show(10))
        }
      }
    }
  }
}
