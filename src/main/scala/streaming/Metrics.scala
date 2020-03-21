package streaming

import org.apache.spark.sql.functions.col
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import csv.CsvUtils
import data.eventEncoder


/**
 * DStream and RDD pipelines.
 */
object Metrics {
  def process(input: DStream[String], windowLength: Int, slidingInterval: Int): Unit = {
    input.window(Seconds(windowLength), Seconds(slidingInterval)).foreachRDD(rdd => {
      println(CsvUtils.datasetFromCSV(rdd, eventEncoder).filter(_.year != null).filter(_.medal != null)
        .groupBy(col("year")).count().sort(-col("count")).show(5))

    })
  }
}
