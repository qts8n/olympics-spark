package streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream


/**
 * DStream and RDD pipelines.
 */
object Metrics {
  case class Medals(city: String, amount: Int)

  final val titles: Map[Int, String] = Map(
    0 -> "ID",
    1 -> "Name",
    2 -> "Sex",
    3 -> "Age",
    4 -> "Height",
    5 -> "Weight",
    6 -> "Team",
    7 -> "NOC",
    8 -> "Games",
    9 -> "Year",
    10 -> "Season",
    11 -> "City",
    12 -> "Sport",
    13 -> "Event",
    14 -> "Medal")

  def topCitiesRDD(input: RDD[String]): RDD[Medals] = {
    input.map(_.replaceAll("\"", "").toLowerCase)
      .map(_.split(",").zipWithIndex.map{ case (e, i) => (i, e) })
      .filter(row => !row(11)._2.isEmpty && !row(14)._2.isEmpty)
      .groupBy(row => row(11)._2)
      .map(tuple => Medals(tuple._1, tuple._2.size))
      .sortBy(city => city.amount, ascending = false)
  }

  def topCities(input: DStream[String], windowLength: Int, slidingInterval: Int, handler: Array[Medals] => Unit): Unit = {
    val sortedCities = input
      .window(Seconds(windowLength), Seconds(slidingInterval)) //create a window
      .transform(rdd => topCitiesRDD(rdd))
    sortedCities.foreachRDD(rdd => handler(rdd.take(10)))
  }

  def process(input: DStream[String], windowLength: Int, slidingInterval: Int): Unit = {
    // TODO: aggregate all metrics here to call in a job or make separate jobs
  }
}
