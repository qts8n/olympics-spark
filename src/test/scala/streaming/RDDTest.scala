package streaming

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, MustMatchers, WordSpec}
import pubsub.EmulatorPublisher
import csv.CsvUtils
import data.eventEncoder
import org.apache.spark.sql.functions.{col, lit}
import streaming.Metrics.{MetricInfo, metrics}

/**
 * Get a single RDD from local Pub/Sub emulator test.
 *
 * @see https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java
 * @see https://github.com/GoogleCloudPlatform/dataproc-pubsub-spark-streaming
 */
class RDDTest extends WordSpec with MustMatchers with BeforeAndAfter {
  private var spark: SparkSession = _
  private var ssc: StreamingContext = _
  private var stream: DStream[String] = _

  before {
    EmulatorPublisher.createTopic()
    EmulatorPublisher.createSubscription()
    spark = SparkSession.builder().appName("unit-testing").master("local[*]").getOrCreate()
    ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    stream = DStreamFactory.getSource(ssc)
  }

  after {
    ssc.awaitTerminationOrTimeout(5000)
    ssc.stop()
    EmulatorPublisher.deleteSubscription()
    EmulatorPublisher.deleteTopic()
  }

  "compute metrics" should {
    "get message stream" in {
      stream.foreachRDD(rdd => {
        if (rdd.isEmpty()) {
          println(s"Current RDD (#${rdd.id}) is empty.")
        } else {
          val rddSize = rdd.count()
          println(s"The size of the current RDD (#${rdd.id}) is $rddSize.")

          val timestamp = new Timestamp(System.currentTimeMillis)
          val timestampString = new SimpleDateFormat("dd:MM:yyyy_HH:mm:ss").format(timestamp)
          val directory = f"${rdd.id}%05d_$timestampString"

          val dataset1 = CsvUtils.datasetFromCSV(rdd, eventEncoder)
          println(s"1: ${dataset1.rdd.getNumPartitions}")
          val dataset2 = dataset1.filter(_.year != null)
          println(s"2: ${dataset2.rdd.getNumPartitions}")
          val dataset3 = dataset2.filter(_.medal != null)
          println(s"3: ${dataset3.rdd.getNumPartitions}")
          val dataset4 = dataset3.groupBy(col("year")).count()
          println(s"4: ${dataset4.rdd.getNumPartitions}")

          for (MetricInfo(metric, description, id) <- metrics) {
            val dataframe = metric(dataset1).withColumn("timestamp", lit(timestamp))
            println(s"${description}:")

            println(directory)
            println(dataframe.show(10))
          }
        }
      })
      ssc.start()
      EmulatorPublisher.publishMessages()
    }
  }
}
