package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, MustMatchers, WordSpec}
import pubsub.EmulatorPublisher

import csv.CsvUtils
import data.eventEncoder

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
      stream.foreachRDD{ rdd => println(CsvUtils.datasetFromCSV(rdd, eventEncoder).show(5))}
      ssc.start()
      EmulatorPublisher.publishMessages()
    }
  }
}
