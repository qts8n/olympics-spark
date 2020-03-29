package streaming

import java.sql.Timestamp
import java.text.SimpleDateFormat

import csv.CsvUtils
import data._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, MustMatchers, WordSpec}
import pubsub.EmulatorPublisher
import streaming.Metrics._

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
    spark.conf.set("spark.sql.shuffle.partitions", 2)
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

          val dataset = CsvUtils.datasetFromCSV(rdd, EventEncoder)

          val metric = metrics(0).localFn
          val dataframe = metric(dataset).withColumn("timestamp", lit(timestamp))

          println(directory)
          println(dataframe.show(10))
        }
      })
      ssc.start()
      EmulatorPublisher.publishMessages()
    }
  }
}
