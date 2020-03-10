package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, MustMatchers, WordSpec}


/**
 * Get a single RDD from local Pub/Sub emulator test.
 *
 * Uses `Emulator` class for connection establishment and data extraction.
 *
 * @see https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java
 * @see https://github.com/GoogleCloudPlatform/dataproc-pubsub-spark-streaming
 */
class RDDTest extends WordSpec with MustMatchers with BeforeAndAfter {
  private var ssc: StreamingContext = _
  private var stream: DStream[String] = _

  before {

    val conf = new SparkConf().setAppName("unit-testing").setMaster("local[*]")
    ssc = new StreamingContext(conf, Seconds(1))
    stream = DStreamFactory.getSource(ssc)
  }

  after {
    ssc.awaitTerminationOrTimeout(5000)
    ssc.stop()
  }

  "compute metrics" should {
    "get message stream" in {
      stream.foreachRDD{ rdd => println(rdd.collect().length)}
      ssc.start()
    }
  }
}
