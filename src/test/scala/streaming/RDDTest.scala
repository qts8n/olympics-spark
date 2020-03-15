package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, MustMatchers, WordSpec}
import pubsub.EmulatorPublisher


/**
 * Get a single RDD from local Pub/Sub emulator test.
 *
 * @see https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java
 * @see https://github.com/GoogleCloudPlatform/dataproc-pubsub-spark-streaming
 */
class RDDTest extends WordSpec with MustMatchers with BeforeAndAfter {
  private var ssc: StreamingContext = _
  private var stream: DStream[String] = _

  before {
    EmulatorPublisher.createTopic();
    val conf = new SparkConf().setAppName("unit-testing").setMaster("local[*]")
    ssc = new StreamingContext(conf, Seconds(1))
    stream = DStreamFactory.getSource(ssc)
  }

  after {
    ssc.awaitTerminationOrTimeout(5000)
    ssc.stop()
    EmulatorPublisher.deleteTopic();
  }

  "compute metrics" should {
    "get message stream" in {
      stream.foreachRDD{ rdd => println(rdd.collect().length)}
      ssc.start()
      EmulatorPublisher.publishMessages();
    }
  }
}
