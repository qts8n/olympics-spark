package streaming

import emulator.ConfigManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest._


/**
 * Get published messages from Google Cloud Pub/Sub queue test.
 *
 * Google cloud JSON-credentials file required.
 * @see https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable
 */
class StreamingTest extends WordSpec with MustMatchers with BeforeAndAfter {
  private var ssc: StreamingContext = _
  private var stream: DStream[String] = _

  before {
    val configManager = ConfigManager.getInstance()
    val conf = new SparkConf()
      .setAppName("unit-testing")
      .setMaster("local[*]")
    ssc = new StreamingContext(conf, Seconds(1))
    stream = DataStreaming.getSource(ssc, configManager.getProject, configManager.getSubscription)
  }

  after {
    if (ssc != null) {
      ssc.awaitTerminationOrTimeout(5000)
      ssc.stop()
    }
  }

  "compute metrics" should {
    "get message stream" in {
      stream.foreachRDD { rdd =>
        println(rdd.collect().length)
      }
      ssc.start()
    }
  }
}
