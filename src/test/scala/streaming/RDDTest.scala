package streaming

import emulator.Emulator
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
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
  private var emulator: Emulator = _
  private var rdd: RDD[String] = _

  before {
    emulator = new Emulator()
    emulator.load()

    val conf = new SparkConf()
      .setAppName("unit-testing")
      .setMaster("local[*]")
    val messages: Seq[String] = emulator.getMessagesSeq
    ssc = new StreamingContext(conf, Seconds(1))
    rdd = ssc.sparkContext.parallelize(messages)
  }

  after {
    emulator.clean()
    ssc.stop()
  }

  "compute metrics" should {
    "get message stream" in {
       rdd.collect().foreach(println)
       rdd.collect().length must be > 0
    }

    "top 10 cities" in {
       Metrics.topCitiesRDD(rdd)
         .take(10)
         .foreach(medal => println(medal.city + " " + medal.amount))
    }
  }
}
