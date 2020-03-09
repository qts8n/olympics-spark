package streaming

import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}


object DataStreaming {
  def getSource(ssc: StreamingContext, project: String, subscription: String): DStream[String] = {
    PubsubUtils
      .createStream(
        ssc,
        project,
        None,
        subscription,
        SparkGCPCredentials.builder.build(),
        StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))
  }
}
