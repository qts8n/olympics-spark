package streaming

import java.nio.charset.StandardCharsets

import emulator.Emulator
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import spark.EmulatorReceiver


object DStreamFactory {
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

  def getSource(ssc: StreamingContext): DStream[String] = {
    ssc.receiverStream(new EmulatorReceiver(StorageLevel.MEMORY_ONLY))
  }
}
