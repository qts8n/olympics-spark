package streaming

import emulator.ConfigManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import streaming.Metrics.process


object JobRunner {
  def getJob(sparkConf: SparkConf): String = {
    val yarnTags = sparkConf.get("spark.yarn.tags")
    yarnTags.split(",").filter(_.startsWith("dataproc_job")).head
  }

  def createContext(windowLength: Int, slidingInterval: Int, checkpointDirectory: String): StreamingContext = {
    val configManager = ConfigManager.getInstance()

    val sparkConf = new SparkConf().setAppName(configManager.getAppName)
    val ssc = new StreamingContext(sparkConf, Seconds(slidingInterval))
    ssc.checkpoint(checkpointDirectory + '/' + getJob(sparkConf))

    val messagesStream = DStreamFactory.getSource(ssc, configManager.getProject, configManager.getSubscription)
    process(messagesStream, windowLength, slidingInterval)
    ssc
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("ERROR: invalid argument number")
      System.exit(1)
    }

    val Seq(windowLength, slidingInterval, totalRunningTime, checkpointDirectory) = args.toSeq

    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(windowLength.toInt, slidingInterval.toInt, checkpointDirectory))

    ssc.start()
    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    } else {
      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }
}
