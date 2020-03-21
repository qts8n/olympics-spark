package streaming

import emulator.ConfigManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import streaming.Metrics.process


object JobRunner {
  def getJob(sparkConf: SparkConf): String = {
    val yarnTags = sparkConf.get("spark.yarn.tags")
    yarnTags.split(",").filter(_.startsWith("dataproc_job")).head
  }

  def createStreamingContext(windowLength: Int, slidingInterval: Int, checkpointDirectory: String): StreamingContext = {
    val configManager = ConfigManager.getInstance()

    val spark = SparkSession.builder().appName(configManager.getAppName).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(slidingInterval))
    ssc.checkpoint(checkpointDirectory + '/' + getJob(spark.sparkContext.getConf))

    val messagesStream = DStreamFactory.getSource(ssc, configManager.getProject, configManager.getSubscription)
    process(messagesStream, windowLength, slidingInterval)
    ssc
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("ERROR: invalid argument number: " + args.length + " (expected 4)")
      System.exit(1)
    }

    val Seq(windowLength, slidingInterval, totalRunningTime, checkpointDirectory) = args.toSeq

    val ssc = createStreamingContext(windowLength.toInt, slidingInterval.toInt, checkpointDirectory)

    ssc.start()
    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    } else {
      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }
}
