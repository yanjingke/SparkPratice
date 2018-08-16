package cn.spark.day5


import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

object LoggerLevels extends Logging{
  val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements

    def setStreamingLogLevels(): Unit ={
      if(!log4jInitialized){
        logInfo("Setting log level to [WARN] for streaming example." +
          " To override add a custom log4j.properties to the classpath.")
        Logger.getRootLogger.setLevel(Level.WARN)
      }
    }
}