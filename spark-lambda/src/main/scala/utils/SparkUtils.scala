package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def isIDE(): Boolean = {
    println(ManagementFactory.getRuntimeMXBean.getInputArguments.toString)
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkSession(appName:String):SparkSession = {

    val spark: SparkSession = isIDE() match {
      case true => SparkSession.builder().appName(appName).config("spark.master", "local[*]").getOrCreate()
      case _ =>  SparkSession.builder().appName(appName).getOrCreate()
    }

    if(isIDE) {
      System.setProperty("hadoop.home.dir","C:\\Softwares\\hadoop-2.7.1\\bin\\winutils")
      spark.sparkContext.setCheckpointDir("file:///c:/temp")
    }else {
      spark.sparkContext.setCheckpointDir("hdfs://lambda-pluralsight:9000/spark/checkpoint")
    }

    spark
  }

}
