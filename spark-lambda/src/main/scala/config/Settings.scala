package config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config=ConfigFactory.load()

  object WebLogGen {
    private val webLogGen=config.getConfig("clickstream")

    lazy val records= webLogGen.getInt("records")
    lazy val timeMultiplier= webLogGen.getInt("time_multiplier")
    lazy val pages = webLogGen.getInt("pages")
    lazy val visitors = webLogGen.getInt("visitors")
    lazy val filePath = webLogGen.getString("file_path")
    lazy val destPath = webLogGen.getString("dest_path")
    lazy val noOfFiles = webLogGen.getInt("number_of_files")
    lazy val weblogTopic = webLogGen.getString("weblog_topic")

  }
}
