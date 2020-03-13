package clickstream

import java.util.{Properties, Random}

import config.Settings
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Properties

object KafkaLogProducer extends App{
  val wlc = Settings.WebLogGen

  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray
  val Visitors = (0 to wlc.visitors).map("Visitor-"+ _)
  val Pages = (0 to wlc.pages).map("Page-"+ _)

  val rnd = new Random()
  val filePath = wlc.filePath
  val destPath = wlc.destPath
  val numOfFiles = wlc.noOfFiles
  val weblogTopic = wlc.weblogTopic

  val props = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

  val kafkaProducer = new KafkaProducer[Nothing,String](props)

  for ( fileCount <- 1 to numOfFiles) {
    val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to wlc.records) {


      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
      timestamp = System.currentTimeMillis()
      val action = iteration % (rnd.nextInt(200) + 1) match {
        case 0 => "purchase"
        case 1 => "add_to_cart"
        case _ => "page_view"
      }
      val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
      val prevPage = referrer match {
        case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
        case _ => ""
      }
      val visitor = Visitors(rnd.nextInt(Pages.length - 1))
      val page = Pages(rnd.nextInt(Pages.length - 1))
      val product = Products(rnd.nextInt(Products.length - 1))

      val line = s"$adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
      val producerRecord = new ProducerRecord[Nothing,String](weblogTopic,line)
      kafkaProducer.send(producerRecord)

      println(s"Incrementing every $incrementTimeEvery")
      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }
    }
    kafkaProducer.close()
  }

}
