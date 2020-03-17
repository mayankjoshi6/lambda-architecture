package streaming

import config.Settings
import domain.{Activity, ActivityByProduct}
import functions._
import org.apache.spark.sql.{SaveMode, functions}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import utils.SparkUtils.{getSparkSession, isIDE}

object KafkaStreamingJob {
  def main(args:Array[String]) = {
    val spark=getSparkSession("lambda-streaming")
    val ssc=new StreamingContext(spark.sparkContext,Seconds(4))
    spark.sparkContext.setLogLevel("ERROR")
    val wlc = Settings.WebLogGen
    val webLogTopic = wlc.weblogTopic


/*    val sourceFilePath = if (isIDE()) {
      "file:///C:/Users/Mayank/Documents/study/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
    } else {
      "file:///vagrant/input"
    }
    println(s"Looking for files in $sourceFilePath")



    //val dStream=ssc.textFileStream(sourceFilePath)*/
    val topics = Array(webLogTopic)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "weblogConsumers",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /*KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )*/

    val dStream = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String,String](topics,kafkaParams))


    //KafkaUtils.createRDD(ssc.sparkContext,kafkaParams,Array(),PreferConsistent)
    val activityStream = dStream.transform(input=>{
        rddToRDDActivity(input)
    })
    import spark.implicits._

     activityStream.foreachRDD { rdd =>
       val activityDF=rdd
      .toDF()
        .selectExpr("timestamp_hour","referrer","action","prevPage","page",
        "visitor","product","inputProps.topic as topic","inputProps.KafkaPartition as KafkaPartition","inputProps.fromOffset as fromOffset",
        "inputProps.toOffset as toOffset")

       activityDF.show()

   /* activityDF.write.partitionBy("topic","KafkaPartition","timestamp_hour")
         .mode(SaveMode.Append)
         .parquet("hdfs://lambda-pluralsight:9000/lambda/weblogs-api")*/
    }
    val newActivityStream = activityStream.transform(rdd=> {
      val df = spark.createDataFrame(rdd)
      df.createOrReplaceTempView("activityByStream")
      val activityByProduct = spark.sql(
        """select product,
          |timestamp_hour,
          |sum(case when action='purchase' then 1 else 0 end) as purchase_count,
          |sum(case when action='add_to_cart' then 1 else 0 end) as add_to_cart_count,
          |sum(case when action='page_view' then 1 else 0 end) as page_view_count
          |from activityByStream
          |group by product,timestamp_hour """.stripMargin)
      import spark.implicits._
      val ret=activityByProduct.map(row=> {
        ((row.getString(0),row.getLong(1)),ActivityByProduct(row.getString(0),row.getLong(1),row.getLong(2),row.getLong(3),row.getLong(4)))
      }).rdd
      ret
    })
    newActivityStream.updateStateByKey((newItemsPerKey: Seq[ActivityByProduct],currentState:Option[(Long,Long,Long)])=> {
      var (purchase_count,add_to_cart_count,page_view_count)=currentState.getOrElse((0L,0L,0L))

      newItemsPerKey.foreach( a=> {
        purchase_count +=a.purchase_count
        add_to_cart_count +=a.add_to_cart_count
        page_view_count +=a.page_view_count
      })
      Some(purchase_count,add_to_cart_count,page_view_count)
    }).print(2)

    val mappingFunction = (k:(String,Long),data:Option[ActivityByProduct],state:State[(Long,Long,Long)]) => {

      var (purchase_count,add_to_cart_count,page_view_count)=state.getOption().getOrElse((0L,0L,0L))


      val newVal =data match {
        case Some(a) => (a.purchase_count,a.add_to_cart_count,a.page_view_count)
        case _ =>  (0L,0L,0L)
      }
      purchase_count +=newVal._1
      add_to_cart_count +=newVal._2
      page_view_count +=newVal._3

      state.update((purchase_count,add_to_cart_count,page_view_count))

      val underExposed = purchase_count match {
        case 0 => 0
        case _ => page_view_count / purchase_count
      }
      underExposed
    }
    val mapStateSpec=StateSpec.function(mappingFunction).timeout(Seconds(30))
    val mappedActivityStream = newActivityStream.mapWithState(mapStateSpec)

    mappedActivityStream.print()

    //dStream.print
    newActivityStream.print
    ssc.start()
    ssc.awaitTermination()
  }

}
