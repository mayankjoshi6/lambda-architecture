package streaming

import config.Settings
import domain.{Activity, ActivityByProduct}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import utils.SparkUtils.{getSparkSession, isIDE}

object KafkaStreamingJob {
  def main(args:Array[String]) = {
    val spark=getSparkSession("lambda-streaming")
    val ssc=new StreamingContext(spark.sparkContext,Seconds(4))
    val wlc = Settings.WebLogGen
    val webLogTopic = wlc.weblogTopic


    val sourceFilePath = if (isIDE()) {
      "file:///C:/Users/Mayank/Documents/study/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
    } else {
      "file:///vagrant/input"
    }
    println(s"Looking for files in $sourceFilePath")

    //val dStream=ssc.textFileStream(sourceFilePath)

    val dStream = KafkaUtils.createStream(ssc,"localhost:2181","lambda-consumer",Map(webLogTopic->1)).map(_._2)

    val activityStream = dStream.transform(input=> {
      input.flatMap{line=>
        val record =line.split("\\t")
        val MS_IN_HOUR= 1000 * 60 * 60
        if(record.length == 7)
          Some(Activity(record(0).toLong/MS_IN_HOUR * MS_IN_HOUR,record(1),record(2),record(3),record(4),record(5),record(6)))
        else
          None
      }
    })
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
