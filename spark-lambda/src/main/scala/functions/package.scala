import domain.Activity
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges

package object functions {

  def rddToRDDActivity(input:RDD[ConsumerRecord[String,String]]):RDD[Activity] = {

    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

    input.mapPartitionsWithIndex({(index,it)=>
      val or = offsetRanges(index)
      it.flatMap{cr=>
        val  line = cr.value()
        val record =line.split("\\t")
        val MS_IN_HOUR= 1000 * 60 * 60
        if(record.length == 7)
          Some(Activity(record(0).toLong/MS_IN_HOUR * MS_IN_HOUR,record(1),record(2),record(3),record(4),record(5),record(6),
            Map("topic"->or.topic.toString,"KafkaPartition"->or.partition.toString,"fromOffset"->or.fromOffset.toString,"toOffset"->or.toString())))
        else
          None
      }
    })
  }
}
