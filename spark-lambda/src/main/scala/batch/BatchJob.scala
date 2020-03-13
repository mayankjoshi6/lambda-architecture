package batch


import domain.Activity
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.SparkUtils._

object BatchJob {



  def main(args:Array[String]): Unit = {

    //val sourceFile = "file:///vagrant/data.tsv"
    val sourceFile = isIDE() match {
      case true => "file:///C:/Users/Mayank/Documents/study/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.csv"
      case _ =>  "file:///c:/tmp/spark-test/data.tsv"
    }


    val spark = getSparkSession("lambda-batch")
    val input = spark.sparkContext.textFile(sourceFile)

    val inputRDD = input.flatMap{line=>
      val record =line.split("\\t")
      val MS_IN_HOUR= 1000 * 60 * 60
      if(record.length == 7)
        Some(Activity(record(0).toLong/MS_IN_HOUR * MS_IN_HOUR,record(1),record(2),record(3),record(4),record(5),record(6)))
      else
        None
    }

    val inputDF = spark.createDataFrame(inputRDD)
    inputDF.printSchema()
    //inputRDD.foreach(println)
    inputDF.createOrReplaceTempView("activity")

    val activityByProduct = spark.sql(
      """select product,
        |timestamp_hour,
        |sum(case when action='purchase' then 1 else 0 end) as purchase_count,
        |sum(case when action='add_to_cart' then 1 else 0 end) as add_to_cart_count,
        |sum(case when action='page_view' then 1 else 0 end) as page_view_count
        |from activity
        |group by product,timestamp_hour """.stripMargin)

    activityByProduct.createOrReplaceTempView("activityByProduct")

    activityByProduct.show()

    if(isIDE()) {
      activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")
    }else {
      activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("file:///c:/tmp/batch1")
    }
    }
}
