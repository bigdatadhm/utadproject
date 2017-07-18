import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SaveParquet {
  def main(args: Array[String]) {

    // Reducimos el nivel del logger
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaBrokers = "localhost:9092"
    val sparkConf = new SparkConf().setAppName("SaveParquet").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topics = "yellow"
    val topicsSet = topics.split(",").toSet
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    val words = stream.map(record => (record.key, record.value)).flatMap(x => x._2.split("\n"))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD { rdd =>

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      import spark.implicits._
      val words = rdd.map(_.split(","))

      val df = words.map { case Array(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16) =>
        Record(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16)
      }.toDF()

      /*   df.createOrReplaceTempView("temp")
         val query = spark.sql("select count(*) from temp")
         query.show() */

      // df.printSchema()
      // df.write.mode(SaveMode.Append).parquet("hdfs://localhost:9000/user/Taxis/yellow/");
      df.writeStream.queryName("SaveToCassandra").outputMode("append")
      val ds = df.select($"vendorid", $"tpep_pickup_datetime", $"tpep_dropoff_datetime",
        $"passenger_count", $"trip_distance", $"ratecodeid", $"store_and_fwd_flag",
        $"pulocationID", $"doLocationID", $"payment_type", $"fare_amount", $"extra",
        $"mta_tax", $"tip_amount", $"tolls_amount", $"improvement_surcharge", $"total_amount").as[Record]

      // This Foreach sink writer writes the output to cassandra.

      /* val query = ds.writeStream.queryName("SaveToCassandra").outputMode("append").foreach(writer).start()
       query.awaitTermination() */



    }

    ssc.start()
    ssc.awaitTermination()

  }


  case class Record(VendorID: String,
                    tpep_pickup_datetime: String,
                    tpep_dropoff_datetime: String,
                    passenger_count: String,
                    trip_distance: String,
                    ratecodeid: String,
                    store_and_fwd_flag: String,
                    pulocationid: String,
                    dolocationid: String,
                    payment_type: String,
                    fare_amount: String,
                    extra: String,
                    mta_tax: String,
                    tip_amount: String,
                    tolls_amount: String,
                    improvement_surcharge: String,
                    total_amount: String)

}
