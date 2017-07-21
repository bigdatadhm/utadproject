
//imports por la parte de spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

//imports por la parte de spark streaming

import org.apache.spark.streaming._

//imports por la parte de kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


import org.apache.log4j.{Level, Logger}


object App2 {

    def main(args: Array[String]) {



      // Reducimos el nivel del logger
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.ERROR)


      val conf = new SparkConf().setAppName("kafkaStreaming").setMaster("local")
//      val sparkContext = new SparkContext(conf)

      val streamingContext = new StreamingContext(conf, Seconds(10))


      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "miGrupo",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val topics = Array("datosTaxis")
      val stream = KafkaUtils.createDirectStream[String, String](
        streamingContext,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

//      val rowStream =stream.map(record => (record.key, record.value))
      val rowStream =stream.map(record => (record.value))

      rowStream.foreachRDD((rdd: RDD[String]) => {

        val sparkContext = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import sparkContext.implicits._
        import org.apache.spark.sql.functions._

        val splitRdd = rdd.map(_.split(","))

        //val schemaDataFrame = rdd.map(w => yellowSchema(w(0).toString,w(1).toString,w(2).toString,w(3),w(4),w(5),w(6),w(7).toString,
        //  w(8).toString,w(9),w(10),w(11).toString,w(12),w(13),w(14),w(15),w(16),w(17),w(18)).toDF()

        val viajesDF = splitRdd.map { case Array(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18) =>
          yellowSchema(s0, s1, s2, s3.toInt, s4.toDouble, s5.toFloat, s6.toFloat, s7.toString, s8.toString,
            s9.toFloat, s10.toFloat, s11, s12.toDouble, s13.toDouble,
            s14.toDouble, s15.toDouble, s16.toDouble, s17.toDouble, s18.toDouble)
        }.toDF()

        viajesDF.show()
        //viajesDF.write.mode(SaveMode.Append).parquet("/tmp/parquet")
        viajesDF.write.mode(SaveMode.Append).parquet("hdfs://sparknode01.localdomain:9000/user/dhm/")
      })

      streamingContext.start()             // Start the computation
      streamingContext.awaitTermination()  // Wait for the computation to terminate

    }
  }

