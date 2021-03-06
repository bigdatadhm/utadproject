
//imports por la parte de spark

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql._


//imports por la parte de spark streaming

import org.apache.spark.streaming._

//imports por la parte de kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


//imports por la parte de Cassandra

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector._
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement
import java.net.URISyntaxException


import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter



import org.apache.log4j.{Level, Logger}

object Inicializa{

  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val bj= new App2("127.0.0.1",9042,"localhost:9092","miGrupo","datosTaxis","hdfs://localhost:9000/user/dhm/")
  }
}
object LanzadorProcesarKafka{

  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val bj= new App2("127.0.0.1",9042,"localhost:9092","miGrupo","datosTaxis","hdfs://localhost:9000/user/dhm/datosTaxis.parquet")
  }
}

//class  App2(cassNodes: String) {
class  procesarKafka(cassNodes: String, cassPort: Int, kfkServers: String, kfkGroup: String, kfkTopics: String,  parquetDest: String) {

  //esta aplicación recibe como primer parámetro un string con los nodos de Cassandra
  //como segundo parámetro el puerto de Cassandra
  //como tercer parámetro el bootstrap.servers de kafka
  //como quinto el topic al que se suscribe
  //como sexto el hdfs de destino de parquet "hdfs://localhost:9000/user/dhm/"


  private val conf = new SparkConf().setAppName("kafkaStreaming").setMaster("local")

  val streamingContext = new StreamingContext(conf, Seconds(15))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> kfkServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> kfkGroup,
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array(kfkTopics)
  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  /*
    //implementar kafkaconsumer para leer gson

    import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
    import org.apache.kafka.common.serialization.StringSerializer
    import collection.JavaConverters._

    // Propiedades del productor
    val config = new Properties()
    // URL del servidor de Kafka
    val address = kfkServers
    config.setProperty("bootstrap.servers",kfkServers)
        // Definimos las propiedades
        // URL del servidor Kafka
    // Grupo al que nos conectaremos
    config.setProperty("group.id",kfkGroup)
    // Definimos el consumidor
    val consumer = new KafkaConsumer[String, String](config, new StringDeserializer, new StringDeserializer)
    // Nos suscribimos al topic
    val topics = List(kfkTopics).asJava
    consumer.subscribe(topics)
    // Consumimos el topic con un timeout
    val received = consumer.poll(600)
    if(!received.isEmpty){
      println(s"There are ${received.count()} entries")

      received.forEach()
      received.forEach((x: ConsumerRecord[String, String])=>{
          println(s"Received: ${x.value()}")
        })
    }
    // Cerramos
    consumer.close()
  */

  val cassCluster = Cluster.builder().addContactPoint(cassNodes).withPort(cassPort).build()
  val cassSession = cassCluster.connect()

  println(s"Connected to cluster: ${cassCluster.getMetadata.getClusterName}")

  // Reducimos el nivel del logger
  private val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  //      val rowStream =stream.map(record => (record.key, record.value))
  val rowStream = stream.map(record => record.value)

  val date = new java.util.Date

  //formato a probar 2017-07-19 09:55:42+0000
  /*  val now = new DateTime
    val pattern = "yyyy-MM-dd HH:mm:ssZ"
    val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)
    val formatted: String = formatter.print(now)
    System.out.println(formatted)

  */
  val miTimestamp = new java.sql.Timestamp(date.getTime)
  System.out.println(miTimestamp)

  rowStream.foreachRDD((rdd: RDD[String]) => {

    val sparkContext = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
    import sparkContext.implicits._
    import org.apache.spark.sql.functions._

    val splitRdd = rdd.map(_.split(","))

    //val viajesDF = splitRdd.map(w => yellowSchema(w(0).toString,w(1).toString,w(2).toString,w(3),w(4),w(5),w(6),w(7).toString,
    //  w(8).toString,w(9),w(10),w(11).toString,w(12),w(13),w(14),w(15),w(16),w(17),w(18)).toDF()

    val viajesDF = splitRdd.map { case Array(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18) =>
      yellowSchema(s0.toInt, s1, s2, s3.toInt, s4.toDouble, s5.toFloat, s6.toFloat, s7.toInt, s8.toString,
        s9.toFloat, s10.toFloat, s11.toInt, s12.toDouble, s13.toDouble,
        s14.toDouble, s15.toDouble, s16.toDouble, s17.toDouble, s18.toDouble)
    }.toDF()

    viajesDF.show()
    // val viajesDateDF = viajesDF.withColumn("trip_date",to_date(formatted.toString))
    println("vamos a agregar la columna con timestamp")

    val viajesDateDF = viajesDF.withColumn("trip_date",current_timestamp() )

    println("hemos agregado la columna con timestamp")

    //val viajesDateDF = viajesDF.withColumn("trip_date",to_date()expr("'"+formatted+"'"))
    //viajesDateDF.schema.printTreeString()
    //miTimestamp
    //viajesDF.write.mode(SaveMode.Append).parquet("/tmp/parquet")

    println("vamos a escribir en parquet el viajesDateDF")
    viajesDateDF.write.mode(SaveMode.Append).parquet(parquetDest)
    //viajesDateDF.write.option("spark.sql.parquet.mergeSchema", "true").mode(SaveMode.Append).parquet(parquetDest)
    //viajesDateDF.write.mode(SaveMode.Append).parquet(parquetDest)
    println("vamos a mostrar el viajesDateDf")

    viajesDateDF.show()
    println("mostrado viajesDateDf")

    //Calculamos los agregados para persistirlos en cassandra

    println("ahora hacemos el agregado")

    //val viajesAggDF=viajesDF.groupBy("vendor_id", "payment_type").sum("passenger_count as ")


    val viajesAggDF=viajesDateDF.groupBy("trip_date", "vendor_id", "payment_type")
      .agg(
        //expr("'"+date.toString+"' as trip_date") -- funciona, pero la fecha hay que ponerla antes (parquet)
        expr("sum(passenger_count) as sum_passenger_count"),
        expr("max(passenger_count) as max_passenger_count"),
        expr("avg(passenger_count) as avg_passenger_count"),
        expr("sum(trip_distance) as sum_trip_distance"),
        expr("max(trip_distance) as max_trip_distance"),
        expr("avg(trip_distance) as avg_trip_distance"),
        expr("sum(fare_amount) as sum_fare_amount"),
        expr("max(fare_amount) as max_fare_amount"),
        expr("avg(fare_amount) as avg_fare_amount"),
        expr("sum(tip_amount) as sum_tip_amount"),
        expr("max(tip_amount) as max_tip_amount"),
        expr("avg(tip_amount) as avg_tip_amount"),
        expr("sum(total_amount) as sum_total_amount"),
        expr("max(total_amount) as max_total_amount"),
        expr("avg(total_amount) as avg_total_amount"),
        expr("count(*) as count_trips")
      )
    viajesAggDF.show()
    //viajesAggDF.save("org.apache.spark.sql.cassandra",SaveMode.Overwrite,options = Map( "c_table" -> "test1", "keyspace" -> "yana_test"))

    println("hemos mostrado el agregado")


    println("vamos a escribir en en cassandra el viajesAggF")

    viajesAggDF
      .write.format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "olaptaxis", "keyspace" ->"utadproject"))
      .mode(SaveMode.Append).save()

    println("hemos escrito en en cassandra el viajesAggF")

    //val viajesAggDateDF = viajesAggDF.withColumn(col(date.toString), "datetime")
    //val viajesAggDateDF = viajesAggDF.withColumn("datetime",expr("date.toString()"))

    //val viajesAggDateDF = viajesAggDF.withColumn("datetime", viajesAggDF.

    //viajesAggDateDF.show()
    //  println("hemos mostrado el sumDF")
    //splitRdd.saveToCassandra("utadproject","olaptaxis")



  })

  cassCluster.close()
  streamingContext.start() // Start the computation
  streamingContext.awaitTermination() // Wait for the computation to terminate

}
