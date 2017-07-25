//imports por la parte de spark

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.conf._

import java.io.BufferedReader
import java.io.InputStreamReader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

//imports por la parte de kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}

object LanzadorGeneraDatos{

  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val bj= new generadorDatos("localhost:9092","miGrupo","datosTaxis", "hdfs://localhost:9000/user/dhm/yellow_tripdata_2016-01.csv")
  }
}

class generadorDatos (kfkServers: String, kfkGroup: String, kfkTopics: String, fichCsv: String) {

  //esta aplicación recibe como primer parámetro el bootstrap.servers de kafka
  //como segundo el grupo de kafka
  // como tercerlo el topic al que se suscribe
  // como cuarto, el fichero csv a enviar por kafka


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> kfkServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> kfkGroup,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array(kfkTopics)


// case class yellowSchema ya definida en SparkSessionSingleton.scala

    val res = new yellowSchema(2,"2016-01-01 00:00:00","2016-01-01 00:00:00",2,1.10,-73.990371704101563.toFloat,40.734695434570313.toFloat,1,"N",-73.981842041015625.toFloat,40.732406616210937.toFloat,2,7.5,0.5,0.5,0,0,0.3,8.8)
    val gson = new Gson()
    val serialized = gson.toJson(res)
    println(s"Serialized ${serialized}")
    val recovered = gson.fromJson(serialized,classOf[yellowSchema])

    println(s"Recovered ${recovered}")

  println("Vamos a abrir el fichero CSV "+fichCsv)

  private val conf = new Configuration()
  println("el open 1")
  conf.addResource(new Path("/usr/local/hadoop-2.7.3/etc/hadoop/core-site.xml"))
  val fs = FileSystem.get(conf)

  println("el open 1")
  println(conf.getRaw("fs.default.name"))
  println(conf.toString)
  val inFile = new Path(fichCsv)
  println("el open 2")
  val in: FSDataInputStream = fs.open(inFile)
  println("abierto")

  val registro = in.readLine().split(",")
  //println(registro)

  val serialized2 = gson.toJson(registro)
  println(s"Serialized ${serialized2}")


  //val res2 = new yellowSchema(registro)

  //val bufferedReader = new BufferedReader(new InputStreamReader(in))
  //val line: String = bufferedReader.readLine
  //println(line)
}
