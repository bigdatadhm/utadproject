//imports por la parte de spark

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql._

//imports por la parte de Cassandra

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector._
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement
import java.net.URISyntaxException
import java.util

import org.joda.time.DateTime
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}

object LanzadorConsultas{

  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val llamador= new Consultas("127.0.0.1", 9042, "hdfs://localhost:9000/user/dhm/datosTaxis.parquet")
  }
}

class Consultas(cassNodes: String, cassPort: Int, parquetDest: String) {

  // Este módulo sirve para hacer la misma consulta contra Cassandra y parquet
  // Las ejecuta ambas y calcula y compara los tiempos necesartios
  // así como los resultados
  // recibe como primer parámetro un string con los nodos de Cassandra
  // como segundo parámetro el puerto de Cassandra
  // como tercero el hdfs de destino de parquet "hdfs://localhost:9000/user/dhm/"

  private val conf = new SparkConf().setAppName("moduloConsultas").setMaster("local")

  val consulta1 = "select vendor_id, payment_type, count(*), sum(total_amount) from datosTaxisTBL group by vendor_id, payment_type"
  val consulta2 = "select vendor_id, payment_type, sum(count_trips), sum(sum_total_amount) from datosTaxisTBLCass group by vendor_id, payment_type"

  val sc = new SparkContext(conf)
  val sqlContext = SparkSession.builder()
    .master("local")
    .appName("SQLmoduloConsultas")
    .getOrCreate()

  val cassCluster = Cluster.builder().addContactPoint(cassNodes).withPort(cassPort).build()
  val cassSession = cassCluster.connect()

  println(s"Connected to cluster: ${cassCluster.getMetadata.getClusterName}")

  // Reducimos el nivel del logger
  private val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

println("vamos a abrir el fichero parquet "+parquetDest)
  val datosTaxisParquetDF = sqlContext.read.parquet(parquetDest)
  println("abierto el fichero parquet "+parquetDest)

  datosTaxisParquetDF.schema.printTreeString()

  for( a <- 1 to 100000){
    println( "BUCLE: entramos en iteracion "+a );

  //datosTaxisParquetDF.show(10)
  datosTaxisParquetDF.createOrReplaceTempView("datosTaxisTBL")

  //println("vamos a ejecutar SQL contra parquet: datosTaxisTBL")

  val date1 = new java.util.Date
  val timestampInicio1 = new java.sql.Timestamp(date1.getTime)
  println("Hora inicio= "+timestampInicio1.toString)
    sqlContext.sql(consulta1).collect.foreach(println)
    //sqlContext.sql("select vendor_id, payment_type, sum(total_amount) from datosTaxisTBL group by vendor_id, payment_type")
    val date2 = new java.util.Date
  val timestampFin1 = new java.sql.Timestamp(date2.getTime)
  println("Hora Fin= "+timestampFin1.toString)
  //val dif = timestampFin-timestampInicio
  //println("Duración consulta a Parquet= "+diferencia)

    //println("vamos a ejecutar SQL contra Cassandra: olaptaxis")

    val date3 = new java.util.Date
    val timestampInicio2 = new java.sql.Timestamp(date3.getTime)
    println("Hora inicio= "+timestampInicio2.toString)
    val olapTaxisDF = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "olaptaxis", "keyspace" -> "utadproject"))
      .load()

    olapTaxisDF.createOrReplaceTempView("datosTaxisTBLCass")
    sqlContext.sql(consulta2).collect.foreach(println)

    val date4 = new java.util.Date
    val timestampFin2 = new java.sql.Timestamp(date4.getTime)
    println("Hora Fin= "+timestampFin2.toString)
    //Thread.sleep(600000)
   Thread.sleep(60000)
    sqlContext.catalog.refreshTable("datosTaxisTBL")

  }
}
