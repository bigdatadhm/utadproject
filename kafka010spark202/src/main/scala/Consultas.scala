//imports por la parte de spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.implicits._

//imports por la parte de spark

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
  val sc = new SparkContext(conf)
  val sqlContext = SparkSession.builder()
    .master("local")
    .appName("SQLmoduloConsultas")
    .getOrCreate()

  //val streamingContext = new StreamingContext(conf, Seconds(60))


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


  datosTaxisParquetDF.show(10)
  datosTaxisParquetDF.createOrReplaceTempView("datosTaxisTBL")

  println("vamos a ejecutar SQL contra datosTaxisTBL")

  sqlContext.sql("select vendor_id, payment_type, sum(total_amount) from datosTaxisTBL group by vendor_id, payment_type").collect.foreach(println)
  println("ejecutado SQL contra datosTaxisTBL")

  //val parquetFileDF = spark.read.parquet("people.parquet")
  // Parqfile.registerTempTable(“employee”)
  // val allrecords = sqlContext.sql("SELeCT * FROM employee")
  // allrecords.show()
  // my_data = sqlContext.read.parquet('hdfs://my_hdfs_path/my_db.db/my_table')
  // sqlContext.sql("select * from people").collect.foreach(println)


}
