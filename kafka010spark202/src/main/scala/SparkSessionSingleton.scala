import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}

case class  yellowSchema (
                           vendor_id: String,
                           tpep_pickup_datetime: String,
                           tpep_dropoff_datetime: String,
                           passenger_count:  Long,
                           trip_distance: Double,
                           pickup_longitude: Float,
                           pickup_latitude: Float,
                           rate_code_id:  String,
                           store_and_fwd_flag:  String,
                           dropoff_longitude: Float,
                           dropoff_latitude: Float,
                           payment_type:  String,
                           fare_amount: Double,
                           extra: Double,
                           mta_tax: Double,
                           tip_amount: Double,
                           tolls_amount: Double,
                           improvement_surcharge: Double,
                           total_amount: Double
                         )

