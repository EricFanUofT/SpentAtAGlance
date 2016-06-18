import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._

object Batch_Processing {

  def main(args: Array[String]) {

    // setup the Spark Context named sc
    val conf = new SparkConf().setAppName("Batch Processing").set("spark.cassandra.connection.host","52.37.185.30")
    val sc = new SparkContext(conf)
    // folder on HDFS to pull the data from
    //val folder_name="hdfs://ec2-52-27-139-213.us-west-2.compute.amazonaws.com:9000/camus/topics/card_transactions/*/*/*/*/*/"
    val folder_name="hdfs://ec2-52-27-139-213.us-west-2.compute.amazonaws.com:9000/insight/"

    // setup the sqlContext
    val sqlContext=SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._

    //open the file from HDFS into a dataframe
    val file = sc.textFile(folder_name+"transaction_data.json")
    val df = sqlContext.read.json(file)

    val df_month_added=df.select(df("date"),df("date").substr(0,5).alias("month"),df("day_of_week"),df("time"),df("name"),df("amount")).toDF()
    df_month_added.registerTempTable("transaction_data")
    // df_month_added.printSchema()

    //save the daily, monthly, and each day of the week summary to Cassandra
    val daily_summary= sqlContext.sql("SELECT name, date, sum(amount) as total_transaction, max(amount) as maximum_transaction FROM transaction_data GROUP BY name,date").map{case Row(name:String, date:String, total_transaction:Double, maximum_transaction:Double)=>Transaction_by_Day(name, date, total_transaction, maximum_transaction)}.saveToCassandra("transaction_space", "daily_summary")
    val monthly_summary=sqlContext.sql("SELECT name, month,sum(amount) as total_transaction, max(amount) as maximum_transaction FROM transaction_data GROUP BY name, month").map{case Row(name:String, month:String, total_transaction:Double, maximum_transaction:Double)=>Transaction_by_Month(name,month,total_transaction, maximum_transaction)}.saveToCassandra("transaction_space", "monthly_summary")
   val day_of_week_summary=sqlContext.sql("SELECT name, date,CAST(day_of_week as Int), sum(amount) as total_transaction, max(amount) as maximum_transaction FROM transaction_data GROUP BY name, day_of_week, date").map{case Row(name:String, date:String, day_of_week:Int, total_transaction:Double, maximum_transaction:Double)=>Transaction_by_Day_of_Week(name, date, day_of_week,total_transaction, maximum_transaction)}.saveToCassandra("transaction_space","day_of_week_summary")
  }
}

case class Transaction_by_Day(name: String, date: String, total_transaction: Double, maximum_transaction: Double)
case class Transaction_by_Month(name: String, month:String, total_transaction: Double, maximum_transaction: Double)
case class Transaction_by_Day_of_Week(name: String, date: String, day_of_week: Int, total_transaction: Double, maximum_transaction: Double)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

