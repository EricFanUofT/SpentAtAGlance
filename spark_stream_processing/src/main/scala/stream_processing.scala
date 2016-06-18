import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object Stream_Processing {

  def main(args: Array[String]) {

//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)

    val brokers = "ec2-52-40-145-60.us-west-2.compute.amazonaws.com:9092"
    val topics = "transactions_message"

    val topicsSet = topics.split(",").toSet

    // Create context with 1 second batch interval

    val conf=new SparkConf(true).setAppName("stream transactions").set("spark.cassandra.connection.host", "52.37.185.30")

    val ssc = new StreamingContext(conf, Seconds(1))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    messages.map(_._2).foreachRDD{ rdd =>
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val df = sqlContext.jsonRDD(rdd)

        df.registerTempTable("transaction_info")

        if(!df.rdd.isEmpty){
            //Do not allow multiple transactions to occur within one second for the same card holder (which are randomly generated); combine into a single transaction if necessary
            sqlContext.sql("SELECT name, date, time, CAST( day_of_week as Int), sum(amount) as transaction FROM transaction_info GROUP BY name, date, time, day_of_week").map{case Row(name: String, date: String, time: String, day_of_week:Int, transaction: Double) => Transaction(name,date,time,day_of_week,transaction)}.saveToCassandra("transaction_space", "transaction_log")
        }
    }
    // Start the computation

    ssc.start()
    ssc.awaitTermination()

  }

}
case class Transaction(name:String, date:String, time:String, day_of_week:Int, transaction:Double)


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

