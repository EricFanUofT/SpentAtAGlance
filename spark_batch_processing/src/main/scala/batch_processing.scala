import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import com.tdunning.math.stats.TDigest
import com.tdunning.math.stats._
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;


object Batch_Processing {

    def main(args: Array[String]) {
//Logger.getLogger("org").setLevel(Level.OFF)
//Logger.getLogger("akka").setLevel(Level.OFF)
    // setup the Spark Context named sc
    val conf = new SparkConf().setAppName("Batch Processing").set("spark.cassandra.connection.host","52.34.145.96")
    val sc = new SparkContext(conf)
    // folder on HDFS to pull the data from
    //val folder_name="hdfs://ec2-52-27-139-213.us-west-2.compute.amazonaws.com:9000/camus/topics/card_transactions/*/*/*/*/*/"
    val folder_name="hdfs://ec2-52-37-105-222.us-west-2.compute.amazonaws.com:9000/insight/"

    // setup the sqlContext
    val sqlContext=SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._

    //open the file from HDFS into a dataframe
    val file = sc.textFile(folder_name+"transaction_data.json")
    val df = sqlContext.read.json(file)

   def convert_to_quarters(date:String):String={
         val quart=(date.slice(5,7).toInt-1)/3+1
         date.take(5)+"Q"+quart.toString
    }
    
    //add a column for month 
    val df_month_added=df.select(df("date"),df("date").substr(0,7).alias("month"),df("day_of_week"),df("time"),df("name"),df("trans_type"),df("amount"))
    //add a column for quarter (every 3 months)
    df_month_added.map(t=>(t(0).toString,t(1).toString,convert_to_quarters(t(0).toString),t(2).toString.toInt,t(3).toString,t(4).toString,t(5).toString,"%.2f".format(t(6)).toDouble)).toDF("date","month","quarter","day_of_week","time","name","trans_type","amount").registerTempTable("transaction_data")


   //determine the daily total and maximum transaction and save to Cassandra (for each credit card user)
   val daily_summary= sqlContext.sql("SELECT name, date, sum(amount) as total_transaction, max(amount) as maximum_transaction FROM transaction_data GROUP BY name,date").map{case Row(name:String, date:String, total_transaction:Double, maximum_transaction:Double)=>Transaction_by_Day(name, date, total_transaction, maximum_transaction)}.saveToCassandra("transaction_space", "daily_summary")
   
   //determine the daily total and maximum transaction on particular day of the week and save to Cassandra (for each user)
   val day_of_week_summary=sqlContext.sql("SELECT name, date,CAST(day_of_week as Int), sum(amount) as total_transaction, max(amount) as maximum_transaction FROM transaction_data GROUP BY name, day_of_week, date").map{case Row(name:String, date:String, day_of_week:Int, total_transaction:Double, maximum_transaction:Double)=>Transaction_by_Day_of_Week(name, date, day_of_week,total_transaction, maximum_transaction)}.saveToCassandra("transaction_space","day_of_week_summary")
 
   //determine the monthly total and maximum transaction and save to Cassandra (for each user)
   val monthly_summary=sqlContext.sql("SELECT name, month,sum(amount) as total_transaction, max(amount) as maximum_transaction FROM transaction_data GROUP BY name, month").map{case Row(name:String, month:String, total_transaction:Double, maximum_transaction:Double)=>Transaction_by_Month(name,month,total_transaction, maximum_transaction)}.saveToCassandra("transaction_space", "monthly_summary")
  
   //determine the monthly total, maximum transaction for all users and the number of distinct buyer
   val monthly_summary_all=sqlContext.sql("SELECT 'key' as key, month, sum(amount) as total_transaction, max(amount) as maximum_transaction, count(DISTINCT name) as distinct_users FROM transaction_data GROUP BY month").map{case Row(key:String, month:String, total_transaction:Double, maximum_transaction:Double, distinct_users:Long)=>Transaction_by_Month_all_Users(key, month,total_transaction,maximum_transaction,distinct_users)}.saveToCassandra("transaction_space","monthly_summary_all_users")
   
   //determine the monthly total transaction for each category for each user
   val categorical_summary=sqlContext.sql("SELECT name,trans_type, month, sum(amount) as total_transaction FROM transaction_data GROUP BY name, trans_type, month").map{case Row(name:String, trans_type:String, month:String, total_transaction:Double)=>Transaction_by_Month_Category(name:String, trans_type:String,month:String,total_transaction:Double)}.saveToCassandra("transaction_space", "user_category_summary")
  
   //determine the monthly total transaction and number distinct buyers for each category 
   val categorical_summary_all_user=sqlContext.sql("SELECT trans_type, month, sum(amount) as total_transaction, count(DISTINCT name) as distinct_users FROM transaction_data GROUP BY trans_type, month").map{case Row(trans_type:String, month:String, total_transaction:Double, distinct_users:Long)=>Transaction_by_Month_Category_all_Users(trans_type:String,month:String,total_transaction:Double, distinct_users:Long)}.saveToCassandra("transaction_space","category_summary_all_users")

    //find the 25th, 50th, 75th, and 90th percentile of transaction amount for each category for each quarter (3 months)
    //list all the categories and which time period they belong in the historical data
    val distinct_quarter_category=sqlContext.sql("SELECT DISTINCT quarter, trans_type FROM transaction_data").collect()
    for (x<-distinct_quarter_category){
        val d_quarter=x(0)
        val d_cat=x(1)
        val quarterly_transaction=sqlContext.sql("SELECT amount FROM transaction_data WHERE quarter='"+d_quarter+"' AND trans_type='"+d_cat+"' ")
        //perform t-digest to calculate the percentiles and save to cassandra
        quarterly_transaction.map{t=>
                                   t(0).asInstanceOf[Double]
                           }.mapPartitions{itr=>
                                               val trans_dist=TDigest.createArrayDigest(32,100)
                                               itr.foreach{data =>
                                                                  trans_dist.add(data)
                                               }
                                               Seq("t"->trans_dist).toIterator.map{case(k,v)=>
                                                                                              val arr=new Array[Byte](v.byteSize)
                                                                                              v.asBytes(ByteBuffer.wrap(arr))
                                                                                              k->arr
                                               }
                            }.reduceByKey{case(t1Arr,t2Arr)=>
                                                             val merged=ArrayDigest.fromBytes(ByteBuffer.wrap(t1Arr))
                                                             merged.add(ArrayDigest.fromBytes(ByteBuffer.wrap(t2Arr)))
                                                             val arr=new Array[Byte](merged.byteSize)
                                                             merged.asBytes(ByteBuffer.wrap(arr))
                                                             arr
                            }.map{case(arr)=>
                                             val merged=ArrayDigest.fromBytes(ByteBuffer.wrap(arr._2))
                                             (d_cat,d_quarter,merged.quantile(0.25d), merged.quantile(0.5d), merged.quantile(0.75d), merged.quantile(0.90d))
                            }.map{t=>(t._1.toString,t._2.toString,t._3,t._4,t._5,t._6)
			    }.saveToCassandra("transaction_space","quarterly_category_percentile",SomeColumns("category","quarter","twenty_fifth","fiftieth","seventy_fifth","ninetieth"))
                            
    }
  }
}  


case class Transaction_by_Month_Category_all_Users(trans_type:String, month:String, total_transaction:Double, distinct_users:Long)
case class Transaction_by_Month_Category(name:String,trans_type:String, month:String, total_transaction:Double)
case class Transaction_by_Month_all_Users(key:String, month:String, total_transaction:Double,maximum_transaction:Double, distinct_users:Long)
case class Transaction_by_Day(name: String, date: String, total_transaction: Double, maximum_transaction: Double)
case class Transaction_by_Month(name: String, month:String, total_transaction: Double, maximum_transaction: Double)
case class Transaction_by_Day_of_Week(name: String, date: String, day_of_week: Int, total_transaction: Double, maximum_transaction: Double)
case class Transaction_by_User_Category(name:String, trans_type:String, month:String, total_transaction:Double)
case class Transaction_by_Category_Month(trans_type: String, month: String, total:Double, distinct_users: Int)

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
