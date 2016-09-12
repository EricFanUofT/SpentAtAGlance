# SpentAtAGlance
## Help Track Your Expense!

### Summary
SpentAtAGlance is a tool that summarizes for the users how much money they have spent on the current day/current month, compares their current spendings with those of the past few days, past few months, and also past few same day of the week (taking into account users might have different transaction activities on different days of the week, e.g. more on the weekends), and what have they been spending money on.  In addition, it comments on whether the users are meeting their budgeting target.  Essentially, this is a tool for users to quickly visualize how much they have spent already before making a new purchase, and hopefully helps the users to meet their budgeting goals.

*The presentation slides can be found* [*here*] (http://www.slideshare.net/EricFan19/eric-fan-insight-project-demo-63572736?qid=e24fd0fb-73f2-476e-8c99-057d605768f2&v=&b=&from_search=1)

Card holder API (http://www.spentglance.online/)

<p align="center">
  <img src="/images/SpentAtAGlance_screenshot1.png" >
</p>

On the other hand, it summarizes statistics for the credit card company including the total monthly transactions of all users, total monthly transaction of an average user, and the percentiles of transactions of different transaction categories.  The results will help the company to make more informed marketing decisions.

Card company API (http://www.spentglance.online/card_company)

<p align="center">
  <img src="/images/SpentAtAGlance_screenshot2.png" />
</p>

### Data Engineering Tools
SpentAtAGlance uses the following tools for the data pipeline:

- Apache Kafka
- Apache HDFS
- Spark
- Spark Streaming
- Apache Cassandra
- Flask with Highcharts and Bootstrap

<p align="center">
  <img src="/images/data_pipeline.png" width="550"/>
</p>

### Data Source
SpentAtAGlance uses producer scripts found in the kafka_producers folder to synthesis the data source as json formatted strings. The input has the following schema:

- "date": String      ("%Y-%m-%d")
- "day_of_week": int
- "time": String      ("%H-%M-%S")
- "name": String
- "trans_type": String
- "amount": double

The historical data for batch processing is ~50 GB in size containing transaction record as early as Jan 1, 2015.  There are approximately 1.3 million different users.  The number of transactions per day is randomly simulated with variety between different days of the week and different months of the year.  There are 20 different transaction categories, each with a different average cost and standard deviation.  

### Data Ingestion
Apache Kafka is used for reading messages produced by the kafka_producers into the topic "transactions_message" which can be consumed by Camus (to store to hdfs for batch processing) and by Spark-Streaming (for stream processing)

### Batch Processing
Ideally, messages from the Kafka topic would be consumed by Camus to HDFS for batch processing daily by Spark to update the statistical summaries.  But for the sake of this project, the 50 GB transaction data was already synthesized and stored in HDFS; nevertheless, Camus has been tested to work with the current pipeline since the messages are in json format.
Various processes are performed by Spark including:

- determining the total and maximum transaction per day, per day on a particular day of the week, and per month
- determining the total monthly transaction per month of all users and number of distinct spenders
- determining the monthly total transaction for each category for each user
- determining the monthly total transaction and number of distinct spenders for each category
- determining the approximate 25th, 50th, 75th, and 90th percentile of transaction amount for each category for each quarter (every 3 months) using the T-digest approach

The results are stored in Casssandra

sbt library dependencies

- "com.tdunning" % "t-digest" % "3.1"
- "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M2"
- "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
- "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"


### Stream Processing
While the previous day's transaction data are being batch processed, new transaction data will be stream processed and stored in a temporary table named "transaction_logs" in Cassandra.  The micro batch timeframe is 1 s (consecutive transactions realistically take more than 1 s to be processed).  The process involved include:

- reading the name, date, day_of_week, amount, transaction type into Cassandra

sbt library dependencies

- "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M2"
- "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"
- "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided"
- "org.apache.spark" % "spark-streaming_2.10" % "1.6.1" % "provided"
- "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1"


### Database
Cassandra is utilized for storing the data (since the data involves time series).  The keyspace and various tables were created in Cassandra using:

- CREATE KEYSPACE transaction_space WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
  - CREATE TABLE daily_summary (name text, date text, total_transaction double, maximum_transaction double, PRIMARY KEY(name,date)) WITH CLUSTERING ORDER BY (date DESC);
  - CREATE TABLE monthly_summary(name text, month text, total_transaction double, maximum_transaction double, PRIMARY KEY(name, month)) WITH CLUSTERING ORDER BY (month DESC);
  - CREATE TABLE day_of_week_summary(name text, date text, day_of_week text, total_transaction double, maximum_transaction double, PRIMARY KEY ((name, day_of_week), date)) WITH CLUSTERING ORDER BY (date DESC);
  - CREATE TABLE user_category_summary(name text, trans_type text, month text, total_transaction double, PRIMARY KEY((name, trans_type),month))WITH CLUSTERING ORDER BY (month DESC);
  - CREATE TABLE category_summary_all_users (month text, trans_type text, total_transaction double, distinct_users bigint, PRIMARY KEY(month,trans_type)) WITH CLUSTERING ORDER BY (trans_type ASC) ;
  - CREATE TABLE monthly_summary_all_users (key text, month text, total_transaction double,maximum_transaction double, distinct_users bigint, PRIMARY KEY (key, month)) WITH CLUSTERING ORDER BY (month DESC);
  - CREATE TABLE quarterly_category_percentile (category text, quarter text, twenty_fifth double, fiftieth double, seventy_fifth double, ninetieth double, PRIMARY KEY(category, quarter)) WITH CLUSTERING ORDER BY (quarter DESC);
  - CREATE TABLE transaction_log (name text, date text, time text, day_of_week int, transaction double, trans_type text, PRIMARY KEY ((name,date),time)) WITH CLUSTERING ORDER BY (time DESC) ;

### API calls
Flask and highcharts are used to render the front-end user interface.  The batch-processed results were combined with the stream-processed data to calculate the current month's spending and for the various charts.

The user can access the tool by calling (http://www.spentglance.online/)
The credit card company can access useful statistical information by calling (http://www.spentglance.online/card_company)





