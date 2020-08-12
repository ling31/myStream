import org.apache.spark.sql.SparkSession

object Test01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("test01")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val inputDataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.100.0.235:1025")
      .option("subscribe", "test8")
      .load()
    //    inputDataFrame.printSchema()
    import spark.implicits._
    val keyValueDataset = inputDataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)").as[(String, String,String)]


    val subwayDataFrame = keyValueDataset.flatMap(t => {
      Array((t._2,t._3))
    }).toDF("value","topic")

    subwayDataFrame.createTempView("test")

    //    val result = spark.sql("SELECT c1, count(1) as num1 FROM test GROUP BY c1 ORDER BY num1 desc")
    val result = spark.sql("select * from test")

    val query = result.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "./test01")
      .start()

    query.awaitTermination()
  }
}