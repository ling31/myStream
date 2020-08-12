package com.ling.example.sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class JavaTest01 {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[5]")
                .appName("test01")
                .config("spark.driver.host", "localhost")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<String> inputDataFrame = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
                .option("subscribe", "test8")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)")
                .as(Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = inputDataFrame.flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING()).groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }

}
