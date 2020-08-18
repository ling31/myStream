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


//        Dataset<Row> inputDataFrame0 = spark.readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
//                .option("subscribe", "a-dl-01")
//                .option("startingOffsets", "latest")
//                .option("group.id", "testdl")
//                .option("failOnDataLoss", false)
//                .option("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
//                .option("value.deserializer", MyEncoderRaw.class.getName())
//                .load();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
                .option("subscribe", "a-dl-00")
                .load();
//        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        StreamingQuery ds = df
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        ds.awaitTermination();


//        Dataset<Row> inputDataFrame = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
//                .option("subscribe", "a-dl-00")
//                .option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//                .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//                .load()
//                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)");
////                .as(Encoders.STRING());
////
////
////        // Generate running word count
////        Dataset<Row> wordCounts = inputDataFrame.flatMap(
////                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
////                Encoders.STRING()).groupBy("value").count();
//
//        // Start running the query that prints the running counts to the console
//        StreamingQuery query = inputDataFrame.writeStream()
//                .outputMode("append")
//                .format("console")
//                .start();
//
//        query.awaitTermination();
    }

}
