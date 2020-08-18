///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.ling.example.sql.streaming;
//
//import cn.sany.evi.gateway.data.bean.RealDataEntity;
//import cn.sany.evi.gateway.data.serializer.DataSerializer;
//import cn.sany.evi.gateway.data.serializer.DataSerializerFactory;
//import cn.sany.evi.gateway.data.serializer.EnumSerializerType;
//import com.witsight.platform.model.BoEntity;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.MapFunction;
//import org.apache.spark.api.java.function.VoidFunction2;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.streaming.StreamingQuery;
//import org.apache.spark.sql.streaming.Trigger;
//
//import cn.sany.evi.gateway.data.bean.RealDataEntity.RealDataInfoEntity;
//import cn.sany.evi.gateway.data.serializer.DataSerializer;
//import cn.sany.evi.gateway.data.serializer.DataSerializerFactory;
//import cn.sany.evi.gateway.data.serializer.EnumSerializerType;
//
//import java.util.Arrays;
//
///**
// * Consumes messages from one or more topics in Kafka and does wordcount.
// * Usage: JavaStructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
// * <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
// * comma-separated list of host:port.
// * <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
// * 'subscribePattern'.
// * |- <assign> Specific TopicPartitions to consume. Json string
// * |  {"topicA":[0,1],"topicB":[2,4]}.
// * |- <subscribe> The topic list to subscribe. A comma-separated list of
// * |  topics.
// * |- <subscribePattern> The pattern used to subscribe to topic(s).
// * |  Java regex string.
// * |- Only one of "assign, "subscribe" or "subscribePattern" options can be
// * |  specified for Kafka source.
// * <topics> Different value format depends on the value of 'subscribe-type'.
// * <p>
// * Example:
// * `$ bin/run-example \
// * sql.streaming.JavaStructuredKafkaWordCount host1:port1,host2:port2 \
// * subscribe topic1,topic2`
// */
//public final class JavaStructuredKafkaMyOpdata {
//
//    public static void main(String[] args) throws Exception {
//
//        String bootstrapServers = "10.100.0.235:1025";
//        String topics = "a-dl-opdata";
//
//        SparkSession spark = SparkSession
//                .builder()
//                .master("local[3]")
//                .appName("test01")
//                .config("spark.driver.host", "localhost")
//                .getOrCreate();
//
//        spark.sparkContext().setLogLevel("WARN");
//
//
//        Dataset<Row> lines0 = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", bootstrapServers)
//                .option("subscribe", topics)
//                .load();
//
//
//
//        lines0.printSchema();
//
//        Dataset<byte[]> bytesRow = lines0.select("value")
//                .as(Encoders.BINARY());
//
//
//        Dataset<BoEntity> BODS = bytesRow.map(new MapFunction<byte[], BoEntity>() {
//            private static final long serialVersionUID = 4130748013775138643L;
//            @Override
//            public BoEntity call(byte[] value) throws Exception {
//                // 反序列化器
//                DataSerializer<BoEntity> deserializer = DataSerializerFactory
//                        .createSerializer(EnumSerializerType.KryoFiltedRealData);
//                BoEntity entity = deserializer.dataDeSerializer(value);
//                System.out.println("==================");
////                System.out.println(entity.getValue("receive_time"));
//                return  entity;
//            }
//        }, Encoders.kryo(BoEntity.class));
//
//        StreamingQuery query = BODS.writeStream()
//                .outputMode("update")
//                .format("console")
//                .option("checkpointLocation", "./test302")
//                .option("failOnDataLoss", false)
//                .start();
//
////        StreamingQuery result = lines0.writeStream().foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset, aLong) -> {
////            System.out.println("batchID => " + aLong);
////            Dataset<Row> rows = rowDataset.select("value");
////        }).trigger(Trigger.ProcessingTime("5 seconds")).start();
////
////        result.awaitTermination();
//    }
//}
