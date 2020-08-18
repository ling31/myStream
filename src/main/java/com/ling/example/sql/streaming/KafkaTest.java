/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ling.example.sql.streaming;

import cn.sany.evi.gateway.data.bean.RealDataEntity.RealDataInfoEntity;
import cn.sany.evi.gateway.data.bean.RealDataEntity;
import cn.sany.evi.gateway.data.serializer.DataSerializer;
import cn.sany.evi.gateway.data.serializer.DataSerializerFactory;
import cn.sany.evi.gateway.data.serializer.EnumSerializerType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.witsight.platform.model.BoEntity;
import com.witsight.platform.util.lang.NumberUtil;
import com.witsight.platform.util.lang.ObjectUtil;
import com.witsight.platform.util.lang.StringUtil;
import com.witsight.platform.util.tools.JsonUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple2;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaStructuredKafkaWordCount <bootstrap-servers> <subscribe-type> <topics>
 * <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
 * comma-separated list of host:port.
 * <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
 * 'subscribePattern'.
 * |- <assign> Specific TopicPartitions to consume. Json string
 * |  {"topicA":[0,1],"topicB":[2,4]}.
 * |- <subscribe> The topic list to subscribe. A comma-separated list of
 * |  topics.
 * |- <subscribePattern> The pattern used to subscribe to topic(s).
 * |  Java regex string.
 * |- Only one of "assign, "subscribe" or "subscribePattern" options can be
 * |  specified for Kafka source.
 * <topics> Different value format depends on the value of 'subscribe-type'.
 * <p>
 * Example:
 * `$ bin/run-example \
 * sql.streaming.JavaStructuredKafkaWordCount host1:port1,host2:port2 \
 * subscribe topic1,topic2`
 */
public final class KafkaTest {
    public static transient JavaSparkContext javaSparkContext = null;
    public static transient SparkContext sparkContext = null;
    public static transient SQLContext sqlContext = null;
    public static transient SparkSession spark = null;

    public static void main(String[] args) throws StreamingQueryException {

        spark = SparkSession
                .builder()
                .master("local[3]")
                .appName("test01")
                .config("spark.driver.host", "localhost")
                .config("spark.cassandra.connection.host", "10.100.0.7,10.100.0.31")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        sparkContext = spark.sparkContext();
        javaSparkContext = new JavaSparkContext(sparkContext);
        sqlContext = spark.sqlContext();


        System.out.println("begin");
//        do_wc(spark);
//        do_op_simple(spark);
//        do_op();
//        do_raw();
//                do_op_batchmapParttions();
        do_raw_batch();

    }

    public static void do_op_batchmapParttions() throws StreamingQueryException {
        Dataset<Row> ds1 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
                .option("subscribe", "a-dl-opdata")
                .load()
                .selectExpr("CAST(key AS STRING)", "value", "topic");

        StreamingQuery query = ds1
                .writeStream()
                .outputMode("append")
                .foreachBatch(
                        new VoidFunction2<Dataset<Row>, Long>() {
                            public void call(Dataset<Row> dataset, Long batchId) {
                                System.out.println("begin batch!!!");
                                dataset.cache();
                                // Transform and write batchDF
                                JavaRDD<BoEntity> jr1 = dataset.select("value").as(Encoders.BINARY()).toJavaRDD().mapPartitions(new FlatMapFunction<Iterator<byte[]>, BoEntity>() {

                                    DataSerializer<BoEntity> deserializer = DataSerializerFactory
                                            .createSerializer(EnumSerializerType.KryoFiltedRealData);
                                    // 返回结果集
                                    List<BoEntity> list = new ArrayList<>(10240);

                                    @Override
                                    public Iterator<BoEntity> call(Iterator<byte[]> iterator) throws Exception {
                                        BoEntity entity = null;
                                        Date receiveTime = null;
                                        Integer receiveDate = null;
                                        list.clear();
                                        while (iterator.hasNext()) {
                                            // 反序列化
                                            entity = deserializer.dataDeSerializer(iterator.next());
                                            // 验证数据有效性
                                            if (!OpDataUtil.validate(entity)) {
                                                continue;
                                            }

                                            receiveTime = entity.getValue(OpDataConstants.OPDATA_RECEIVE_TIME);
                                            receiveDate = NumberUtil.toInt(OpDataUtil.formatUtcYYYYMMDD(receiveTime));
                                            // 添加接收日期
                                            entity.put(OpDataConstants.OPDATA_RECEIVE_DATE, receiveDate);
                                            list.add(entity);
                                        }

                                        return list.iterator();
                                    }
                                });
                                if (jr1.isEmpty()) {
                                    return;
                                }

                                jr1.foreach(x-> System.out.println(x));

                                // 第一条数据的年份
                                int year = jr1.first().getValue(OpDataConstants.OPDATA_RECEIVE_DATE);
                                year = Float.valueOf(year * 0.0001f).intValue();

                                //读取schema信息
                                String schemaCql = "select column_name from system_schema.columns where keyspace_name='" + "evidev"
                                        + "' and table_name='" + OpDataConstants.CASSANDRA_TABLE_PREFIX + year + "';";
                                ResultSet rs = CassandraConnector.apply(sparkContext).openSession().execute(schemaCql);
                                BoEntity schema = new BoEntity();
                                for (com.datastax.oss.driver.api.core.cql.Row column : rs.all()) {
                                    schema.put(column.getString(0).toLowerCase(), null);
                                }

                                // 数据转成Json
                                JavaRDD<String> jsonRDD = jr1.map(entity -> {
                                    OpDataUtil.extras2json(entity, schema);
                                    return JsonUtil.toJsonWithType(entity);
                                });

                                // 创建Json的 Dataset
                                Dataset<String> jsonDS = sqlContext.createDataset(jsonRDD.rdd(), Encoders.STRING());
                                // 转换为Row的 Dataset
                                Dataset<Row> row2DB = sqlContext.read().json(jsonDS);
                                System.out.println("...................");
                                // 写Cassandra
                                row2DB.write().format("org.apache.spark.sql.cassandra").option("keyspace", "evidev")
                                        .option("table", OpDataConstants.CASSANDRA_TABLE_PREFIX + year + "_dl").mode(SaveMode.Append).save();

                            }
                        }
                )
                .trigger(Trigger.ProcessingTime(5000))
                .start();

        query.awaitTermination();

    }

    public static void do_op_batchmap() throws StreamingQueryException {
        Dataset<Row> ds1 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
                .option("subscribe", "a-dl-opdata")
                .load()
                .selectExpr("CAST(key AS STRING)", "value", "topic");

        StreamingQuery query = ds1
                .writeStream()
                .outputMode("append")
                .foreachBatch(
                        new VoidFunction2<Dataset<Row>, Long>() {
                            public void call(Dataset<Row> dataset, Long batchId) {
                                System.out.println("begin batch!!!");
                                dataset.cache();
                                // Transform and write batchDF
                                JavaRDD<BoEntity> jr = dataset.select("value").as(Encoders.BINARY()).toJavaRDD().map(new Function<byte[], BoEntity>() {
                                    DataSerializer<BoEntity> deserializer = DataSerializerFactory
                                            .createSerializer(EnumSerializerType.KryoFiltedRealData);

                                    @Override
                                    public BoEntity call(byte[] row) throws Exception {
                                        BoEntity entity = deserializer.dataDeSerializer(row);
                                        Date receiveTime = entity.getValue(OpDataConstants.OPDATA_RECEIVE_TIME);
                                        Integer receiveDate = NumberUtil.toInt(OpDataUtil.formatUtcYYYYMMDD(receiveTime));
                                        // 添加接收日期
                                        entity.put(OpDataConstants.OPDATA_RECEIVE_DATE, receiveDate);
                                        return entity;
                                    }
                                });

                                if (jr.isEmpty()) {
                                    return;
                                }
                                jr.foreach(x -> System.out.println(x));

                                // 第一条数据的年份
                                int year = jr.first().getValue(OpDataConstants.OPDATA_RECEIVE_DATE);
                                year = Float.valueOf(year * 0.0001f).intValue();

                                //读取schema信息
                                String schemaCql = "select column_name from system_schema.columns where keyspace_name='" + "evidev"
                                        + "' and table_name='" + OpDataConstants.CASSANDRA_TABLE_PREFIX + year + "';";
                                ResultSet rs = CassandraConnector.apply(sparkContext).openSession().execute(schemaCql);
                                BoEntity schema = new BoEntity();
                                for (com.datastax.oss.driver.api.core.cql.Row column : rs.all()) {
                                    schema.put(column.getString(0).toLowerCase(), null);
                                }

                                // 数据转成Json
                                JavaRDD<String> jsonRDD = jr.map(entity -> {
                                    OpDataUtil.extras2json(entity, schema);
                                    return JsonUtil.toJsonWithType(entity);
                                });

                                // 创建Json的 Dataset
                                Dataset<String> jsonDS = sqlContext.createDataset(jsonRDD.rdd(), Encoders.STRING());
                                // 转换为Row的 Dataset
                                Dataset<Row> row2DB = sqlContext.read().json(jsonDS);
                                System.out.println("...................");
                                // 写Cassandra
                                row2DB.write().format("org.apache.spark.sql.cassandra").option("keyspace", "evidev")
                                        .option("table", OpDataConstants.CASSANDRA_TABLE_PREFIX + year + "_dl").mode(SaveMode.Append).save();

                            }
                        }
                )
                .trigger(Trigger.ProcessingTime(5000))
                .start();

        query.awaitTermination();

    }

    public static void do_raw_batch() throws StreamingQueryException {
        Dataset<Row> ds1 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
                .option("subscribe", "a-dl-raw")
                .load()
                .selectExpr("CAST(key AS STRING)", "value", "topic");

        StreamingQuery query = ds1
                .writeStream()
                .outputMode("append")
                .foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    // Tuple2类型的设备原始工况list
                    List<Tuple2<String, RealDataInfoEntity>> list = new ArrayList<>(10240);
                    // 设备工况数据protobuf序列化器
                    DataSerializer<RealDataInfoEntity> deserializer = DataSerializerFactory
                            .createSerializer(EnumSerializerType.ProtobufRealData);

                    @Override
                    public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                        System.out.println("begin batch!!!");
                        rowDataset.cache();
                        // Transform and write batchDF
                        JavaPairRDD<String, RealDataInfoEntity> jr = rowDataset.select("value").as(Encoders.BINARY()).toJavaRDD().mapPartitionsToPair(
                                new PairFlatMapFunction<Iterator<byte[]>, String, RealDataInfoEntity>() {
                                    @Override
                                    public Iterator<Tuple2<String, RealDataInfoEntity>> call(
                                            Iterator<byte[]> iterator) throws Exception {

                                        // 清空
                                        list.clear();
                                        // 工况数据对象
                                        RealDataInfoEntity entity = null;
                                        while (iterator.hasNext()) {
                                            entity = deserializer.dataDeSerializer(iterator.next());
                                            // 判断设备loginId,数据接收时间是否为空
//                                            if (null == entity || StringUtil.isEmpty(entity.getDeviceId())
//                                                    || ObjectUtil.isEmpty(entity.getDataTime())) {
//                                                if (logger.isWarnEnabled()) {
//                                                    logger.warn(
//                                                            "pull realData from kafka is null or deviceId is null or dataTime is null ");
//                                                }
//                                                continue;
//                                            }
                                            // 添加Tuple2类型的设备原始工况到list
                                            list.add(new Tuple2<String, RealDataInfoEntity>(entity.getDeviceId(), entity));
                                        }
                                        // 返回Tuple2类型的设备原始工况list
                                        return list.iterator();
                                    }
                                }
                        );
                        jr.foreach(x-> System.out.println(x));
                        javaFunctions(jr).writerBuilder("evidev", "exca_opdata_original_dl",
                                new CassandraRowWriterOriginal.CassandraRowWriterFactory()).saveToCassandra();
                    }
                })
                /*.foreach(new ForeachWriter<Row>() {
                    // Tuple2类型的设备原始工况list
                    List<Tuple2<String, RealDataInfoEntity>> list = new ArrayList<>(10240);
                    // 设备工况数据protobuf序列化器
                    DataSerializer<RealDataInfoEntity> deserializer = DataSerializerFactory
                            .createSerializer(EnumSerializerType.ProtobufRealData);

                    @Override
                    public boolean open(long partitionId, long epochId) {
                        System.out.println("open....");

                        return true;
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        System.out.println("size:" + list.size());
                        JavaPairRDD rdd = javaSparkContext.parallelizePairs(list);
                        javaFunctions(rdd).writerBuilder("evidev", "exca_opdata_original_dl",
                                new CassandraRowWriterOriginal.CassandraRowWriterFactory()).saveToCassandra();
                        System.out.println("close...");
                    }

                    @Override
                    public void process(Row value) {
                        System.out.println("key:" + value.getString(0));
//                        System.out.println(value.getString(1));
                        System.out.println("topic:" + value.getString(2));
                        RealDataInfoEntity entity = deserializer.dataDeSerializer(value.getAs(1));
                        list.add(new Tuple2<String, RealDataInfoEntity>(entity.getDeviceId(), entity));

                    }
                })*/
                .trigger(Trigger.ProcessingTime(5000))
                .start();

        query.awaitTermination();

    }

    public static void do_raw() throws StreamingQueryException {
        Dataset<Row> ds1 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
                .option("subscribe", "a-dl-raw")
                .load()
                .selectExpr("CAST(key AS STRING)", "value", "topic");

        StreamingQuery query = ds1
                .writeStream()
                .outputMode("append")
                .foreach(new ForeachWriter<Row>() {
                    // Tuple2类型的设备原始工况list
                    List<Tuple2<String, RealDataInfoEntity>> list = new ArrayList<>(10240);
                    // 设备工况数据protobuf序列化器
                    DataSerializer<RealDataInfoEntity> deserializer = DataSerializerFactory
                            .createSerializer(EnumSerializerType.ProtobufRealData);

                    @Override
                    public boolean open(long partitionId, long epochId) {
                        System.out.println("open....");

                        return true;
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        System.out.println("size:" + list.size());
                        JavaPairRDD rdd = javaSparkContext.parallelizePairs(list);
                        javaFunctions(rdd).writerBuilder("evidev", "exca_opdata_original_dl",
                                new CassandraRowWriterOriginal.CassandraRowWriterFactory()).saveToCassandra();
                        System.out.println("close...");
                    }

                    @Override
                    public void process(Row value) {
                        System.out.println("key:" + value.getString(0));
//                        System.out.println(value.getString(1));
                        System.out.println("topic:" + value.getString(2));
                        RealDataInfoEntity entity = deserializer.dataDeSerializer(value.getAs(1));
                        list.add(new Tuple2<String, RealDataInfoEntity>(entity.getDeviceId(), entity));

                    }
                })
                .trigger(Trigger.ProcessingTime(5000))
                .start();

        query.awaitTermination();

    }


    public static void do_op_simple(SparkSession spark) throws StreamingQueryException {
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
                .option("subscribe", "a-dl-opdata")
                .load()
                .selectExpr("CAST(key AS STRING)", "value", "topic");

        StreamingQuery query = lines
                .writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();

    }

    public static void do_wc(SparkSession spark) throws StreamingQueryException {
        Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.100.0.235:1025")
                .option("subscribe", "a-dl-00").load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());
//
//        // Generate running word count
//        Dataset<String> wordCounts = lines
//                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
//                        Encoders.STRING());

        // Start running the query that prints the running counts to the console
        StreamingQuery query = lines
                .writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();

    }
}