/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ling.kafka.consumer.deserialize;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 实现一个消费者实例代码.
 * 
 * @author smartloli.
 *
 *         Created by May 6, 2018
 */
public class JConsumerDeserialize extends Thread {

	/** 自定义序列化消费者实例入口. */
	public static void main(String[] args) {
		JConsumerDeserialize jconsumer = new JConsumerDeserialize();
		jconsumer.start();
	}

	/** 初始化Kafka集群信息. */
	private Properties configure() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "linux01:9092,linux02:9092,linux03:9092");// 指定Kafka集群地址
		props.put("group.id", "ke");// 指定消费者组
		props.put("enable.auto.commit", "true");// 开启自动提交
		props.put("auto.commit.interval.ms", "1000");// 自动提交的时间间隔
		// 反序列化消息主键
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// 反序列化消费记录
		props.put("value.deserializer", "com.ling.kafka.consumer.deserialize.JSalaryDeserializer");
		return props;
	}

	/** 实现一个单线程消费者. */
	@Override
	public void run() {
		// 创建一个消费者实例对象
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configure());
		// 订阅消费主题集合
		consumer.subscribe(Arrays.asList("a1"));
		// 实时消费标识
		boolean flag = true;
		while (flag) {
			// 获取主题消息数据
			// 如果不在props里配的话就类似工况中的情形 对比工况改写掉
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				// 循环打印消息记录
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}
		// 出现异常关闭消费者对象
		consumer.close();
	}

}
