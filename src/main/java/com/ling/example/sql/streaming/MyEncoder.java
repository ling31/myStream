//package com.ling.example.sql.streaming;
//
//
//import cn.sany.evi.gateway.data.serializer.DataSerializer;
//import cn.sany.evi.gateway.data.serializer.DataSerializerFactory;
//import cn.sany.evi.gateway.data.serializer.EnumSerializerType;
//import com.witsight.platform.model.BoEntity;
//import org.apache.kafka.common.serialization.Serializer;
//
//import java.util.Map;
//
//public class MyEncoder implements Serializer<BoEntity> {
//
//
//	@Override
//	public void configure(Map<String, ?> configs, boolean isKey) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public byte[] serialize(String topic, BoEntity data) {
//		DataSerializer<BoEntity> serializer = DataSerializerFactory.createSerializer(EnumSerializerType.KryoFiltedRealData);
//		byte[] result = serializer.dataSerializer(data);
//		return result;
//	}
//
//	@Override
//	public void close() {
//		// TODO Auto-generated method stub
//
//	}
//}
//
//
