//package com.ling.example.sql.streaming;
//
//
//import cn.sany.evi.gateway.data.serializer.DataSerializer;
//import cn.sany.evi.gateway.data.serializer.DataSerializerFactory;
//import cn.sany.evi.gateway.data.serializer.EnumSerializerType;
//import com.witsight.platform.model.BoEntity;
//import org.apache.kafka.common.serialization.Deserializer;
//import org.apache.kafka.common.serialization.Serializer;
//
//import java.util.Map;
//
//public class MyDecoder implements Deserializer<BoEntity> {
//
//
//	@Override
//	public void configure(Map<String, ?> configs, boolean isKey) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public BoEntity deserialize(String topic, byte[] data) {
//		DataSerializer<BoEntity> deserializer = DataSerializerFactory
//				.createSerializer(EnumSerializerType.KryoFiltedRealData);
//		BoEntity bo = deserializer.dataDeSerializer(data);
//		return bo;
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
