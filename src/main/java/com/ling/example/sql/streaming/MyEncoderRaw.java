//package com.ling.example.sql.streaming;
//
//
//import cn.sany.evi.gateway.data.bean.RealDataEntity.RealDataInfoEntity;
//import cn.sany.evi.gateway.data.serializer.DataSerializer;
//import cn.sany.evi.gateway.data.serializer.DataSerializerFactory;
//import cn.sany.evi.gateway.data.serializer.EnumSerializerType;
//import org.apache.kafka.common.serialization.Serializer;
//
//import java.util.Map;
//
//public class MyEncoderRaw implements Serializer<RealDataInfoEntity> {
//
//
//	@Override
//	public void configure(Map<String, ?> configs, boolean isKey) {
//		// TODO Auto-generated method stub
//
//	}
//
//	@Override
//	public byte[] serialize(String topic, RealDataInfoEntity data) {
//		DataSerializer<RealDataInfoEntity> serializer = DataSerializerFactory.createSerializer(EnumSerializerType.ProtobufRealData);
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
