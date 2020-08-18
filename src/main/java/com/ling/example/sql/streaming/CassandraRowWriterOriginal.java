package com.ling.example.sql.streaming;

import cn.sany.evi.gateway.data.bean.RealDataEntity.RealDataInfoEntity;
import cn.sany.evi.gateway.data.entity.frame.FcpType;
import cn.sany.evi.gateway.data.utils.ProtoBufConvertUtils;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;
//import com.evi.datahandle.WFConstants;
import com.witsight.platform.util.lang.StringUtil;
import scala.Tuple2;
import scala.collection.IndexedSeq;
import scala.collection.Seq;

import java.io.Serializable;

/**
 * 说明：写入设备原始工况数据到Cassandra
 * 
 * @Title: CassandraRowWriterOriginal.java
 * @Package com.evi.datahandle.service.impl.opdata2
 * @See: {@link #CassandraRowWriterOriginal}
 * @Copyright: Copyright (c) 2017 </br>
 * @Company: sany huax witsight team by product
 * @author: kangj12
 * @date: 2019年5月30日 下午7:15:00
 * @version: V1.0
 *
 */
public class CassandraRowWriterOriginal implements RowWriter<Tuple2<String, RealDataInfoEntity>> {
	/** rowWriter 序列化ID */
	private static final long serialVersionUID = -5208116069287840380L;
	// cassandra rowWriter
	private static RowWriter<Tuple2<String, RealDataInfoEntity>> writer = new CassandraRowWriterOriginal();

	/**   
	 * 说明：创建RowWriter
	 * @Title: CassandraRowWriterOriginal.java 
	 * @Package com.evi.datahandle.service.impl.opdata2 
	 * @See: {@link #CassandraRowWriterFactory}
	 * @Copyright: Copyright (c) 2017 </br>
	 * @Company: sany huax witsight team by product
	 * @author: kangj12   
	 * @date: 2019年6月3日 下午1:50:18 
	 * @version: V1.0
	 *
	 */
	public static class CassandraRowWriterFactory implements RowWriterFactory<Tuple2<String, RealDataInfoEntity>>, Serializable {
		/** CassandraRowWriter.Factory 序列化ID */
		private static final long serialVersionUID = 5432537364994240414L;

		@Override
		public RowWriter<Tuple2<String, RealDataInfoEntity>> rowWriter(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
			// 返回RowWriter
			return writer;
		}
	}

	@Override
	public Seq<String> columnNames() {
		// 返回cassandra中原始工况表中列名称
		return OpDataUtil.getColumnNamesOriginal();
	}

	@Override
	public void readColumnValues(Tuple2<String, RealDataInfoEntity> values, Object[] buffer) {
		// 设备工况数据对象
		RealDataInfoEntity original = values._2;
		buffer[0] = original.getDeviceId();
		buffer[1] = getDataVersion(original);
		buffer[2] = ProtoBufConvertUtils.timeStampTODate(original.getDataTime()).getTime();
		buffer[3] = OpDataUtil.formatUtcYYYYMMDD(ProtoBufConvertUtils.timeStampTODate(original.getDataTime()));
		buffer[4] = ProtoBufConvertUtils.byteStringToByteArray(original.getRealData());
		buffer[5] = original.getFcpType();
		buffer[6] = original.getDataGateway();
	}

	/**
	 * 说明：获取设备原始工况的协议版本号
	 * 
	 * @Title: getDataVersion
	 * @See: {@link #getDataVersion(RealDataInfoEntity)}
	 * @author kangj12
	 * @param original
	 *            设备原始工况数据
	 * @return 协议版本号
	 * @date: 2019年5月30日 下午7:46:44
	 */
	private String getDataVersion(RealDataInfoEntity original) {
		if (original.getFcpType() == FcpType.ONE.value()) {
			return StringUtil.integerToString(original.getProtocolVersion());
		}
		byte[] realDatas = ProtoBufConvertUtils.byteStringToByteArray(original.getRealData());
		if (null == realDatas || realDatas.length < 4) {
			//返回默认协议版本号
			return WFConstants.DEFAULT_PROTOCOL_VERSION;
		}
		// 返回数据协议版本号
		return StringUtil.integerToString(realDatas[3] & 0xFF);
	}
}
