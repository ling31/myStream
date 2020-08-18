package com.ling.example.sql.streaming;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.witsight.platform.model.BoEntity;
import com.witsight.platform.util.lang.StringUtil;
import com.witsight.platform.util.tools.JsonUtil;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * 说明：工况数据工具类
 * 
 * @Title: OpDataUtil.java
 * @Package com.evi.datahandle.service.impl.opdata2
 * @Copyright: Copyright (c) 2017</br>
 * @Company: sany huax witsight team by product
 * @author: tangj42
 * @date: 2019年4月28日 上午11:48:03
 * @version: V1.0
 *
 */
public class OpDataUtil {
	/** 系统默认时区偏移量 */
	public static final int TIME_ZONE_DEFAULT_RAW_OFFSET = TimeZone.getDefault().getRawOffset();
	/** 系统默认时区 */
	public static final float TIME_ZONE_DEFAULT = OpDataUtil.TIME_ZONE_DEFAULT_RAW_OFFSET * 0.001f / 60 / 60;

	/** 本地日期格式化器,UTC,格式 HH:mm */
	private static ThreadLocal<SimpleDateFormat> localHHMM = new ThreadLocal<SimpleDateFormat>();
	/** 本地日期格式化器,UTC,格式 yyyyMMdd */
	private static ThreadLocal<SimpleDateFormat> localUtcYYYYMMDD = new ThreadLocal<SimpleDateFormat>();
	/** 本地日期格式化器,UTC,格式 yyyy-MM-dd HH:mm:ss */
	private static ThreadLocal<SimpleDateFormat> localUtcYYYYMMDDHHMMSS = new ThreadLocal<SimpleDateFormat>();
	/** 原始工况字段名称列表 */
	private static Seq<String> cassandraTableColumnNamesOriginal = genColumnNamesOriginal();
	private static ThreadLocal<StringWriter> localJsonWriter = new ThreadLocal<StringWriter>();
	private static ThreadLocal<JsonGenerator> localJsonGenerator = new ThreadLocal<JsonGenerator>();

	private static StringWriter getStringWriter() {
		StringWriter writer = localJsonWriter.get();
		if (writer == null) {
			writer = new StringWriter(4096);
			localJsonWriter.set(writer);
		}
		return writer;
	}

	private static JsonGenerator getJsonGenerator() {
		JsonGenerator generator = localJsonGenerator.get();
		if (generator == null) {
			try {
				generator = new JsonFactory().createGenerator(getStringWriter());
			} catch (IOException e) {
			}
			localJsonGenerator.set(generator);
		}
		return generator;
	}

	/**
	 * 
	 * 说明：工况数据转Json
	 * 
	 * @Title: opdata2Json
	 * @author tangj42
	 * @param opdata
	 *            工况数据
	 * @return Json
	 * @date: 2019年5月20日 下午3:08:21
	 */
	public static String opdata2Json(BoEntity opdata) {
		String json = "{}";
		if (null == opdata) {
			return json;
		}

		StringWriter writer = getStringWriter();
		JsonGenerator generator = getJsonGenerator();
		writer.getBuffer().setLength(0);

		try {
			generator.writeStartObject();
			for (Entry<String, Object> column : opdata.entrySet()) {
				generator.writeObjectField(column.getKey(), column.getValue());
			}
			generator.writeEndObject();
			generator.flush();
			json = writer.toString();
		} catch (IOException e) {
		}

		return json;
	}

	/**
	 * 
	 * 说明：格式化时间,格式:HHMM
	 * 
	 * @Title: formatHHMM
	 * @author tangj42
	 * @param date
	 * @return HHMM
	 * @date: 2019年5月8日 下午3:27:25
	 */
	public static String formatHHMM(Date date) {
		SimpleDateFormat sdf = localHHMM.get();
		if (sdf == null) {
			sdf = new SimpleDateFormat("HH:mm");
			localHHMM.set(sdf);
		}
		return sdf.format(date);
	}

	/**
	 * 
	 * 说明：格式化日期 - UTC,格式:YYYYMMDD
	 * 
	 * @Title: formatUtcYYYYMMDD
	 * @author tangj42
	 * @param date
	 *            日期
	 * @return YYYYMMDD
	 * @date: 2019年5月8日 下午3:20:00
	 */
	public static String formatUtcYYYYMMDD(Date date) {
		if (null == date) {
			return null;
		}
		SimpleDateFormat sdf = localUtcYYYYMMDD.get();
		if (sdf == null) {
			sdf = new SimpleDateFormat("yyyyMMdd");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			localUtcYYYYMMDD.set(sdf);
		}
		return sdf.format(date);
	}

	/**
	 * 
	 * 说明：格式化日期 - UTC,格式:YYYYMMDDHHMMSS
	 * 
	 * @Title: formatUtcYYYYMMDDHHMMSS
	 * @author tangj42
	 * @param date
	 *            日期
	 * @return YYYYMMDDHHMMSS
	 * @date: 2019年5月8日 下午3:21:20
	 */
	public static String formatUtcYYYYMMDDHHMMSS(Date date) {
		if (null == date) {
			return null;
		}
		SimpleDateFormat sdf = localUtcYYYYMMDDHHMMSS.get();
		if (sdf == null) {
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			localUtcYYYYMMDDHHMMSS.set(sdf);
		}
		return sdf.format(date);
	}

	/**
	 * 
	 * 说明：判断是否有效的工况数据
	 * 
	 * @Title: validate
	 * @author tangj42
	 * @param data
	 *            工况数据
	 * @return true: 有效<br>
	 *         false 无效
	 * @date: 2019年4月28日 上午11:49:15
	 */
	public static boolean validate(BoEntity data) {
		if (null == data || !OpDataUtil.validateColumn(data, OpDataConstants.OPDATA_LOGIN_ID, String.class)
				|| !OpDataUtil.validateColumn(data, OpDataConstants.OPDATA_DATA_VERSION, String.class)
				|| !OpDataUtil.validateColumn(data, OpDataConstants.OPDATA_FCP, String.class)
				|| !OpDataUtil.validateColumn(data, OpDataConstants.OPDATA_RECEIVE_TIME, Date.class)) {
			return false;
		}

		Object temp = null;
		temp = data.get(OpDataConstants.OPDATA_LOGIN_ID);
		if (StringUtil.isEmpty((String) temp)) {
			return false;
		}

		temp = data.get(OpDataConstants.OPDATA_DATA_VERSION);
		if (StringUtil.isEmpty((String) temp) || ((String) temp).equals("14")) {
			return false;
		}

		temp = data.get(OpDataConstants.OPDATA_FCP);
		if (StringUtil.isEmpty((String) temp)) {
			return false;
		}

		return true;
	}

	/**
	 * 
	 * 说明：纠正Float数据NaN和Infinite的情况, 因为无法写入MySql
	 * 
	 * @Title: correctFloatColumn
	 * @author tangj42
	 * @param data
	 *            工况数据
	 * @date: 2018年6月1日 上午10:07:11
	 */
	public static void correctFloatColumn(BoEntity data) {
		if (null == data) {
			return;
		}

		Object valueObject;
		for (Entry<String, Object> entry : data.entrySet()) {
			valueObject = entry.getValue();
			if (!(valueObject instanceof Float)) {
				continue;
			}
			entry.setValue(correctFloat((Float) valueObject));
		}
	}

	/**
	 * 
	 * 说明：纠正Float值
	 * 
	 * @Title: correctFloat
	 * @author tangj42
	 * @param value
	 *            原float
	 * @return 纠正后float
	 * @date: 2019年5月9日 上午11:08:38
	 */
	public static float correctFloat(float value) {
		if (Float.isNaN(value)) {
			return OpDataConstants.FLOAT_VALUE_NAN_ALT;
		} else if (Float.isInfinite((value))) {
			return OpDataConstants.FLOAT_VALUE_INFINITE_ALT;
		} else {
			return value;
		}
	}

	/**
	 * 说明：生成Cassandra中原始工况数据表的列名
	 * 
	 * @Title: genColumnNamesOriginal
	 * @See: {@link #genColumnNamesOriginal()}
	 * @author kangj12
	 * @return 原始工况数据列名
	 * @date: 2019年5月30日 下午7:42:40
	 */
	private static Seq<String> genColumnNamesOriginal() {
		List<String> names = new ArrayList<>();
		names.add("login_id");
		names.add("data_version");
		names.add("receive_time");
		names.add("receive_date");
		names.add("data");
		names.add("fcp");
		names.add("data_gateway");
//		return scala.collection.JavaConversions.asScalaBuffer(names);
		return JavaConverters.asScalaIteratorConverter(names.iterator()).asScala().toSeq();
	}


	/**
	 * 说明：获取Cassandra中原始工况数据表的列名
	 * 
	 * @Title: getColumnNamesOriginal
	 * @See: {@link #getColumnNamesOriginal()}
	 * @author kangj12
	 * @return 原始工况数据表的列名
	 * @date: 2019年5月30日 下午7:42:29
	 */
	public static Seq<String> getColumnNamesOriginal() {
		return cassandraTableColumnNamesOriginal;
	}

	/**
	 * 
	 * 说明：将schema中不存在的字段以json包装成一个独立的字段集字段
	 * 
	 * @Title: extras2json
	 * @author tangj42
	 * @param data
	 *            工况数据
	 * @param schema
	 *            schema字段信息
	 * @date: 2019年6月25日 下午2:34:52
	 */
	public static void extras2json(BoEntity data, BoEntity schema) {
		if (null == data || null == schema || !data.containsKey(OpDataConstants.OPDATA_DATA_VERSION)) {
			return;
		}

		int version = Integer.parseInt(data.getValue(OpDataConstants.OPDATA_DATA_VERSION));
		if (version > 128 || (version > 63 && version < 128)) {// 添加协议19时，修改为非挖机协议才做此处理
			// 将数据库中不存在的字段用json包装
			BoEntity extraColumns = new BoEntity();
			for (Entry<String, Object> entry : data.entrySet()) {
				if (!schema.containsKey(entry.getKey())) {
					extraColumns.put(entry.getKey(), entry.getValue());
				}
			}

			if (!extraColumns.isEmpty()) {
				for (String key : extraColumns.keySet()) {
					data.remove(key);
				}

				data.put(OpDataConstants.OPDATA_EXTRAS, JsonUtil.toJsonWithType(extraColumns));
			}
		}
	}

	/**
	 * 
	 * 说明：按指定时区转换时间
	 * 
	 * @Title: getDateByTimeZone
	 * @author tangj42
	 * @param date
	 *            时间，系统默认时区
	 * @param timeZone
	 *            时区，-12.0～12.0
	 * @return 转换后的时间，系统默认时区
	 * @date: 2019年12月13日 下午3:44:53
	 */
	public static Date getDateByTimeZone(Date date, Float timeZone) {
		if (null == date) {
			date = new Date();
		}

		if (null == timeZone) {
			timeZone = TIME_ZONE_DEFAULT;
		}

		long time = date.getTime() - TIME_ZONE_DEFAULT_RAW_OFFSET + (int) (timeZone * 60 * 60 * 1000);

		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		return cal.getTime();
	}

	/**
	 * 
	 * 说明：验证工况数据中字段的有效性
	 * 
	 * @Title: validateColumn
	 * @author tangj42
	 * @param data
	 *            工况数据
	 * @param name
	 *            字段名
	 * @param t
	 *            字段类型
	 * @return true/false
	 * @date: 2019年12月16日 下午1:11:24
	 */
	public static <T> boolean validateColumn(BoEntity data, String name, Class<T> t) {
		if (null == data || StringUtil.isEmpty(name) || null == t) {
			return false;
		}

		Object value = data.get(name);
		if (value != null && value.getClass().equals(t)) {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * 说明：获取工况中字段的值
	 * 
	 * @Title: getColumnValue
	 * @author tangj42
	 * @param data
	 *            工况数据
	 * @param name
	 *            字段名
	 * @param t
	 *            字段类新
	 * @return 字段值，或验证失败返回null
	 * @date: 2019年12月16日 下午1:12:07
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getColumnValue(BoEntity data, String name, Class<T> t) {
		if (validateColumn(data, name, t)) {
			return (T) data.get(name);
		}

		return null;
	}

}
