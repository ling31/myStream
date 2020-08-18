package com.ling.example.sql.streaming;

import java.io.Serializable;

/**
 * 说明：[]
 * 
 * @Title: WFConstants.java
 * @Package com.evi.datahandle
 * @See: []<br/>
 *       Copyright: Copyright (c) 2017<br/>
 *       Company:sany huax witsight team by product
 * @author: fuyp
 * @date: 2017年11月9日 上午10:58:52
 * @version: V1.0
 *
 */
public class WFConstants implements Serializable {
	private static final long serialVersionUID = -3868790351038238779L;

	/**
	 * 
	 * 说明：提交模式
	 * 
	 * @Title: WFConstants.java
	 * @Package com.sany.datahandle
	 * @See: {@link SPARK_SUBMIT} Copyright: Copyright (c) 2016 Company:sany team by product
	 * @author: fuyp
	 * @date: 2017年7月18日 上午9:58:03
	 * @version: V1.0
	 *
	 */
	public static enum SPARK_SUBMIT {
		DCOS, // spark-submit／dcos提交模式
		CODE,// 代码提交模式
		;
	}

	public static final String CONFIG_KEY_SPARK_MONGODB_OUTPUT_DATABASE = "spark.mongodb.output.database";

	public static final String SIMPLE_TASKNAME = "simple";
	public static final String OP_DATA_INSPECTOR_TASKNAME = "op_data_inspector";
	public static final String OP_DATA_REALTIME_TASKNAME = "op_data_rt";
	public static final String OP_DATA_REALTIME2_TASKNAME = "op_data_rt2";
	public static final String OP_DATA_CASSANDRA_TASKNAME = "op_data_ca";
	public static final String OP_DATA_CASSANDRA2_TASKNAME = "op_data_ca2";
	public static final String OP_DATA_CASSANDRA_RECOVERY_TASKNAME = "op_data_ca_recovery";
	public static final String OP_DATA_VAS_CASSANDRA2_TASKNAME = "op_data_vas2";
	public static final String OP_DATA_MONITOR_CASSANDRA_TASKNAME = "op_data_monitor_cassandra";
	public static final String OP_DATA_MONITOR_MYSQL_TASKNAME = "op_data_monitor_mysql";
	public static final String OP_DATA_BOUNDARY_TASKNAME = "op_data_boundary";
	/**
	 * 说明：原始工况处理taskName
	 */
	public static final String OP_DATA_RAW_CASSANDRA2_TASKNAME = "op_data_raw";
	/**
	 * 说明：vas platform 原始工况处理taskName
	 */
	public static final String OP_DATA_VAS_RAW_CASSANDRA2_TASKNAME = "op_data_vas_raw";
	/**
	 * 说明：设备原始工况默认协议版本号
	 */
	public static final String DEFAULT_PROTOCOL_VERSION = "0";
	/*** 工况data_version 96***/
	public static final String DATA_VERSION_NINTY_SIX = "96";
	/*** 工况data_version 97***/
	public static final String DATA_VERSION_NINTY_SEVEN = "97";
	/*** 工况data_version 98***/
	public static final String DATA_VERSION_NINTY_EIGHT = "98";
	/*** 工况data_version 99***/
	public static final String DATA_VERSION_NINTY_NINE = "99";

}
