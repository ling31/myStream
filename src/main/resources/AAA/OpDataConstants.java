package com.evi.datahandle.service.impl.opdata2;

/**
 * 说明：工况数据常量类
 * 
 * @Title: Constants.java
 * @Package com.evi.datahandle.service.impl.opdata2
 * @Copyright: Copyright (c) 2017</br>
 * @Company: sany huax witsight team by product
 * @author: tangj42
 * @date: 2019年4月25日 下午3:20:14
 * @version: V1.0
 *
 */
public class OpDataConstants {
	/** 字段名：登录号 */
	public static final String OPDATA_LOGIN_ID = "login_id";
	/** 字段名：机号 */
	public static final String OPDATA_SERIAL_NO = "serial_no";
	/** 字段名：协议版本号 */
	public static final String OPDATA_DATA_VERSION = "data_version";
	/** 字段名：FCP版本号 */
	public static final String OPDATA_FCP = "fcp";
	/** 字段名：接收时间 */
	public static final String OPDATA_RECEIVE_TIME = "receive_time";
	/** 字段名：接收时间转换为当地时间 */
	public static final String OPDATA_RECEIVE_TIME_LOCAL = "receive_time_local";
	/** 字段名：接收日期 */
	public static final String OPDATA_RECEIVE_DATE = "receive_date";
	/** 字段名：时区 */
	public static final String OPDATA_TIME_ZONE = "time_zone";
	/** 字段名：设备时间 */
	public static final String OPDATA_MACHINE_TIMESTAMP = "machine_timestamp";
	/** 字段名：经度 */
	public static final String OPDATA_LONGITUDE = "longitude";
	/** 字段名：纬度 */
	public static final String OPDATA_LATITUDE = "latitude";
	/** 字段名：实时经度 */
	public static final String OPDATA_LONGITUDE_REALTIME = "longitude_realtime";
	/** 字段名：实时纬度 */
	public static final String OPDATA_LATITUDE_REALTIME = "latitude_realtime";
	/** 字段名：有效经纬度接收时间 */
	public static final String OPDATA_RECEIVE_TIME_VALID_LAL = "receive_time_valid_lal";
	/** 字段名：有效油位接收时间 */
	public static final String OPDATA_RECEIVE_TIME_VALID_FUEL_LEVEL = "receive_time_valid_fuellevel";
	/** 字段名：总工时 */
	public static final String OPDATA_TOTAL_WORKTIME = "total_worktime";
	/** 字段名：总怠速时间 */
	public static final String OPDATA_TOTAL_IDLE_TIME = "total_idle_time";
	/** 字段名：总油耗 */
	public static final String OPDATA_TOTAL_FUEL_CONSUMPTION = "total_fuel_consumption";
	/** 字段名：总怠速油耗 */
	public static final String OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION = "total_idle_fuel_consumption";
	/** 字段名：发动机转速 */
	public static final String OPDATA_ENGINE_SPEED = "engine_speed";
	/** 字段名：锁机级别 */
	public static final String OPDATA_LOCK_LEVEL = "lock_level";
	/** 字段名：锁机剩余时间 */
	public static final String OPDATA_LOCK_TIME_LEFT = "lock_time_left";
	/** 字段名：油位 */
	public static final String OPDATA_FUEL_LEVEL = "fuel_level";
	/** 字段名：额外字段 */
	public static final String OPDATA_EXTRAS = "extras";
	/** 字段名：液压油温 */
	public static final String OPDATA_HYDRAULIC_OIL_TEMPERATURE = "hydraulic_oil_temperature";
	/** 字段名：水温 */
	public static final String OPDATA_COOLING_WATER_TEMPERATURE = "cooling_water_temperature";
	/** 字段名：协议245的真实receive_time */
	public static final String OPDATA_TAKE_TIME = "take_time";

	/** 字段名：日总工时 */
	public static final String MYSQL_DAY_WORKTIME = "dayworktime";
	/** 字段名：日总怠速时间 */
	public static final String MYSQL_IDLE_TIME = "idletime";
	/** 字段名：日油耗 */
	public static final String MYSQL_FUEL_CONSUME = "fuelconsume";
	/** 字段名：日怠速油耗 */
	public static final String MYSQL_IDLE_FUEL_CONSUME = "idlefuelconsume";
	/** 字段名：日总工时基准 */
	public static final String MYSQL_DAY_WORKTIME_BASE = "totalworktimebase";
	/** 字段名：日总怠速时间基准 */
	public static final String MYSQL_IDLE_TIME_BASE = "totalidletimebase";
	/** 字段名：日油耗基准 */
	public static final String MYSQL_FUEL_CONSUME_BASE = "totalfuelconsumebase";
	/** 字段名：日怠速油耗基准 */
	public static final String MYSQL_IDLE_FUEL_CONSUME_BASE = "totalidlefuelconsumebase";
	/** 字段名：日工作时间段合计总工时 */
	public static final String MYSQL_WORK_TIMELINE_TOTAL = "worktimelinetotal";
	/** 字段名：是否电改液控 */
	public static final String MYSQL_IS_LOCK_FUTILE = "islockfutile";
	/** 字段名：上午总工时 */
	public static final String MYSQL_AM_WORKTIME = "amworktime";
	/** 字段名：总工时上跳计数 */
	public static final String MYSQL_TOTAL_WORKTIME_JUMP_UP = "totalworktimejumpup";
	/** 字段名：总工时下跳计数 */
	public static final String MYSQL_TOTAL_WORKTIME_JUMP_DOWN = "totalworktimejumpdown";
	/** 字段名：实时总工时 */
	public static final String MYSQL_TOTAL_WORKTIME_REALTIME = "total_worktime_realtime";
	/** 字段名：实时总怠速时间 */
	public static final String MYSQL_TOTAL_IDLE_TIME_REALTIME = "total_idle_time_realtime";
	/** 字段名：实时总油耗 */
	public static final String MYSQL_TOTAL_FUEL_CONSUMPTION_REALTIME = "total_fuel_consumption_realtime";
	/** 字段名：实时总怠速油耗 */
	public static final String MYSQL_TOTAL_IDLE_FUEL_CONSUMPTION_REALTIME = "total_idle_fuel_consumption_realtime";
	/** 字段名： 每日开始油位 */
	public static final String MYSQL_DAY_FUEL_LEVEL_START = "dayfuellevelstart";
	/** 字段名： 每日结束油位 */
	public static final String MYSQL_DAY_FUEL_LEVEL_END = "dayfuellevelend";
	/** 字段名： 每日加油量 */
	public static final String MYSQL_DAY_FUEL_CHARGING = "dayfuelcharging";
	/** 字段名： 开机时间 */
	public static final String MYSQL_FUEL_CHARGING_START_UP_TIME = "startuptime";
	/** 字段名： 关机时间油位 */
	public static final String MYSQL_FUEL_CHARGING_SHUT_DOWN_FUEL_LEVEL = "shutdownfuellevel";
	/** 字段名： 每日最高液压油温 */
	public static final String MYSQL_MAX_HYDRAULIC_OIL_TEMPERATURE = "max_hydraulic_oil_temperature";
	/** 字段名： 每日最高水温 */
	public static final String MYSQL_MAX_COOLING_WATER_TEMPERATURE = "max_cooling_water_temperature";
	/** 字段名：ID */
	public static final String MYSQL_ID = "id";
	/** 字段名：VERSION */
	public static final String MYSQL_VERSION = "version";
	/** 字段名：创建时间 */
	public static final String MYSQL_CREATE_DATE = "create_date";
	/** 字段名：最后更新时间 */
	public static final String MYSQL_LAST_MODIFY = "last_modify";
	/** 字段名：总工时-油位>0 */
	public static final String MYSQL_TOTAL_WORKTIME_VALID_FUEL_LEVEL = "total_worktime_valid_fuel_level";
	/** 字段名：接收时间-总工时>0 */
	public static final String MYSQL_RECEIVE_TIME_VALID_TOTAL_WORKTIME = "receive_time_valid_total_worktime";
	/** 字段名：接收时间-总油耗>0 */
	public static final String MYSQL_RECEIVE_TIME_VALID_TOTAL_FUEL_CONSUME = "receive_time_valid_total_fuel_consume";
	/** 字段名：总工时-总油耗>0 */
	public static final String MYSQL_TOTAL_WORKTIME_VALID_TOTAL_FUEL_CONSUME = "total_worktime_valid_total_fuel_consume";
	/** 字段名：接收时间-总怠速时间>0 */
	public static final String MYSQL_RECEIVE_TIME_VALID_TOTAL_IDLE_TIME = "receive_time_valid_total_idle_time";
	/** 字段名：接收时间-总怠速油耗>0 */
	public static final String MYSQL_RECEIVE_TIME_VALID_TOTAL_IDLE_FUEL_CONSUME = "receive_time_valid_total_idle_fuel_consumption";

	/** 字段名：设备登录号 */
	public static final String MONGO_LOGIN_ID = "LoginID";
	/** 字段名：设备提供商编码 */
	public static final String MONGO_UPDATE_TIME = "UpdateTime";
	/** 字段名：创建时间 */
	public static final String MONGO_CREATE_TIME = "CreateTime";
	/** 字段名：报表时间 */
	public static final String MONGO_REPORT_TIME = "ReportTime";
	/** 字段名：报表时间 */
	public static final String MONGO_TIME_ZONE = "TimeZone";
	/** 字段名：经度 */
	public static final String MONGO_LONGITUDE = "Longitude";
	/** 字段名：纬度 */
	public static final String MONGO_LATITUDE = "Latitude";
	/** 字段名：协议版本号 */
	public static final String MONGO_DATA_VERSION = "DataVersion";
	/** 字段名：设备提供商编码 */
	public static final String MONGO_MACHINE_PROVIDER_CODE = "MachineProvider_Code";
	/** 字段名：总工时 */
	public static final String MONGO_TOTAL_WORKTIME = "TotalWorkTime";
	/** 字段名：日总工时 */
	public static final String MONGO_WORKTIME = "WorkTime";
	/** 字段名：总怠速时间 */
	public static final String MONGO_TOTAL_IDLE_TIME = "TotalIdleTime";
	/** 字段名：日总怠速时间 */
	public static final String MONGO_IDLETIME = "IdleTime";
	/** 字段名：总油耗 */
	public static final String MONGO_TOTAL_FUEL_CONSUME = "TotalFC";
	/** 字段名：日总油耗 */
	public static final String MONGO_FUEL_COMSUME = "FuelConsume";
	/** 字段名：总怠速油耗 */
	public static final String MONGO_TOTAL_IDLE_FUEL_CONSUME = "TotalIdleFC";
	/** 字段名：日总怠速油耗 */
	public static final String MONGO_IDLE_FUEL_CONSUME = "IdleFuelConsume";
	/** 字段名：日总工时基准 */
	public static final String MONGO_WORKTIME_BASE = "TotalWorkTimeBase";
	/** 字段名：日总怠速时间基准 */
	public static final String MONGO_IDLETIME_BASE = "TotalIdleTimeBase";
	/** 字段名：日总油耗基准 */
	public static final String MONGO_FUEL_CONSUME_BASE = "TotalFuelConsumeBase";
	/** 字段名：日总怠速油耗基准 */
	public static final String MONGO_IDLE_FUEL_CONSUME_BASE = "TotalIdleFuelConsumeBase";
	/** 字段名：是否工作 */
	public static final String MONGO_IS_WORK = "IsWork";
	/** 字段名：开始工作时间 */
	public static final String MONGO_WORKTIME_START_OPERATION = "WorkTimeStartOperation";
	/** 字段名：是否在线 */
	public static final String MONGO_IS_ONLINE = "IsOnline";
	/** 字段名：未登入小时数 */
	public static final String MONGO_NOT_LOGIN_HOURS = "NotLoginHour";
	/** 字段名：未登入天数 */
	public static final String MONGO_NOT_LOGIN_DAYS = "NotLoginDays";
	/** 字段名：工作时间段 */
	public static final String MONGO_WORK_TIMELINE = "WorkTimeLine";
	/** 字段名：怠速时间段*/
	public static final String MONGO_IDLE_TIMELINE = "IdleTimeLine";
	/** 字段名：上次怠速增加时间点*/
	public static final String MONGO_IDLING_UPDATETIME = "IdleTimeUpdate";
	/** 字段名：工作时间段开始时间 */
	public static final String MONGO_WORK_TIMELINE_BEGIN = "BeginTime";
	/** 字段名：工作时间段结束时间 */
	public static final String MONGO_WORK_TIMELINE_END = "EndTime";
	/** 字段名：工作时间段合计工时 */
	public static final String MONGO_WORK_TIMELINE_TOTAL = "WorkTimeLineTotal";
	/** 字段名：工时上跳计数 */
	public static final String MONGO_TOTAL_WORKTIME_JUMP_UP = "TotalWorkTimeJumpUp";
	/** 字段名：工时下跳计数 */
	public static final String MONGO_TOTAL_WORKTIME_JUMP_DOWN = "TotalWorkTimeJumpDown";
	/** 字段名：是否作为统计的数据 */
	public static final String MONGO_IS_STATISTICS = "Is_Statistics";
	/** 字段名：是否电改液控 */
	public static final String MONGO_IS_LOCK_FUTILE = "IsLockFutile";
	/** 字段名：上午总工时 */
	public static final String MONGO_AM_WORKTIME = "AMWorkTime";
	/** 字段名：油位 */
	public static final String MONGO_FUEL_LEVEL = "FuelLevel";
	/** 字段名：发动机转速 */
	public static final String MONGO_ENGINE_SPEED = "EngineSpeed";
	/** 字段名：锁机级别 */
	public static final String MONGO_LOCK_LEVEL = "LockLevel";
	/** 字段名：锁机剩余时间 */
	public static final String MONGO_LOCK_TIME_LEFT = "LockTimeLeft";
	/** 字段名： 每日开始油位 */
	public static final String MONGO_DAY_FUEL_LEVEL_START = "DayFuelLevelStart";
	/** 字段名： 每日结束油位 */
	public static final String MONGO_DAY_FUEL_LEVEL_END = "DayFuelLevelEnd";
	/** 字段名： 每日加油量 */
	public static final String MONGO_DAY_FUEL_CHARGING = "DayFuelCharging";
	/** 字段名： 每日最高液压油温 */
	public static final String MONGO_MAX_HYDRAULIC_OIL_TEMPERATURE = "MaxHydraulicOilTemperature";
	/** 字段名： 每日最高水温 */
	public static final String MONGO_MAX_COOLING_WATER_TEMPERATURE = "MaxCoolingWaterTemperature";

	/** Cassandra 表名前缀 */
	public static final String CASSANDRA_TABLE_PREFIX = "exca_opdata_";
	/** vas platform Cassandra 表名前缀 */
	public static final String VAS_CASSANDRA_TABLE_PREFIX = "vas_opdata";
	/** Mongo 日报表表名 */
	public static final String MONGO_COLLECTION = "EVI_BIZ_DAYINFO_DL";
	/** MySql 实时表表名 */
	public static final String MYSQL_TABLE_NAME = "tab_biz_realtime_dl";

	/** Float 0 */
	public static final float FLOAT_VALUE_ZERO = 0.0001f;
	/** Float NAN */
	public static final float FLOAT_VALUE_NAN_ALT = -9999.99f;
	/** Float Infinite */
	public static final float FLOAT_VALUE_INFINITE_ALT = -99999.99f;
	/*桩机协议号开始*/
	public static final int ZHUANGJI_VERSION_START = 128;
	/*桩机协议号结束*/
	public static final int ZHUANGJI_VERSION_END = 149;
}
