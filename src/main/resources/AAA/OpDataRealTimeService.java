package com.evi.datahandle.service.impl.opdata2;

import cn.sany.evi.gateway.data.serializer.DataSerializer;
import cn.sany.evi.gateway.data.serializer.DataSerializerFactory;
import cn.sany.evi.gateway.data.serializer.EnumSerializerType;
import com.evi.datahandle.WFConstants;
import com.evi.datahandle.config.DataSourceProperties;
import com.evi.datahandle.service.AbstractHanderService;
import com.evi.datahandle.util.DataSourcePool;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.spark.MongoConnector;
import com.witsight.platform.exception.BusinessException;
import com.witsight.platform.model.BoEntity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 
 * 说明：Spark Streaming 工况数据解析入 MySql实时表和Mongo日报表
 * 
 * @Title: FloorDataServiceRT2.java
 * @Package com.evi.datahandle.service.impl.floordata
 * @See: {@link OpDataRealTimeService} </br>
 * @Copyright: Copyright (c) 2017 </br>
 * @Company: sany huax witsight team by product
 * @author: tangj42
 * @date: 2018年5月31日 下午3:09:51
 * @version: V1.0
 *
 */
@Service(WFConstants.OP_DATA_REALTIME2_TASKNAME)
@EnableConfigurationProperties(DataSourceProperties.class)
public class OpDataRealTimeService extends AbstractHanderService {

	private static final long serialVersionUID = 7254151626598505601L;

	// 公共日志对象
	private final static Logger logger = LoggerFactory.getLogger(OpDataRealTimeService.class);

	/** Mongo 批处理设置 */
	private static final BulkWriteOptions MONGO_BULK_OPTIONS = new BulkWriteOptions().ordered(false)
			.bypassDocumentValidation(true);

	/** SQL: 从MySql实时表读取上次最后保存的总工时等统计信息 **/
	private static final String SQL_LOAD_COUNTER = new StringBuilder().append("select ")
			.append(OpDataConstants.MYSQL_DAY_WORKTIME_BASE).append(",").append(OpDataConstants.MYSQL_IDLE_TIME_BASE)
			.append(",").append(OpDataConstants.MYSQL_FUEL_CONSUME_BASE).append(",")
			.append(OpDataConstants.MYSQL_IDLE_FUEL_CONSUME_BASE).append(",")
			.append(OpDataConstants.OPDATA_RECEIVE_TIME).append(",").append(OpDataConstants.MYSQL_DAY_WORKTIME)
			.append(",").append(OpDataConstants.MYSQL_IDLE_TIME).append(",").append(OpDataConstants.MYSQL_FUEL_CONSUME)
			.append(",").append(OpDataConstants.MYSQL_IDLE_FUEL_CONSUME).append(",")
			.append(OpDataConstants.OPDATA_TOTAL_WORKTIME).append(",").append(OpDataConstants.OPDATA_TOTAL_IDLE_TIME)
			.append(",").append(OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION).append(",")
			.append(OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION).append(",")
			.append(OpDataConstants.MYSQL_IS_LOCK_FUTILE).append(",").append(OpDataConstants.MYSQL_AM_WORKTIME)
			.append(",").append(OpDataConstants.MYSQL_TOTAL_WORKTIME_REALTIME).append(",")
			.append(OpDataConstants.MYSQL_TOTAL_IDLE_TIME_REALTIME).append(",")
			.append(OpDataConstants.MYSQL_TOTAL_FUEL_CONSUMPTION_REALTIME).append(",")
			.append(OpDataConstants.MYSQL_TOTAL_IDLE_FUEL_CONSUMPTION_REALTIME).append(",")
			.append(OpDataConstants.MYSQL_TOTAL_WORKTIME_JUMP_UP).append(",")
			.append(OpDataConstants.MYSQL_TOTAL_WORKTIME_JUMP_DOWN).append(",").append(OpDataConstants.OPDATA_LONGITUDE)
			.append(",").append(OpDataConstants.OPDATA_LATITUDE).append(",")
			.append(OpDataConstants.MYSQL_DAY_FUEL_LEVEL_START).append(",")
			.append(OpDataConstants.MYSQL_DAY_FUEL_LEVEL_END).append(",")
			.append(OpDataConstants.MYSQL_DAY_FUEL_CHARGING).append(",")
			.append(OpDataConstants.MYSQL_FUEL_CHARGING_SHUT_DOWN_FUEL_LEVEL).append(",")
			.append(OpDataConstants.MYSQL_FUEL_CHARGING_START_UP_TIME).append(",")
			.append(OpDataConstants.OPDATA_TIME_ZONE).append(",")
			.append(OpDataConstants.MYSQL_MAX_HYDRAULIC_OIL_TEMPERATURE).append(",")
			.append(OpDataConstants.MYSQL_MAX_COOLING_WATER_TEMPERATURE)

			.append(" from ").append(OpDataConstants.MYSQL_TABLE_NAME).append(" where ")
			.append(OpDataConstants.OPDATA_LOGIN_ID).append(" = ?").toString();

	/** insert sql 对应的字段名数组 */
	private static final String[] INSERT_COLUMN_NAMES = new String[] { OpDataConstants.MYSQL_ID,
			OpDataConstants.MYSQL_VERSION, OpDataConstants.MYSQL_CREATE_DATE, OpDataConstants.MYSQL_LAST_MODIFY,
			OpDataConstants.OPDATA_LOGIN_ID, OpDataConstants.OPDATA_DATA_VERSION, OpDataConstants.OPDATA_FCP,
			OpDataConstants.OPDATA_RECEIVE_TIME };

	private static final String SQL_SCHEMA = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME ='"
			+ OpDataConstants.MYSQL_TABLE_NAME + "'";

	/** timeline 格式分隔符 HH:MM */
	private static final String TIME_LINE_SEPARATOR = ":";

	@Autowired
	private transient SparkContext sparkContext;// spark context

	@Autowired
	private DataSourceProperties dataSourceProperties;// mysql data source 属性

	private String mongoDataBase;// mongo 数据库名称

	private int mySqlBatchSize;// mysql batch size

	private Map<Long, Long> timeOfReadMySql = new HashMap<>();
	private Map<Long, Long> timeOfReadMongo = new HashMap<>();

	@Override
	public boolean execute(JavaDStream<ConsumerRecord<byte[], byte[]>> javaDStream,
                           AtomicReference<OffsetRange[]> offsetRanges, String... args) throws BusinessException {

		// 注册Kryo序列化类
		sparkContext.getConf().registerKryoClasses(new Class[] {
				com.evi.datahandle.service.impl.opdata2.OpDataUtil.class, com.witsight.platform.model.BoEntity.class });

		// 获取MySql设置的Batch Size
		mySqlBatchSize = getProperties().getMysqlBatchSize();

		JavaSparkContext jsc = new JavaSparkContext(sparkContext);

		// 创建 Mongo Connector
		MongoConnector mongoConnector = MongoConnector.create(jsc);

		// 广播 Mongo Connector
		final Broadcast<MongoConnector> mc = jsc.broadcast(mongoConnector);

		// 获取 Mongo 数据库名
		mongoDataBase = sparkContext.getConf().get(WFConstants.CONFIG_KEY_SPARK_MONGODB_OUTPUT_DATABASE);

		JavaPairDStream<String, List<BoEntity>> stream = javaDStream.mapPartitionsToPair(
				new PairFlatMapFunction<Iterator<ConsumerRecord<byte[], byte[]>>, String, List<BoEntity>>() {
					private static final long serialVersionUID = 4481135049241851661L;
					DataSerializer<BoEntity> deserializer = DataSerializerFactory
							.createSerializer(EnumSerializerType.KryoFiltedRealData);

					Map<String, Tuple2<String, List<BoEntity>>> groupOpData = new HashMap<>(10240);

					@Override
					public Iterator<Tuple2<String, List<BoEntity>>> call(
							Iterator<ConsumerRecord<byte[], byte[]>> iterator) throws Exception {
						BoEntity entity = null;
						String loginId = null;
						Tuple2<String, List<BoEntity>> opData;

						while (iterator.hasNext()) {
							// 反序列化
							entity = deserializer.dataDeSerializer(iterator.next().value());
							// 验证数据有效性
							if (!OpDataUtil.validate(entity)) {
								continue;
							}

							// 工况按登录号分组
							loginId = entity.getValue(OpDataConstants.OPDATA_LOGIN_ID);
							opData = groupOpData.get(loginId);
							if (null == opData) {
								opData = new Tuple2<String, List<BoEntity>>(loginId, new ArrayList<>());
								groupOpData.put(loginId, opData);
							}

							opData._2.add(entity);
						}

						return groupOpData.values().iterator();
					}
				});

		// 开始更新数据库
		updateDatabaseWithBatch(stream, offsetRanges, mc.getValue());

		return true;

	}

	private void loadTimeLine(String id, Date lastUpdateTime, Stack<Date> timeline,
			MongoCollection<Document> collection) {
		try {
			Calendar cal = Calendar.getInstance();
			cal.setTime(lastUpdateTime);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

			// 生成报表时间 yyyy-MM-dd 00:00:00.000
			lastUpdateTime = cal.getTime();

			// 读取设备工作时间段信息
			FindIterable<Document> result = collection
					.find(Filters.and(Filters.eq(OpDataConstants.MONGO_LOGIN_ID, id),
							Filters.eq(OpDataConstants.MONGO_REPORT_TIME, lastUpdateTime)))
					.projection(new Document(OpDataConstants.MONGO_WORK_TIMELINE, 1));

			Document row = result.first();

			if (row != null && row.containsKey(OpDataConstants.MONGO_WORK_TIMELINE)) {

				// 初始化设备状态中的工作时间段信息

				@SuppressWarnings("unchecked")
				List<Document> ranges = (List<Document>) row.get(OpDataConstants.MONGO_WORK_TIMELINE);

				String[] temp;

				// 工作时间段强制转换为 HH:MM:00 - HH:MM:59
				for (Document range : ranges) {
					temp = ((String) range.get(OpDataConstants.MONGO_WORK_TIMELINE_BEGIN)).split(TIME_LINE_SEPARATOR);
					cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(temp[0]));
					cal.set(Calendar.MINUTE, Integer.parseInt(temp[1]));
					cal.set(Calendar.SECOND, 0);
					timeline.push(cal.getTime());

					temp = ((String) range.get(OpDataConstants.MONGO_WORK_TIMELINE_END)).split(TIME_LINE_SEPARATOR);
					cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(temp[0]));
					cal.set(Calendar.MINUTE, Integer.parseInt(temp[1]));
					cal.set(Calendar.SECOND, 59);
					timeline.push(cal.getTime());
				}
			}
		} catch (Exception e) {
			if (logger.isErrorEnabled()) {
				logger.error("error while load time line from mongo", e);
			}
		}
	}

	/*todo*/
	private void loadIdleTimeLine(String id, Date lastUpdateTime, Stack<Date> timeline,
							  MongoCollection<Document> collection) {
		try {
			Calendar cal = Calendar.getInstance();
			cal.setTime(lastUpdateTime);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

			// 生成报表时间 yyyy-MM-dd 00:00:00.000
			lastUpdateTime = cal.getTime();

			// 读取设备工作时间段信息
			FindIterable<Document> result = collection
					.find(Filters.and(Filters.eq(OpDataConstants.MONGO_LOGIN_ID, id),
							Filters.eq(OpDataConstants.MONGO_REPORT_TIME, lastUpdateTime)))
					.projection(new Document(OpDataConstants.MONGO_IDLE_TIMELINE, 1));

			Document row = result.first();

			if (row != null && row.containsKey(OpDataConstants.MONGO_IDLE_TIMELINE)) {

				// 初始化设备状态中的工作时间段信息

				@SuppressWarnings("unchecked")
				List<Document> ranges = (List<Document>) row.get(OpDataConstants.MONGO_IDLE_TIMELINE);

				String[] temp;

				// 工作时间段强制转换为 HH:MM:00 - HH:MM:59
				for (Document range : ranges) {
					temp = ((String) range.get(OpDataConstants.MONGO_WORK_TIMELINE_BEGIN)).split(TIME_LINE_SEPARATOR);
					cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(temp[0]));
					cal.set(Calendar.MINUTE, Integer.parseInt(temp[1]));
					cal.set(Calendar.SECOND, 0);
					timeline.push(cal.getTime());

					temp = ((String) range.get(OpDataConstants.MONGO_WORK_TIMELINE_END)).split(TIME_LINE_SEPARATOR);
					cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(temp[0]));
					cal.set(Calendar.MINUTE, Integer.parseInt(temp[1]));
					cal.set(Calendar.SECOND, 59);
					timeline.push(cal.getTime());
				}
			}

		} catch (Exception e) {
			if (logger.isErrorEnabled()) {
				logger.error("error while load time line from mongo", e);
			}
		}
	}

	/*todo*/
	private long loadIdleTimeUpdate(String id, Date lastUpdateTime,MongoCollection<Document> collection) {
		try {
			Calendar cal = Calendar.getInstance();
			cal.setTime(lastUpdateTime);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);

			// 生成报表时间 yyyy-MM-dd 00:00:00.000
			lastUpdateTime = cal.getTime();

			FindIterable<Document> result = collection
					.find(Filters.and(Filters.eq(OpDataConstants.MONGO_LOGIN_ID, id),
							Filters.eq(OpDataConstants.MONGO_REPORT_TIME, lastUpdateTime)))
					.projection(new Document(OpDataConstants.MONGO_IDLING_UPDATETIME, 1));

			Document row = result.first();

			if (row != null && row.containsKey(OpDataConstants.MONGO_IDLING_UPDATETIME)) {
				long time = (long)row.get(OpDataConstants.MONGO_IDLING_UPDATETIME);
				return time;
			}
			else{
				return 0;
			}
		} catch (Exception e) {
			if (logger.isErrorEnabled()) {
				logger.error("error while load time line from mongo", e);
			}
			return 0;
		}
	}

	/**
	 * 
	 * 说明：读取MySql中该设备的最新信息
	 * 
	 * @Title: loadMySql
	 * @See: {@link loadMySql}
	 * @author tangj42
	 * @param id
	 * @param state
	 * @param conn
	 * @return 是否读取成功
	 * @date: 2018年9月19日 上午11:48:44
	 */
	private boolean loadMySql(String id, RealTimeState state, Connection conn) {

		boolean success = false;

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(SQL_LOAD_COUNTER);
			stmt.setString(1, id);
			rs = stmt.executeQuery();

			if (rs.first()) {
				Object temp = rs.getObject(29);
				if (temp != null) {
					state.setTimeZone(rs.getFloat(29));
				}

				Calendar cal = Calendar.getInstance();
				cal.setTimeZone(TimeZone.getTimeZone("UTC"));
				Date updateTime = rs.getTimestamp(5, cal);

				updateTime = OpDataUtil.getDateByTimeZone(updateTime, state.getTimeZone());

				RealTimeCounter counterBase = new RealTimeCounter();
				counterBase.setDate(updateTime);
				counterBase.setWorkTime(rs.getFloat(1));
				counterBase.setIdleTime(rs.getFloat(2));
				counterBase.setFuelConsume(rs.getFloat(3));
				counterBase.setIdleFuelConsume(rs.getFloat(4));
				state.initCounterBase(counterBase);

				RealTimeCounter counter = state.getCounter();
				counter.setDate(updateTime);
				counter.setWorkTime(rs.getFloat(6));
				counter.setIdleTime(rs.getFloat(7));
				counter.setFuelConsume(rs.getFloat(8));
				counter.setIdleFuelConsume(rs.getFloat(9));

				RealTimeCounter lastCounter = state.getLastCounter();
				lastCounter.setDate(updateTime);
				lastCounter.setWorkTime(rs.getFloat(10));
				lastCounter.setIdleTime(rs.getFloat(11));
				lastCounter.setFuelConsume(rs.getFloat(12));
				lastCounter.setIdleFuelConsume(rs.getFloat(13));

				state.setLockFutile(rs.getInt(14) == 1);
				state.setAMWorkTime(rs.getFloat(15));

				RealTimeCounter lastCounterRT = state.getLastCounterRealTime();
				lastCounterRT.setDate(updateTime);
				lastCounterRT.setWorkTime(rs.getFloat(16));
				lastCounterRT.setIdleTime(rs.getFloat(17));
				lastCounterRT.setFuelConsume(rs.getFloat(18));
				lastCounterRT.setIdleFuelConsume(rs.getFloat(19));

				state.setTWTJumpUp(rs.getInt(20));
				state.setTWTJumpDown(rs.getInt(21));

				state.setLastValidLongitude(rs.getFloat(22));
				state.setLastValidLatitude(rs.getFloat(23));

				RealTimeStateFuelLevel fuelState = state.getFuelLevelState();
				temp = rs.getObject(24);
				if (temp != null) {
					fuelState.setDayFuelLevelStart(rs.getFloat(24));
				}

				temp = rs.getObject(25);
				if (temp != null) {
					fuelState.setDayFuelLevelEnd(rs.getFloat(25));
				}

				fuelState.setDayFuelCharging(rs.getFloat(26));

				temp = rs.getObject(27);
				if (temp != null) {
					fuelState.setShutDownFuelLevel(rs.getFloat(27));
				}

				temp = rs.getObject(28);
				if (temp != null) {
					fuelState.setStartUpTime(rs.getTimestamp(28, cal));
				}

				RealTimeStateDayMax dayMaxState = state.getDayMaxState();

				temp = rs.getObject(30);
				if (temp != null) {
					dayMaxState.setMaxHydraulicOilTemperature(rs.getFloat(30));
				}

				temp = rs.getObject(31);
				if (temp != null) {
					dayMaxState.setMaxCoolingWaterTemperature(rs.getFloat(31));
				}

				success = true;
			}
		} catch (Exception se) {
			if (logger.isErrorEnabled()) {
				logger.error("error while load info from mysql", se);
			}
		} finally {

			try {
				if (rs != null) {
					rs.close();
					rs = null;
				}
			} catch (SQLException se) {
				logger.error(se.getMessage());
			}

			try {
				if (stmt != null) {
					stmt.close();
					stmt = null;
				}
			} catch (SQLException se) {
				logger.error(se.getMessage());
			}
		}

		return success;
	}

	/**
	 * 
	 * 说明：插入数据到MySql
	 * 
	 * @Title: insertMySql
	 * @See: {@link insertMySql}
	 * @author tangj42
	 * @param state
	 *            设备状态
	 * @param conn
	 *            MySql 连接
	 * @date: 2018年9月19日 下午1:04:42
	 */
	private void insertMySql(RealTimeState state, Connection conn) {
		Statement stmtInsert = null;

		try {
			stmtInsert = conn.createStatement();
			stmtInsert.executeUpdate(generateSqlInsert(state));
		} catch (Exception se) {
			if (logger.isErrorEnabled()) {
				logger.error("error while insert mysql", se);
			}
		} finally {

			try {
				if (stmtInsert != null) {
					stmtInsert.close();
					stmtInsert = null;
				}
			} catch (SQLException se) {
				logger.error(se.getMessage());
			}
		}
	}

	/**
	 * 
	 * 说明：更新设备最新信息
	 * 
	 * @Title: update
	 * @See: {@link update}
	 * @author tangj42
	 * @param data
	 *            设备最新数据
	 * @param mc
	 *            Mongo Connector
	 * @param conn
	 *            MySql 连接
	 * @return 设备最新信息
	 * @date: 2018年9月19日 下午1:06:29
	 */
	private RealTimeState update(Tuple2<String, List<BoEntity>> data, MongoCollection<Document> collection,
                                 Connection conn) throws FileNotFoundException {

		RealTimeState curState = new RealTimeState();

		boolean needInsert = false;

		String loginid = data._1;

		long start = Calendar.getInstance().getTimeInMillis();
		// 从MySql实时表读取上次最后保存的总工时等统计信息
		boolean isExist = loadMySql(loginid, curState, conn);
		long cost = Calendar.getInstance().getTimeInMillis() - start;
		if (timeOfReadMySql.containsKey(cost)) {
			timeOfReadMySql.put(cost, timeOfReadMySql.get(cost) + 1);
		} else {
			timeOfReadMySql.put(cost, 1L);
		}

		if (isExist) {
			if (curState.getCounterBase().getDate() != null) {// 如果日期为null导致后续crash
				// 从Mongo日报表读取并初始化设备状态中的工作时间段信息
				start = Calendar.getInstance().getTimeInMillis();
				loadTimeLine(loginid, curState.getCounterBase().getDate(), curState.getTimeLine(), collection);
				/*todo*/
				loadIdleTimeLine(loginid, curState.getCounterBase().getDate(), curState.getIdleTimeLine(), collection);
				long time =loadIdleTimeUpdate(loginid, curState.getCounterBase().getDate(), collection);
				curState.setLastIdlingUpdateTime(time);

				cost = Calendar.getInstance().getTimeInMillis() - start;
				if (timeOfReadMongo.containsKey(cost)) {
					timeOfReadMongo.put(cost, timeOfReadMongo.get(cost) + 1);
				} else {
					timeOfReadMongo.put(cost, 1L);
				}
			}
		} else {
			needInsert = true;
		}

		// 用当前的数据更新设备的状态
		OutputStream ops=new FileOutputStream("c:/test.txt",true) ;
		PrintStream ps = new PrintStream(ops);
		curState.update(data._2,ps);
		// 更新字段值
		if (curState.isUpdated()) {
			updateColumnsMySql(curState);
			if (needInsert) {
				insertMySql(curState, conn);
			}
		}

		return curState;
	}

	private void updateColumnsMySql(RealTimeState state) {
		BoEntity data = state.getData();
		Object valueObject;

		valueObject = data.get(OpDataConstants.OPDATA_RECEIVE_TIME);
		data.put(OpDataConstants.OPDATA_RECEIVE_TIME, OpDataUtil.formatUtcYYYYMMDDHHMMSS((Date) valueObject));

		// float value;
		// valueObject = data.get(OpDataConstants.OPDATA_FUEL_LEVEL);
		// if (valueObject instanceof Float) {
		// value = (Float) valueObject;
		// if (value < 0.0f) {
		// value = 0.0f;
		// } else if (value > 100.0f) {
		// value = 100.0f;
		// }
		// data.put(OpDataConstants.OPDATA_FUEL_LEVEL, value);
		// }
	}

	private void updateDatabaseWithBatch(JavaPairDStream<String, List<BoEntity>> stream,
                                         AtomicReference<OffsetRange[]> offsetRanges, MongoConnector mc) {
		stream.foreachRDD((rdd) -> {

			rdd.foreachPartition(iterator -> {

				MongoClient client = null;

				Connection conn = null;
				Statement stmt = null;

				List<BoEntity> batchOpData = new ArrayList<>(mySqlBatchSize);
				List<String> batchSQLs = new ArrayList<>();
				List<UpdateOneModel<Document>> documents = new ArrayList<UpdateOneModel<Document>>();

				try {

					conn = DataSourcePool.getPool(dataSourceProperties).getConnection();

					stmt = conn.createStatement();

					String sql = null;

					client = mc.acquireClient();
					MongoCollection<Document> collection = client.getDatabase(mongoDataBase)
							.getCollection(OpDataConstants.MONGO_COLLECTION);

					RealTimeState state = null;

					int mysqlTotal = 0;
					int mysqlSuccess = 0;

					int mongoTotal = 0;

					int batchCnt = 0;

					long batchStartTime = 0;
					long batchTotalTime = 0;

					Tuple2<String, List<BoEntity>> data = null;

					// 读取schema信息
					BoEntity schema = new BoEntity();
					ResultSet rs = stmt.executeQuery(SQL_SCHEMA);
					while (rs.next()) {
						schema.put(rs.getString(1).toLowerCase(), null);
					}
					rs.close();

					long logStartTime = Calendar.getInstance().getTimeInMillis();

					timeOfReadMySql.clear();
					timeOfReadMongo.clear();

					long stateStartTime, stateTotal = 0L;

					int countTWTJump = 0;
					int countTWTJumpNew = 0;
					int countTWTJumpAll = 0;
					List<String> logs = new ArrayList<>();

					while (iterator.hasNext()) {

						data = iterator.next();
						stateStartTime = Calendar.getInstance().getTimeInMillis();
						state = update(data, collection, conn);
						stateTotal += Calendar.getInstance().getTimeInMillis() - stateStartTime;

						// job 重启后接收到的数据可能会早于上次最后保存的数据,导致数据为空造成后续处理失败
						if (!state.isUpdated()) {
							continue;
						}

						if (state.getTWTUpWithinBatch() > 0 || state.getTWTDownWithinBatch() > 0) {
							++countTWTJump;
							logs.addAll(state.getLogs());
						}

						if (state.getTWTJumpUp() > 0 || state.getTWTJumpDown() > 0) {
							++countTWTJumpAll;
							if (state.getTWTJumpUp() == state.getTWTUpWithinBatch()
									&& state.getTWTJumpDown() == state.getTWTDownWithinBatch()) {
								++countTWTJumpNew;
							}
						}

						++mysqlTotal;

						OpDataUtil.extras2json(state.getData(), schema);
						sql = generateSqlUpdate(state);

						batchOpData.add(state.getData());
						batchSQLs.add(sql);

						if (++batchCnt >= mySqlBatchSize) {
							stmt.clearBatch();
							for (String updateSql : batchSQLs) {
								stmt.addBatch(updateSql);
							}

							batchStartTime = Calendar.getInstance().getTimeInMillis();

							try {
								conn.setAutoCommit(false);
								stmt.executeBatch();
								conn.commit();
								mysqlSuccess += batchCnt;
							} catch (SQLException se) {
								logger.error("MySql batch update failed, try update individually!!!");

								if (conn != null) {
									conn.rollback();
								}

								List<Integer> failedIdx = new ArrayList<>();
								conn.setAutoCommit(true);
								for (int i = 0; i < batchSQLs.size(); ++i) {
									try {
										stmt.execute(batchSQLs.get(i));
										++mysqlSuccess;
									} catch (SQLException se2) {
										failedIdx.add(i);
										logger.error("MySql update failed!!! {}", se2.getMessage());
										logger.error(batchSQLs.get(i));
									}
								}

								String sqlUpdateReceiveTime;
								for (int idx : failedIdx) {
									sqlUpdateReceiveTime = generateSqlUpdateReceiveTime(batchOpData.get(idx));
									try {
										stmt.execute(sqlUpdateReceiveTime);
									} catch (SQLException se2) {
										logger.error("MySql update receive_time failed!!! {}", se2.getMessage());
										logger.error(sqlUpdateReceiveTime);
									}
								}
							}

							batchTotalTime += Calendar.getInstance().getTimeInMillis() - batchStartTime;
							batchOpData.clear();
							batchSQLs.clear();
							batchCnt = 0;
						}

						documents.addAll(state.getDocuments());
						state.release();
					}


					if (batchCnt > 0) {
						stmt.clearBatch();
						for (String updateSql : batchSQLs) {
							stmt.addBatch(updateSql);
						}

						batchStartTime = Calendar.getInstance().getTimeInMillis();

						try {
							conn.setAutoCommit(false);
							stmt.executeBatch();
							conn.commit();
							mysqlSuccess += batchCnt;
						} catch (SQLException se) {
							logger.error("MySql batch update failed, try update individually!!!");

							if (conn != null) {
								conn.rollback();
							}

							List<Integer> failedIdx = new ArrayList<>();
							conn.setAutoCommit(true);
							for (int i = 0; i < batchSQLs.size(); ++i) {
								try {
									stmt.execute(batchSQLs.get(i));
									++mysqlSuccess;
								} catch (SQLException se2) {
									failedIdx.add(i);
									logger.error("MySql update failed!!! {}", se2.getMessage());
									logger.error(batchSQLs.get(i));
								}
							}

							String sqlUpdateReceiveTime;
							for (int idx : failedIdx) {
								sqlUpdateReceiveTime = generateSqlUpdateReceiveTime(batchOpData.get(idx));
								try {
									stmt.execute(sqlUpdateReceiveTime);
								} catch (SQLException se2) {
									logger.error("MySql update receive_time failed!!! {}", se2.getMessage());
									logger.error(sqlUpdateReceiveTime);
								}
							}
						}
						batchTotalTime += Calendar.getInstance().getTimeInMillis() - batchStartTime;
					}

					stmt.clearBatch();
					batchOpData.clear();
					batchSQLs.clear();
					batchOpData = null;
					batchSQLs = null;

					logger.error("Save to MySql cost = " + batchTotalTime * 0.001 + " seconds, success / total = "
							+ mysqlSuccess + " / " + mysqlTotal);

					mongoTotal = documents.size();

					batchStartTime = Calendar.getInstance().getTimeInMillis();
					if (!documents.isEmpty()) {
						collection.bulkWrite(documents, MONGO_BULK_OPTIONS);
					}

					documents.clear();
					documents = null;

					logger.error("Save to Mongo cost = "
							+ (Calendar.getInstance().getTimeInMillis() - batchStartTime) * 0.001 + " seconds, rows = "
							+ mongoTotal);

					if (countTWTJump > 1000) {
						for (String log : logs) {
							logger.error(log);
						}
					}

					logger.error("●TWT JUMP devices within batch = " + countTWTJump);
					logger.error("●TWT JUMP devices within batch new = " + countTWTJumpNew);
					logger.error("●TWT JUMP devices all = " + countTWTJumpAll);

				} catch (SQLException e) {
					if (logger.isErrorEnabled()) {
						logger.error("error while update mysql", e);
					}
				} catch (MongoException e) {
					if (logger.isErrorEnabled()) {
						logger.error("error while update mongo", e);
					}
				} finally {
					if (stmt != null) {
						stmt.close();
						stmt = null;
					}

					if (conn != null) {
						conn.close();
					}

					if (client != null) {
						mc.releaseClient(client);
					}
				}
			});

			saveOffest(offsetRanges);
		});
	}

	/**
	 * 
	 * 说明：组装MySql实时表的Insert Sql, 只在设备初次上线时会被调用一次
	 * 
	 * @Title: generateSqlInsert
	 * @See: {@link #generateSqlInsert}
	 * @author tangj42
	 * @param state
	 *            设备状态信息
	 * @return Insert Sql
	 * @date: 2018年6月1日 上午10:04:23
	 */
	private static String generateSqlInsert(RealTimeState state) {

		StringBuilder sql = new StringBuilder(4096);

		BoEntity data = state.getData();

		String[] values = new String[INSERT_COLUMN_NAMES.length];

		values[0] = values[4] = "'" + data.get(OpDataConstants.OPDATA_LOGIN_ID) + "'";
		values[1] = "'0'";
		values[2] = "'" + OpDataUtil.formatUtcYYYYMMDDHHMMSS(Calendar.getInstance().getTime()) + "'";
		values[3] = "'" + OpDataUtil.formatUtcYYYYMMDDHHMMSS(Calendar.getInstance().getTime()) + "'";
		values[5] = "'" + data.get(OpDataConstants.OPDATA_DATA_VERSION) + "'";
		values[6] = "'" + data.get(OpDataConstants.OPDATA_FCP) + "'";
		values[7] = "'" + data.get(OpDataConstants.OPDATA_RECEIVE_TIME) + "'";

		sql.append("insert into ").append(OpDataConstants.MYSQL_TABLE_NAME).append(" (")
				.append(String.join(",", INSERT_COLUMN_NAMES)).append(") values (").append(String.join(",", values))
				.append(")");

		return sql.toString();
	}

	/**
	 * 
	 * 说明：组装MySql实时表的Update Sql
	 * 
	 * @Title: generateSqlUpdate
	 * @See: {@link #generateSqlUpdate}
	 * @author tangj42
	 * @param state
	 *            设备状态信息
	 * @return Update Sql
	 * @date: 2018年6月1日 上午10:06:14
	 */
	private static String generateSqlUpdate(RealTimeState state) {

		StringBuilder sql = new StringBuilder(4096);

		BoEntity data = state.getData();

		if (data.containsKey(OpDataConstants.OPDATA_EXTRAS)) {
			String extras = data.getValue(OpDataConstants.OPDATA_EXTRAS);
			extras = extras.replace("\\", "\\\\").replace("'", "\\'");
			data.put(OpDataConstants.OPDATA_EXTRAS, extras);
		}

		String loginId = (String) data.get(OpDataConstants.OPDATA_LOGIN_ID);
		data.remove(OpDataConstants.OPDATA_LOGIN_ID);
		RealTimeCounter counter = state.getCounter();
		RealTimeCounter counterBase = state.getCounterBase();
		OpDataUtil.correctFloatColumn(data);

		sql.append("update ").append(OpDataConstants.MYSQL_TABLE_NAME).append(" set ");
		sql.append(OpDataConstants.MYSQL_DAY_WORKTIME).append(" = '").append(counter.getWorkTime()).append("',");
		sql.append(OpDataConstants.MYSQL_IDLE_TIME).append(" = '").append(counter.getIdleTime()).append("',");
		sql.append(OpDataConstants.MYSQL_FUEL_CONSUME).append(" = '").append(counter.getFuelConsume()).append("',");
		sql.append(OpDataConstants.MYSQL_IDLE_FUEL_CONSUME).append(" = '").append(counter.getIdleFuelConsume())
				.append("',");
		sql.append(OpDataConstants.MYSQL_DAY_WORKTIME_BASE).append(" = '").append(counterBase.getWorkTime())
				.append("',");
		sql.append(OpDataConstants.MYSQL_IDLE_TIME_BASE).append(" = '").append(counterBase.getIdleTime()).append("',");
		sql.append(OpDataConstants.MYSQL_FUEL_CONSUME_BASE).append(" = '").append(counterBase.getFuelConsume())
				.append("',");
		sql.append(OpDataConstants.MYSQL_IDLE_FUEL_CONSUME_BASE).append(" = '").append(counterBase.getIdleFuelConsume())
				.append("',");
		sql.append(OpDataConstants.MYSQL_WORK_TIMELINE_TOTAL).append(" = '").append(state.getWorkTimeLineTotal())
				.append("',");
		sql.append(OpDataConstants.MYSQL_IS_LOCK_FUTILE).append(" = '").append(state.isLockFutile() ? 1 : 0)
				.append("',");
		sql.append(OpDataConstants.MYSQL_AM_WORKTIME).append(" = '").append(state.getAMWorkTime()).append("',");
		sql.append(OpDataConstants.MYSQL_TOTAL_WORKTIME_JUMP_UP).append(" = '").append(state.getTWTJumpUp())
				.append("',");
		sql.append(OpDataConstants.MYSQL_TOTAL_WORKTIME_JUMP_DOWN).append(" = '").append(state.getTWTJumpDown())
				.append("',");
		sql.append(OpDataConstants.MYSQL_LAST_MODIFY).append(" = '")
				.append(OpDataUtil.formatUtcYYYYMMDDHHMMSS(Calendar.getInstance().getTime())).append("'");

		RealTimeStateFuelLevel fuelLevelState = state.getFuelLevelState();

		if (fuelLevelState.getDayFuelLevelStart() == null) {
			sql.append(",").append(OpDataConstants.MYSQL_DAY_FUEL_LEVEL_START).append(" = null");
		} else {
			sql.append(",").append(OpDataConstants.MYSQL_DAY_FUEL_LEVEL_START).append(" = '")
					.append(fuelLevelState.getDayFuelLevelStart()).append("'");
		}

		if (fuelLevelState.getDayFuelLevelEnd() == null) {
			sql.append(",").append(OpDataConstants.MYSQL_DAY_FUEL_LEVEL_END).append(" = null");
		} else {
			sql.append(",").append(OpDataConstants.MYSQL_DAY_FUEL_LEVEL_END).append(" = '")
					.append(fuelLevelState.getDayFuelLevelEnd()).append("'");
		}

		sql.append(",").append(OpDataConstants.MYSQL_DAY_FUEL_CHARGING).append(" = '")
				.append(fuelLevelState.getDayFuelCharging()).append("'");

		if (fuelLevelState.getShutDownFuelLevel() == null) {
			sql.append(",").append(OpDataConstants.MYSQL_FUEL_CHARGING_SHUT_DOWN_FUEL_LEVEL).append(" = null");
		} else {
			sql.append(",").append(OpDataConstants.MYSQL_FUEL_CHARGING_SHUT_DOWN_FUEL_LEVEL).append(" = '")
					.append(fuelLevelState.getShutDownFuelLevel()).append("'");
		}

		if (fuelLevelState.getStartUpTime() != null) {
			sql.append(",").append(OpDataConstants.MYSQL_FUEL_CHARGING_START_UP_TIME).append(" = '")
					.append(OpDataUtil.formatUtcYYYYMMDDHHMMSS(fuelLevelState.getStartUpTime())).append("'");
		}

		RealTimeStateDayMax dayMaxState = state.getDayMaxState();
		if (dayMaxState.getMaxHydraulicOilTemperature() != null) {
			sql.append(",").append(OpDataConstants.MYSQL_MAX_HYDRAULIC_OIL_TEMPERATURE).append(" = '")
					.append(dayMaxState.getMaxHydraulicOilTemperature()).append("'");
		}

		if (dayMaxState.getMaxCoolingWaterTemperature() != null) {
			sql.append(",").append(OpDataConstants.MYSQL_MAX_COOLING_WATER_TEMPERATURE).append(" = '")
					.append(dayMaxState.getMaxCoolingWaterTemperature()).append("'");
		}

		Date machineTimeStamp = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_MACHINE_TIMESTAMP, Date.class);
		if (machineTimeStamp != null) {
			data.put(OpDataConstants.OPDATA_MACHINE_TIMESTAMP, OpDataUtil.formatUtcYYYYMMDDHHMMSS(machineTimeStamp));
		}

		for (Entry<String, Object> col : data.entrySet()) {
			sql.append(",").append(col.getKey()).append(" = '").append(col.getValue()).append("'");
		}

		data.put(OpDataConstants.OPDATA_LOGIN_ID, loginId);

		sql.append(" where login_id = '").append(loginId).append("'");

		return sql.toString();
	}

	private static String generateSqlUpdateReceiveTime(BoEntity opdata) {
		StringBuilder sql = new StringBuilder();

		String loginId = opdata.getValue(OpDataConstants.OPDATA_LOGIN_ID);
		String receiveTime = opdata.getValue(OpDataConstants.OPDATA_RECEIVE_TIME);

		sql.append("update ").append(OpDataConstants.MYSQL_TABLE_NAME).append(" set ");
		sql.append(OpDataConstants.OPDATA_RECEIVE_TIME).append(" = '").append(receiveTime).append("'");
		sql.append(",").append(OpDataConstants.MYSQL_LAST_MODIFY).append(" = '")
				.append(OpDataUtil.formatUtcYYYYMMDDHHMMSS(Calendar.getInstance().getTime())).append("'");
		sql.append(" where ").append(OpDataConstants.OPDATA_LOGIN_ID).append(" = '").append(loginId).append("'");

		return sql.toString();
	}
}
