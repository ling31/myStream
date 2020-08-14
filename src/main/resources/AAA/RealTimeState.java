package com.evi.datahandle.service.impl.opdata2;

import com.evi.datahandle.decoder.opdata.OpDataDecoderUtil;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.witsight.platform.model.BoEntity;
import com.witsight.platform.util.lang.CollectionUtil;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.*;

/**
 * 说明：设备实时状态类
 *
 * @Title: RealTimeState.java
 * @Package com.evi.datahandle.service.impl.opdata
 * @See: {@link RealTimeState} </br>
 * @Copyright: Copyright (c) 2017 </br>
 * @Company: sany huax witsight team by product
 * @author: tangj42
 * @date: 2018年9月19日 上午10:45:55
 * @version: V1.0
 */
public class RealTimeState implements Serializable {

    private static final long serialVersionUID = -3295144034660148210L;
    // 公共日志对象
    private final static Logger logger = LoggerFactory.getLogger(RealTimeState.class);

    /**
     * 设备工作最小发动机转速
     */
    private static final int DEVICE_WORKING_ENGINE_SPEED_MIN = 600;
    /**
     * 10分钟内无转速大于等于600的数据上传,则认为该设备停止工作
     */
    private static final int DEVICE_STOP_WORKING_INTERVAL = 10 * 60 * 1000;
    /*2分钟内怠速字段不增加认为停止怠速*/
    private static final int DEVICE_STOP_IDLING_INTERVAL = 2 * 60 * 1000;
    /**
     * 工时跳变最小跳变量:0.5小时
     */
    private static final float TOTAL_WORK_TIME_JUMP_VALID_DIFF_HOUR_MIN = 0.5f;
    /**
     * 工时跳变最小时间间隔:5分钟
     */
    private static final long TOTAL_WORK_TIME_JUMP_VALID_DURATION_MS = 5 * 60 * 1000;

    /**
     * 电改液控判定, 锁机级别为1时,最小发动机转速
     */
    private static final int LOCK_FUTILE_OF_LEVEL1_ENGINE_SPEED_MIN = 1400;
    /**
     * 电改液控判定, 锁机级别为2时,最小发动机转速
     */
    private static final int LOCK_FUTILE_OF_LEVEL2_ENGINE_SPEED_MIN = 1200;

    /**
     * 测试设备用登陆号结尾
     */
    private static final String TEST_DEVICE_LOGIN_ID_SUFFIX_0000 = "0000";
    /**
     * 测试设备用登陆号结尾
     */
    private static final String TEST_DEVICE_LOGIN_ID_SUFFIX_0008 = "0008";

    private BoEntity lastData = null;// 上一条数据
    private Stack<Date> timeLine = new Stack<>();// 工时时间段信息
    /*怠速工时时间段信息*/
    private Stack<Date> idleTimeLine = new Stack<>();
    private Stack<UpdateOneModel<Document>> mongoDocuments = new Stack<>();// mongo数据集
    private RealTimeCounter counter = new RealTimeCounter();// 实时统计信息
    private RealTimeCounter lastCounter = new RealTimeCounter();// 上一次统计信息
    private RealTimeCounter lastCounterRealTime = new RealTimeCounter();// 上一次统计信息 - 实时字段值
    private RealTimeCounter counterBase = null;// 实时基准信息
    private RealTimeCounter counterTemp = new RealTimeCounter();// 临时统计信息

    /**
     * mongo 更新配置
     */
    private static final UpdateOptions MONGO_UPDATE_OPTIONS = new UpdateOptions().upsert(true)
            .bypassDocumentValidation(true);

    private long lastUpdateTime = 0;// ms
    private long lastWorkingUpdateTime = 0;// ms
    /*上次怠速增长时间点*/
    private long lastIdlingUpdateTime = 0;// ms
    private float workTimeAM = 0.0f;// hour, 每日上午总工时统计
    private float workTimeLineTotal = 0.0f;// hour
    private long startTimeOfAM = 0L;// 上午开始时间
    private long endTimeOfAM = 0L;// 上午结束时间

    private Date reportTime = null;// 日报表日期
    private boolean isDeviceWorking = false;// 是否设备在工作
    private boolean isDeviceJustStartWorking = false;// 是否设备正开始工作
    private boolean isDeviceIdling = false;// 是否设备在怠速
    private boolean isDeviceJustStartIdling = false;// 是否设备正开始怠速
    private int countTWTJumpUp = 0;// Work time total 正跳变计数
    private int countTWTJumpDown = 0;// Work time total 负跳变计数
    private Date createTime = null;// 每天对应记录的创建时间
    private boolean isUpdated = false;// 标记是否当前批次的数据为新数据并更新
    private boolean isLockFutile = false;// 是否电改液控

    private float lastValidLongitude = 0.0f; // 最近有效经度
    private float lastValidLatitude = 0.0f;// 最近有效纬度

    private Float timeZone = null;
    private RealTimeStateFuelLevel fuelLevelState = new RealTimeStateFuelLevel();
    private RealTimeStateDayMax dayMaxState = new RealTimeStateDayMax();

    public RealTimeState() {
    }

    public void release() {
        lastData = null;
        timeLine.clear();
        timeLine = null;
        idleTimeLine.clear();
        idleTimeLine = null;
        mongoDocuments.clear();
        mongoDocuments = null;
        counter = null;
        lastCounter = null;
        lastCounterRealTime = null;
        counterBase = null;
        counterTemp = null;
        fuelLevelState = null;
    }

    public void setTWTJumpUp(int value) {
        this.countTWTJumpUp = value;
    }

    public void setTWTJumpDown(int value) {
        this.countTWTJumpDown = value;
    }

    /**
     * 说明：判断数据是否被更新
     *
     * @return 是否被更新
     * @Title: isUpdated
     * @See: {@link isUpdated}
     * @author tangj42
     * @date: 2018年9月19日 上午10:48:03
     */
    public boolean isUpdated() {
        return isUpdated;
    }

    /**
     * 说明：判断设备是否电改液控
     *
     * @return 是否电改液控
     * @Title: isLockFutile
     * @See: {@link isLockFutile}
     * @author tangj42
     * @date: 2018年9月19日 上午10:48:52
     */
    public boolean isLockFutile() {
        return this.isLockFutile;
    }

    /**
     * 说明：设置是否电改液控
     *
     * @param value 是否电改液控
     * @Title: setLockFutile
     * @See: {@link setLockFutile}
     * @author tangj42
     * @date: 2018年9月19日 上午10:50:09
     */
    public void setLockFutile(boolean value) {
        this.isLockFutile = value;
    }

    public void initCounterBase(RealTimeCounter other) {
        if (null == counterBase) {
            counterBase = other;
        }
    }

    public Stack<Date> getTimeLine() {
        return timeLine;
    }

    public Stack<Date> getIdleTimeLine() {
        return idleTimeLine;
    }

    public long getLastIdlingUpdateTime() {
        return lastIdlingUpdateTime;
    }

    public void setLastIdlingUpdateTime(long value) {
        lastIdlingUpdateTime = value;
    }

    public int getTWTJumpUp() {
        return countTWTJumpUp;
    }

    public int getTWTJumpDown() {
        return countTWTJumpDown;
    }

    public RealTimeCounter getCounter() {
        return counter;
    }

    public RealTimeCounter getLastCounter() {
        return lastCounter;
    }

    public RealTimeCounter getLastCounterRealTime() {
        return lastCounterRealTime;
    }

    public RealTimeCounter getCounterBase() {
        return counterBase;
    }

    /**
     * 说明：更新设备的最新统计信息
     *
     * @param list 最新批次的工况数据
     * @Title: update
     * @See: {@link update}
     * @author tangj42
     * @date: 2018年9月19日 上午10:55:22
     */
    public void update(List<BoEntity> list, PrintStream ps) {
        if (CollectionUtil.isEmpty(list)) {
            return;
        }

        BoEntity newData = null;

        float engineSpeed = 0.0f;
        int lockLevel = 0;

        int newDay = 0;
        int lastDay = 0;

        Date receiveTime = null;
        Date lastReceiveTime = null;
        long newUpdateTime = 0;

        isUpdated = false;

        mongoDocuments.clear();

        for (int i = 0; i < list.size(); ++i) {

            newData = list.get(i);
            if (null == newData) {
                continue;
            }

            if (null == this.timeZone) {
                updateTimeZone(newData);
            }
            /*判断是否是桩机*/
            boolean ZHUANGJI = false;
            int dataVersion = Integer.valueOf(newData.getValue(OpDataConstants.OPDATA_DATA_VERSION));
            if (dataVersion >= OpDataConstants.ZHUANGJI_VERSION_START && dataVersion <= OpDataConstants.ZHUANGJI_VERSION_END) {
                ZHUANGJI = true;
            }
//			if(!ZHUANGJI){
//				continue;
//			}
            String loginid = newData.getValue(OpDataConstants.OPDATA_LOGIN_ID);

            generateLocalDate(newData);

            receiveTime = newData.getValue(OpDataConstants.OPDATA_RECEIVE_TIME_LOCAL);

            newUpdateTime = receiveTime.getTime();

            newDay = Integer.parseInt(OpDataDecoderUtil.formatYYYYMMDD(receiveTime));

            float newTotalIdleTime = 0.0f;
            float lastTotalIdleTime = 0.0f;
            if (OpDataUtil.validateColumn(newData, OpDataConstants.OPDATA_TOTAL_IDLE_TIME, Float.class)) {
                newTotalIdleTime = newData.getValue(OpDataConstants.OPDATA_TOTAL_IDLE_TIME);
            }

            boolean hisFirst = false;
            if (null == lastData) {
                /*是历史第一条数据*/
                if (null == counterBase) {
                    lastDay = 0;
                    counterBase = new RealTimeCounter();
                    hisFirst = true;
                    ps.println("####历史第一条数据：" + newTotalIdleTime + " 接受时间：" + receiveTime);
                }
                /*不是历史第一条数据*/
                else {
                    lastUpdateTime = lastCounter.getDate().getTime();
                    lastDay = Integer.parseInt(OpDataDecoderUtil.formatYYYYMMDD(lastCounter.getDate()));
                    lastTotalIdleTime = lastCounter.getIdleTime();
                    ps.println(loginid + "####本批第【1】条数据：" + newTotalIdleTime + " 接受时间：" + receiveTime + " 版本：" + String.valueOf(dataVersion));
                }
                if (!timeLine.isEmpty()) {
                    lastWorkingUpdateTime = timeLine.lastElement().getTime();
                }
                ps.println("    lastIdlingUpdateTime from load：" + new Date(lastIdlingUpdateTime));
            }
            /*不是本批第一条数据*/
            else {
                lastReceiveTime = lastData.getValue(OpDataConstants.OPDATA_RECEIVE_TIME_LOCAL);
                lastDay = Integer.parseInt(OpDataDecoderUtil.formatYYYYMMDD(lastReceiveTime));
                lastUpdateTime = lastReceiveTime.getTime();
                /*todo*/
                if (OpDataUtil.validateColumn(lastData, OpDataConstants.OPDATA_TOTAL_IDLE_TIME, Float.class)) {
                    lastTotalIdleTime = lastData.getValue(OpDataConstants.OPDATA_TOTAL_IDLE_TIME);
                }

                ps.println(loginid + "####本批第0【" + (i + 1) + "】条数据：" + newTotalIdleTime + " 接受时间：" + receiveTime);
                ps.println("    lastIdlingUpdateTime：" + new Date(lastIdlingUpdateTime));
            }

            isDeviceWorking = false;
            if (OpDataUtil.validateColumn(newData, OpDataConstants.OPDATA_ENGINE_SPEED, Float.class)) {
                engineSpeed = newData.getValue(OpDataConstants.OPDATA_ENGINE_SPEED);
                if (engineSpeed >= DEVICE_WORKING_ENGINE_SPEED_MIN) {
                    isDeviceWorking = true;
                }
            }

            if (OpDataUtil.validateColumn(newData, OpDataConstants.OPDATA_LOCK_LEVEL, Integer.class)) {
                lockLevel = newData.getValue(OpDataConstants.OPDATA_LOCK_LEVEL);
            }

            /*顺序正确*/
            if (newUpdateTime >= lastUpdateTime) {
                isDeviceJustStartWorking = false;
                if (isDeviceWorking) {
                    if (newUpdateTime - lastWorkingUpdateTime >= DEVICE_STOP_WORKING_INTERVAL) {
                        isDeviceJustStartWorking = true;
                    }
                    lastWorkingUpdateTime = newUpdateTime;
                }
///////////////////////////////////////////////////////////////////////////////////////////
                /*todo*/
                /*判断当前数据是否idling*/
                if (ZHUANGJI) {
                    isDeviceIdling = false;
                    isDeviceJustStartIdling = false;

                    if (idleTimeLine.isEmpty()) {
                        ps.println("    timeline为空");
                        //史上第一条
                        if (hisFirst) {
                            isDeviceIdling = false;
                            ps.println("    史上第一条忽略判断是否idle");
                        }
                        //一直就没idling的第一条add
                        else if (newTotalIdleTime > lastTotalIdleTime) {
                            isDeviceIdling = true;
                            isDeviceJustStartIdling = true;
                            lastIdlingUpdateTime = newUpdateTime;
                            ps.println(lastTotalIdleTime + " -> " + newTotalIdleTime);
                            ps.println("    timeline为空的第一次增加，is/just true，设置lastIdlingUpdateTime：" + new Date(lastIdlingUpdateTime));
                        }
                    }
                    //idletimeline非空
                    else {
                        ps.println("    timeline非空：" + idleTimeLine);
                        //比上一条idle增加了 mysql double(50,6) 改成 double(50,8)
                        if (newTotalIdleTime > lastTotalIdleTime) {
                            isDeviceIdling = true;
                            if (newUpdateTime - lastIdlingUpdateTime > DEVICE_STOP_IDLING_INTERVAL) {
                                isDeviceJustStartIdling = true;
                                ps.println(lastTotalIdleTime + " -> " + newTotalIdleTime);
                                ps.println("    【增加】idle增加，比上次增加大于1分钟，is/just true");
                            } else {
                                isDeviceJustStartIdling = false;
                                ps.println(lastTotalIdleTime + " -> " + newTotalIdleTime);
                                ps.println("    【增加】idle增加，比上次增加小于1分钟，is true/ just false");
                            }
                            lastIdlingUpdateTime = newUpdateTime;
                            ps.println("    更新lastIdlingUpdateTime：" + new Date(lastIdlingUpdateTime));
                        }
                        //虽然值没增加 但是小于时间间隔时还是增加，不过更新时间不变
                        else if ((newTotalIdleTime <= lastTotalIdleTime) && (newUpdateTime - lastIdlingUpdateTime < DEVICE_STOP_IDLING_INTERVAL)) {
                            isDeviceIdling = true;
                            ps.println(lastTotalIdleTime + " -> " + newTotalIdleTime);
                            ps.println("    【增加】idle不变，比上次增加小于1分钟，is true/ just：" + isDeviceJustStartIdling);
                            ps.println("    不更新lastIdlingUpdateTime，上次：" + new Date(lastIdlingUpdateTime));
                        } else {
                            ps.println("    【没增】idle不变，比上次增加大于1分钟，is false/ just：" + isDeviceJustStartIdling);
                            ps.println("    不更新lastIdlingUpdateTime，上次：" + new Date(lastIdlingUpdateTime));
                        }
                    }
                }
///////////////////////////////////////////////////////////////////////////////////////////
                if (lastCounterRealTime.getDate() != null) {
                    updateTWTJumpCounter(newData);
                }

                /*没有跨天*/
                if (newDay == lastDay) {

                    if (null == reportTime) {
                        updateReportTime(receiveTime);
                    }

                    if (isDeviceWorking) {
                        if (isDeviceJustStartWorking || timeLine.isEmpty()) {
                            timeLine.push(receiveTime);
                            timeLine.push(receiveTime);
                        } else {
                            timeLine.pop();
                            timeLine.push(receiveTime);
                        }
                    }

                    /*更新桩机的怠速时间段*/
                    if (ZHUANGJI) {
                        if (isDeviceIdling) {
                            if (isDeviceJustStartIdling || idleTimeLine.isEmpty()) {
                                idleTimeLine.push(receiveTime);
                                idleTimeLine.push(receiveTime);
                                ps.println("    isidling，just为true 或者 timeline为空，push*2：" + receiveTime);
                                ps.println("    新timeline为：" + idleTimeLine);
                            } else {
                                idleTimeLine.pop();
                                idleTimeLine.push(receiveTime);
                                ps.println("    isidling，just为false，pop，push：" + receiveTime);
                                ps.println("    新timeline为：" + idleTimeLine);
                            }
                        }
                    }
                }
                /*元素是新的一天，包括历史第一条*/
                else if (newDay > lastDay) {

                    updateTimeZone(newData);
                    generateLocalDate(newData);
                    receiveTime = newData.getValue(OpDataConstants.OPDATA_RECEIVE_TIME_LOCAL);
                    lastWorkingUpdateTime = receiveTime.getTime();

                    countTWTJumpUp = 0;
                    countTWTJumpDown = 0;

                    /*是当批次中间一条元素跨天，此刻就要在mongo插，在mysql更新*/
                    if (lastData != null) {
                        generateExtraColumns(list, lastData);
                        generateDocument(lastData);
                    }

                    timeLine.clear();
                    idleTimeLine.clear();
                    ps.println("    跨天注意!!!，timeline清空!!!");
                    counter.zero();
                    counterBase.zero();
                    fuelLevelState.reset();
                    dayMaxState.reset();

                    isLockFutile = false;

                    workTimeAM = 0.0f;

                    if (isDeviceWorking) {
                        timeLine.push(receiveTime);
                        timeLine.push(receiveTime);
                    }

                    /*跨天时桩机怠速时间段新建*/
                    if (ZHUANGJI) {
                        if (isDeviceIdling) {
                            idleTimeLine.push(receiveTime);
                            idleTimeLine.push(receiveTime);
                            ps.println("    isDeviceIdling=true，跨天，push*2：" + receiveTime);
                            ps.println("    新timeline为：" + idleTimeLine);
                            ps.println(lastTotalIdleTime + " -> " + newTotalIdleTime);
                            ps.println("    lastIdlingUpdateTime：" + new Date(lastIdlingUpdateTime));
                        } else {
                            ps.println("    isDeviceIdling=false，跨天！无动作：" + receiveTime);
                            ps.println(lastTotalIdleTime + " -> " + newTotalIdleTime);
                            ps.println("    lastIdlingUpdateTime：" + new Date(lastIdlingUpdateTime));
                        }
                    }

                    updateReportTime(receiveTime);
                    mongoDocuments.push(null);// 占位 一天一条
                    // generateDocument(newData, true);
                }

                updateCounterBase(newData);

                fuelLevelState.update(newData, lastUpdateTime);
                dayMaxState.update(newData);

                lastData = newData;

                updateCounter();

                updateLockFutile(engineSpeed, lockLevel);

                isUpdated = true;
                ps.println("    第" + (i + 1) + "条元素后timeline为：" + idleTimeLine);

            }
            /*暂时不管*/
            else {
                if (newDay == lastDay) {
                    if (receiveTime.before(counterBase.getDate())) {
                        updateCounterBase(newData);
                    }

                    if (lastData != null && updateLockFutile(engineSpeed, lockLevel)) {
                        isUpdated = true;
                    }
                }
            }

            newData.remove(OpDataConstants.OPDATA_TIME_ZONE);
        }

        if (isUpdated) {
            generateExtraColumns(list, lastData);
            generateDocument(lastData);
            lastData.remove(OpDataConstants.OPDATA_RECEIVE_TIME_LOCAL);
            lastData.put(OpDataConstants.OPDATA_TIME_ZONE, timeZone);
        }
    }

    private void updateTimeZone(BoEntity opData) {
        Float tz = OpDataUtil.getColumnValue(opData, OpDataConstants.OPDATA_TIME_ZONE, Float.class);
        if (null == tz) {
            setTimeZone(OpDataUtil.TIME_ZONE_DEFAULT);
        } else {
            setTimeZone(tz);
        }
    }

    private void generateLocalDate(BoEntity opData) {
        Date receiveTime = opData.getValue(OpDataConstants.OPDATA_RECEIVE_TIME);
        opData.put(OpDataConstants.OPDATA_RECEIVE_TIME_LOCAL, OpDataUtil.getDateByTimeZone(receiveTime, timeZone));
    }

    private void generateExtraColumns(List<BoEntity> rows, BoEntity values) {

        if (CollectionUtil.isEmpty(rows) || null == values) {
            return;
        }

        BoEntity row = null;
        Date receiveTime;
        String receiveTimeUTC;
        Float longitude, latitude, fuellevel, totalWorkTime, totalIdleTime, totalFuelConsume, totalIdleFuelConsume,
                value;
        boolean isLnLUpdated = false;

        for (int i = 0; i < rows.size(); ++i) {
            row = rows.get(i);
            receiveTime = row.getValue(OpDataConstants.OPDATA_RECEIVE_TIME);
            receiveTimeUTC = OpDataUtil.formatUtcYYYYMMDDHHMMSS(receiveTime);

            totalWorkTime = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_TOTAL_WORKTIME, Float.class);
            if (totalWorkTime != null && Float.isFinite(totalWorkTime) && totalWorkTime > 0.0f) {
                values.put(OpDataConstants.MYSQL_RECEIVE_TIME_VALID_TOTAL_WORKTIME, receiveTimeUTC);
            }

            totalIdleTime = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_TOTAL_IDLE_TIME, Float.class);
            if (totalIdleTime != null && Float.isFinite(totalIdleTime) && totalIdleTime > 0.0f) {
                values.put(OpDataConstants.MYSQL_RECEIVE_TIME_VALID_TOTAL_IDLE_TIME, receiveTimeUTC);
            }

            fuellevel = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_FUEL_LEVEL, Float.class);
            if (fuellevel != null && Float.isFinite(fuellevel) && fuellevel > 0.0f) {
                values.put(OpDataConstants.OPDATA_RECEIVE_TIME_VALID_FUEL_LEVEL, receiveTimeUTC);
                if (totalWorkTime != null && Float.isFinite(totalWorkTime)) {
                    values.put(OpDataConstants.MYSQL_TOTAL_WORKTIME_VALID_FUEL_LEVEL, totalWorkTime);
                }
            }

            totalFuelConsume = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION,
                    Float.class);
            if (totalFuelConsume != null && Float.isFinite(totalFuelConsume) && totalFuelConsume > 0.0f) {
                values.put(OpDataConstants.MYSQL_RECEIVE_TIME_VALID_TOTAL_FUEL_CONSUME, receiveTimeUTC);
                if (totalWorkTime != null && Float.isFinite(totalWorkTime)) {
                    values.put(OpDataConstants.MYSQL_TOTAL_WORKTIME_VALID_TOTAL_FUEL_CONSUME, totalWorkTime);
                }
            }

            totalIdleFuelConsume = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION,
                    Float.class);
            if (totalIdleFuelConsume != null && Float.isFinite(totalIdleFuelConsume) && totalIdleFuelConsume > 0.0f) {
                values.put(OpDataConstants.MYSQL_RECEIVE_TIME_VALID_TOTAL_IDLE_FUEL_CONSUME, receiveTimeUTC);
            }

            longitude = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_LONGITUDE, Float.class);
            latitude = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_LATITUDE, Float.class);

            if (null == longitude || null == latitude || Float.isInfinite(longitude) || Float.isInfinite(latitude)) {
                continue;
            }

            // 保留最近有效的经纬度
            if (Math.abs(longitude) > OpDataConstants.FLOAT_VALUE_ZERO
                    && Math.abs(latitude) > OpDataConstants.FLOAT_VALUE_ZERO) {
                values.put(OpDataConstants.OPDATA_LONGITUDE, longitude);
                values.put(OpDataConstants.OPDATA_LATITUDE, latitude);
                values.put(OpDataConstants.OPDATA_RECEIVE_TIME_VALID_LAL, receiveTimeUTC);

                isLnLUpdated = true;
            }

            // 记录实时经纬度
            values.put(OpDataConstants.OPDATA_LONGITUDE_REALTIME, longitude);
            values.put(OpDataConstants.OPDATA_LATITUDE_REALTIME, latitude);
        }

        if (!isLnLUpdated) {
            values.put(OpDataConstants.OPDATA_LONGITUDE, this.lastValidLongitude);
            values.put(OpDataConstants.OPDATA_LATITUDE, this.lastValidLatitude);
        }

        // 记录以下字段的实时值
        value = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_TOTAL_WORKTIME, Float.class);
        if (value != null) {
            values.put(OpDataConstants.MYSQL_TOTAL_WORKTIME_REALTIME, value);
        }

        value = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_TOTAL_IDLE_TIME, Float.class);
        if (value != null) {
            values.put(OpDataConstants.MYSQL_TOTAL_IDLE_TIME_REALTIME, value);
        }

        value = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION, Float.class);
        if (value != null) {
            values.put(OpDataConstants.MYSQL_TOTAL_FUEL_CONSUMPTION_REALTIME, value);
        }

        value = OpDataUtil.getColumnValue(row, OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION, Float.class);
        if (value != null) {
            values.put(OpDataConstants.MYSQL_TOTAL_IDLE_FUEL_CONSUMPTION_REALTIME, value);
        }
    }

    private boolean updateLockFutile(float engineSpeed, int lockLevel) {

        boolean updated = false;

        if (!isLockFutile) {
            if (1 == lockLevel && engineSpeed >= LOCK_FUTILE_OF_LEVEL1_ENGINE_SPEED_MIN) {
                isLockFutile = true;
                updated = true;
            } else if (2 == lockLevel && engineSpeed >= LOCK_FUTILE_OF_LEVEL2_ENGINE_SPEED_MIN) {
                isLockFutile = true;
                updated = true;
            }
        }

        return updated;
    }

    /**
     * 说明：统计工时跳变次数
     *
     * @param cur
     * @Title: updateTWTJumpCounter
     * @author tangj42
     * @date: 2019年5月8日 下午3:17:19
     */
    private void updateTWTJumpCounter(BoEntity cur) {

        String loginId = (String) cur.get(OpDataConstants.OPDATA_LOGIN_ID);
        if (loginId.endsWith(TEST_DEVICE_LOGIN_ID_SUFFIX_0000) || loginId.endsWith(TEST_DEVICE_LOGIN_ID_SUFFIX_0008)) {
            return;
        }

        Date receiveTimeCur = cur.getValue(OpDataConstants.OPDATA_RECEIVE_TIME_LOCAL);
        long diffReceiveTime = receiveTimeCur.getTime() - lastCounterRealTime.getDate().getTime();
        if (diffReceiveTime > TOTAL_WORK_TIME_JUMP_VALID_DURATION_MS) {
            return;
        }

        if (OpDataUtil.validateColumn(cur, OpDataConstants.OPDATA_TOTAL_WORKTIME, Float.class)) {
            float curTWT = cur.getValue(OpDataConstants.OPDATA_TOTAL_WORKTIME);
            float lastTWT = lastCounterRealTime.getWorkTime();
            if (curTWT >= OpDataConstants.FLOAT_VALUE_ZERO && lastTWT >= OpDataConstants.FLOAT_VALUE_ZERO) {
                float diffTWT = curTWT - lastTWT;
                if (diffTWT < -TOTAL_WORK_TIME_JUMP_VALID_DIFF_HOUR_MIN) {
                    ++countTWTJumpDown;
                    logTWTDown(cur.getValue(OpDataConstants.OPDATA_LOGIN_ID), receiveTimeCur,
                            lastCounterRealTime.getDate(), curTWT, lastTWT, countTWTJumpUp);
                } else if (diffTWT > TOTAL_WORK_TIME_JUMP_VALID_DIFF_HOUR_MIN) {
                    if (Math.abs(diffTWT - 1.0f) >= OpDataConstants.FLOAT_VALUE_ZERO) {
                        ++countTWTJumpUp;
                        logTWTUp(cur.getValue(OpDataConstants.OPDATA_LOGIN_ID), receiveTimeCur,
                                lastCounterRealTime.getDate(), curTWT, lastTWT, countTWTJumpUp);
                    }
                }
            }
        }

        lastCounterRealTime.set(cur);
    }

    private Date getCSTTime(Date receiveTime, float timeZone) {

        long time = receiveTime.getTime() + OpDataUtil.TIME_ZONE_DEFAULT_RAW_OFFSET - (int) (timeZone * 60 * 60 * 1000);

        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        return cal.getTime();

    }

    List<String> logs = new ArrayList<>();
    private int countTWTUp = 0;
    private int countTWTDown = 0;

    public List<String> getLogs() {
        return logs;
    }


    public int getTWTUpWithinBatch() {
        return countTWTUp;
    }

    public int getTWTDownWithinBatch() {
        return countTWTDown;
    }

    private void logTWTUp(String loginId, Date receiveTime, Date receiveTimeLast, float curTWT, float lastTWT,
                          int count) {
        receiveTime = getCSTTime(receiveTime, this.timeZone);
        receiveTimeLast = getCSTTime(receiveTimeLast, this.timeZone);
        float diffTWT = curTWT - lastTWT;
        String receiveTimeUTC = OpDataUtil.formatUtcYYYYMMDDHHMMSS(receiveTime);
        String receiveTimeLastUTC = OpDataUtil.formatUtcYYYYMMDDHHMMSS(receiveTimeLast);
        logs.add("▲TWT UP @ " + loginId + " : Time=" + receiveTimeUTC + ",TimeLast=" + receiveTimeLastUTC + ",TWT="
                + curTWT + ",TWTLast=" + lastTWT + ",TWTDiff=" + diffTWT + ",count=" + count);

        ++countTWTUp;
    }

    private void logTWTDown(String loginId, Date receiveTime, Date receiveTimeLast, float curTWT, float lastTWT,
                            int count) {
        receiveTime = getCSTTime(receiveTime, this.timeZone);
        receiveTimeLast = getCSTTime(receiveTimeLast, this.timeZone);
        float diffTWT = curTWT - lastTWT;
        String receiveTimeUTC = OpDataUtil.formatUtcYYYYMMDDHHMMSS(receiveTime);
        String receiveTimeLastUTC = OpDataUtil.formatUtcYYYYMMDDHHMMSS(receiveTimeLast);
        logs.add("▼TWT DOWN @ " + loginId + " : Time=" + receiveTimeUTC + ",TimeLast=" + receiveTimeLastUTC + ",TWT="
                + curTWT + ",TWTLast=" + lastTWT + ",TWTDiff=" + diffTWT + ",count=" + count);

        ++countTWTDown;
    }

    /**
     * 说明：更新基准信息
     *
     * @param data
     * @Title: updateCounterBase
     * @author tangj42
     * @date: 2019年5月8日 下午3:17:46
     */
    private void updateCounterBase(BoEntity data) {
        Float value;

        counterBase.setDate(data.getValue(OpDataConstants.OPDATA_RECEIVE_TIME_LOCAL));

        value = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_WORKTIME, Float.class);
        if (value != null && value >= OpDataConstants.FLOAT_VALUE_ZERO) {
            if (counterBase.getWorkTime() < OpDataConstants.FLOAT_VALUE_ZERO) {
                counterBase.setWorkTime(value);
                lastCounter.setWorkTime(value);
            } else if (value < counterBase.getWorkTime()) {
                // counterBase.setWorkTime(value);
            }
        }

        value = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_IDLE_TIME, Float.class);
        if (value != null && value >= OpDataConstants.FLOAT_VALUE_ZERO) {
            if (counterBase.getIdleTime() < OpDataConstants.FLOAT_VALUE_ZERO) {
                counterBase.setIdleTime(value);
                lastCounter.setIdleTime(value);
            } else if (value < counterBase.getIdleTime()) {
                // counterBase.setIdleTime(value);
            }
        }

        value = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION, Float.class);
        if (value != null && value >= OpDataConstants.FLOAT_VALUE_ZERO) {
            if (counterBase.getFuelConsume() < OpDataConstants.FLOAT_VALUE_ZERO) {
                counterBase.setFuelConsume(value);
                lastCounter.setFuelConsume(value);
            } else if (value < counterBase.getFuelConsume()) {
                // counterBase.setFuelConsume(value);
            }
        }

        value = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION, Float.class);
        if (value != null && value >= OpDataConstants.FLOAT_VALUE_ZERO) {
            if (counterBase.getIdleFuelConsume() < OpDataConstants.FLOAT_VALUE_ZERO) {
                counterBase.setIdleFuelConsume(value);
                lastCounter.setIdleFuelConsume(value);
            } else if (value < counterBase.getIdleFuelConsume()) {
                // counterBase.setIdleFuelConsume(value);
            }
        }
    }

    /**
     * 说明：更新日报表日期
     *
     * @param date
     * @Title: updateReportTime
     * @author tangj42
     * @date: 2019年5月8日 下午3:18:06
     */
    private void updateReportTime(Date date) {

        Calendar calendar = Calendar.getInstance();

        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        reportTime = calendar.getTime();

        startTimeOfAM = reportTime.getTime();

        calendar.set(Calendar.HOUR_OF_DAY, 12);
        endTimeOfAM = calendar.getTimeInMillis();
    }

    /**
     * 说明：获取最新的数据
     *
     * @return 最新的数据
     * @Title: getData
     * @See: {@link getData}
     * @author tangj42
     * @date: 2018年9月19日 上午10:52:02
     */
    public BoEntity getData() {
        return lastData;
    }

    /**
     * 说明：获取最新数据的 mongo documents
     *
     * @return mongo documents
     * @Title: getDocuments
     * @See: {@link getDocuments}
     * @author tangj42
     * @date: 2018年9月19日 上午10:52:44
     */
    public Stack<UpdateOneModel<Document>> getDocuments() {
        return mongoDocuments;
    }

    /**
     * 说明：更新统计信息
     *
     * @Title: updateCounter
     * @author tangj42
     * @date: 2019年5月8日 下午3:18:52
     */
    private void updateCounter() {

        if (null == lastData) {
            return;
        }

        counterTemp.set(lastData);

        // 只有总工时为有效值时，才更新后续的统计字段
        boolean isInvalidData = true;

        float value = 0.0f;

        if (counterBase.getWorkTime() < OpDataConstants.FLOAT_VALUE_ZERO) {
            counter.setWorkTime(0.0f);
        } else {
            value = counterTemp.getWorkTime() - counterBase.getWorkTime();

            if (value < 0.0f || value > 24.0f) {
                value = 0.0f;
            }

            if (value > counter.getWorkTime()) {
                counter.setWorkTime(value);
                isInvalidData = false;
            }
        }

        if (isInvalidData) {
            return;
        }

        if (counterBase.getIdleTime() < OpDataConstants.FLOAT_VALUE_ZERO) {
            counter.setIdleTime(0.0f);
        } else {
            value = counterTemp.getIdleTime() - counterBase.getIdleTime();

            if (value < 0.0f /* || value > 24.0f */) {
                value = 0.0f;
            }

            if (value > counter.getIdleTime()) {
                counter.setIdleTime(value);
            }
        }

        if (counterBase.getFuelConsume() < OpDataConstants.FLOAT_VALUE_ZERO) {
            counter.setFuelConsume(0.0f);
        } else {
            value = counterTemp.getFuelConsume() - counterBase.getFuelConsume();

            if (value > counter.getFuelConsume()) {
                counter.setFuelConsume(value);
            }
        }

        if (counterBase.getIdleFuelConsume() < OpDataConstants.FLOAT_VALUE_ZERO) {
            counter.setIdleFuelConsume(0.0f);
        } else {
            value = counterTemp.getIdleFuelConsume() - counterBase.getIdleFuelConsume();

            if (value > counter.getIdleFuelConsume()) {
                counter.setIdleFuelConsume(value);
            }
        }
    }

    /**
     * 说明：获取基于 time line 计算的当日总工时
     *
     * @return 当日总工时
     * @Title: getWorkTimeLineTotal
     * @See: {@link getWorkTimeLineTotal}
     * @author tangj42
     * @date: 2018年9月19日 上午10:53:31
     */
    public float getWorkTimeLineTotal() {
        return workTimeLineTotal;
    }

    /**
     * 说明：获取上午总工时
     *
     * @return getAMWorkTime
     * @Title: getAMWorkTime
     * @See: {@link getAMWorkTime}
     * @author tangj42
     * @date: 2018年9月19日 上午10:54:33
     */
    public float getAMWorkTime() {
        return workTimeAM;
    }

    public void setAMWorkTime(float amWorkTime) {
        workTimeAM = amWorkTime;
    }

/*	private void generateDocument(BoEntity data) {

		Document doc = new Document();
		Integer intValue;
		Float floatValue;

		// 以下4个字段值一天内只增不减， 每天开始的数据如果<=0.0则保留前一天的最后值,
		// 直到有 > 0.0的数据到达， 以第一条有效数据为基准开始一天的统计
		floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_WORKTIME, Float.class);
		if (null == floatValue) {
			data.put(OpDataConstants.OPDATA_TOTAL_WORKTIME, 0.0f);
		} else {
			if (floatValue < lastCounter.getWorkTime()) {
				data.put(OpDataConstants.OPDATA_TOTAL_WORKTIME, lastCounter.getWorkTime());
			}
		}

		floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_IDLE_TIME, Float.class);
		if (null == floatValue) {
			data.put(OpDataConstants.OPDATA_TOTAL_IDLE_TIME, 0.0f);
		} else {
			if (floatValue < lastCounter.getIdleTime()) {
				data.put(OpDataConstants.OPDATA_TOTAL_IDLE_TIME, lastCounter.getIdleTime());
			}
		}

		floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION, Float.class);
		if (null == floatValue) {
			data.put(OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION, 0.0f);
		} else {
			if (floatValue < lastCounter.getFuelConsume()) {
				data.put(OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION, lastCounter.getFuelConsume());
			}
		}

		floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION, Float.class);
		if (null == floatValue) {
			data.put(OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION, 0.0f);
		} else {
			if (floatValue < lastCounter.getIdleFuelConsume()) {
				data.put(OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION, lastCounter.getIdleFuelConsume());
			}
		}

		doc.put(OpDataConstants.MONGO_MACHINE_PROVIDER_CODE, "1");
		doc.put(OpDataConstants.MONGO_DATA_VERSION, data.get(OpDataConstants.OPDATA_DATA_VERSION));
		doc.put(OpDataConstants.MONGO_TOTAL_WORKTIME, data.get(OpDataConstants.OPDATA_TOTAL_WORKTIME));
		doc.put(OpDataConstants.MONGO_WORKTIME, counter.getWorkTime());
		doc.put(OpDataConstants.MONGO_TOTAL_IDLE_TIME, data.get(OpDataConstants.OPDATA_TOTAL_IDLE_TIME));
		doc.put(OpDataConstants.MONGO_IDLETIME, counter.getIdleTime());
		doc.put(OpDataConstants.MONGO_TOTAL_FUEL_CONSUME, data.get(OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION));
		doc.put(OpDataConstants.MONGO_TOTAL_IDLE_FUEL_CONSUME,
				data.get(OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION));
		doc.put(OpDataConstants.MONGO_FUEL_COMSUME, counter.getFuelConsume());
		doc.put(OpDataConstants.MONGO_UPDATE_TIME, data.get(OpDataConstants.OPDATA_RECEIVE_TIME));
		doc.put(OpDataConstants.MONGO_IDLE_FUEL_CONSUME, counter.getIdleFuelConsume());

		doc.put(OpDataConstants.MONGO_WORKTIME_BASE, counterBase.getWorkTime());
		doc.put(OpDataConstants.MONGO_IDLETIME_BASE, counterBase.getIdleTime());
		doc.put(OpDataConstants.MONGO_FUEL_CONSUME_BASE, counterBase.getFuelConsume());
		doc.put(OpDataConstants.MONGO_IDLE_FUEL_CONSUME_BASE, counterBase.getIdleFuelConsume());
		doc.put(OpDataConstants.MONGO_LONGITUDE, data.get(OpDataConstants.OPDATA_LONGITUDE));
		doc.put(OpDataConstants.MONGO_LATITUDE, data.get(OpDataConstants.OPDATA_LATITUDE));
		doc.put(OpDataConstants.MONGO_IS_WORK, counter.getWorkTime() >= 1.0f ? 1 : 0);
		doc.put(OpDataConstants.MONGO_WORKTIME_START_OPERATION,
				counter.getWorkTime() >= 1.0f ? counter.getWorkTime() : 0.0f);
		doc.put(OpDataConstants.MONGO_IS_ONLINE, 1);
		doc.put(OpDataConstants.MONGO_NOT_LOGIN_HOURS, 0.0f);
		doc.put(OpDataConstants.MONGO_NOT_LOGIN_DAYS, 0.0f);

		floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_FUEL_LEVEL, Float.class);
		if (floatValue != null) {
			doc.put(OpDataConstants.MONGO_FUEL_LEVEL, floatValue);
		}

		floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_ENGINE_SPEED, Float.class);
		if (floatValue != null) {
			doc.put(OpDataConstants.MONGO_ENGINE_SPEED, floatValue);

		}

		intValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_LOCK_LEVEL, Integer.class);
		if (intValue != null) {
			doc.put(OpDataConstants.MONGO_LOCK_LEVEL, intValue);
		}

		floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_LOCK_TIME_LEFT, Float.class);
		if (floatValue != null) {
			doc.put(OpDataConstants.MONGO_LOCK_TIME_LEFT, floatValue);
		}

		doc.put(OpDataConstants.MONGO_IS_STATISTICS,
				(counter.getWorkTime() < 0.0f || counter.getWorkTime() > 24.0f) ? 0 : 1);

		doc.put(OpDataConstants.MONGO_IS_LOCK_FUTILE, isLockFutile ? 1 : 0);

		long receive_time = ((Date) data.get(OpDataConstants.OPDATA_RECEIVE_TIME_LOCAL)).getTime();

		if (receive_time >= startTimeOfAM && receive_time <= endTimeOfAM) {
			workTimeAM = counter.getWorkTime();

			if (workTimeAM < 0.0f || workTimeAM > 12.0f) {
				workTimeAM = 0.0f;
			}
		}

		doc.put(OpDataConstants.MONGO_AM_WORKTIME, workTimeAM);

		workTimeLineTotal = 0.0f;

		if (timeLine.size() > 0) {
			List<Document> timeline = new ArrayList<>();
			Document docTime = null;

			long timeLineTotal = 0L;
			int validCount = 0;

			Date beginTime;
			Date endTime;

			long beginTimeMS = 0;
			long endTimeMS = 0;
			long timeDiff = 0;

			Calendar cal = Calendar.getInstance();

			Iterator<Date> iter = timeLine.iterator();// have to use iterator,not "pop" since it's referenced
			while (iter.hasNext()) {

				beginTime = iter.next();
				endTime = iter.next();

				if (!beginTime.after(endTime)) {
					docTime = new Document();
					docTime.put(OpDataConstants.MONGO_WORK_TIMELINE_BEGIN, OpDataUtil.formatHHMM(beginTime));
					docTime.put(OpDataConstants.MONGO_WORK_TIMELINE_END, OpDataUtil.formatHHMM(endTime));
					timeline.add(docTime);

					cal.setTime(beginTime);
					cal.set(Calendar.SECOND, 0);
					cal.set(Calendar.MILLISECOND, 0);
					beginTimeMS = cal.getTimeInMillis();

					cal.setTime(endTime);
					cal.add(Calendar.MINUTE, 1);
					cal.set(Calendar.SECOND, 0);
					cal.set(Calendar.MILLISECOND, 0);
					endTimeMS = cal.getTimeInMillis();

					timeDiff = endTimeMS - beginTimeMS;

					timeLineTotal += timeDiff;

					++validCount;
				}
			}

			workTimeLineTotal = (float) (timeLineTotal * 0.001) / 3600;

			if (validCount > 0) {
				doc.put(OpDataConstants.MONGO_WORK_TIMELINE, timeline);
			}
		}

		doc.put(OpDataConstants.MONGO_WORK_TIMELINE_TOTAL, workTimeLineTotal);

		doc.put(OpDataConstants.MONGO_TIME_ZONE, this.timeZone);

		doc.put(OpDataConstants.MONGO_TOTAL_WORKTIME_JUMP_UP, countTWTJumpUp);
		doc.put(OpDataConstants.MONGO_TOTAL_WORKTIME_JUMP_DOWN, countTWTJumpDown);

		if (!mongoDocuments.isEmpty() && null == mongoDocuments.lastElement()) {
			createTime = Calendar.getInstance().getTime();
		}

		if (createTime != null) {
			doc.put(OpDataConstants.MONGO_CREATE_TIME, createTime);
		}

		doc.put(OpDataConstants.MONGO_DAY_FUEL_LEVEL_START, this.fuelLevelState.getDayFuelLevelStart());
		doc.put(OpDataConstants.MONGO_DAY_FUEL_LEVEL_END, this.fuelLevelState.getDayFuelLevelEnd());
		doc.put(OpDataConstants.MONGO_DAY_FUEL_CHARGING, this.fuelLevelState.getDayFuelCharging());

		floatValue = dayMaxState.getMaxHydraulicOilTemperature();
		if (floatValue != null) {
			doc.put(OpDataConstants.MONGO_MAX_HYDRAULIC_OIL_TEMPERATURE, floatValue);
		}

		floatValue = dayMaxState.getMaxCoolingWaterTemperature();
		if (floatValue != null) {
			doc.put(OpDataConstants.MONGO_MAX_COOLING_WATER_TEMPERATURE, floatValue);
		}

		Bson filter = Filters.eq(OpDataConstants.MONGO_LOGIN_ID, data.get(OpDataConstants.OPDATA_LOGIN_ID));
		filter = Filters.and(filter, Filters.eq(OpDataConstants.MONGO_REPORT_TIME, reportTime));

		if (!mongoDocuments.isEmpty()) {
			mongoDocuments.pop();
		}

		mongoDocuments
				.push(new UpdateOneModel<Document>(filter, new Document().append("$set", doc), MONGO_UPDATE_OPTIONS));
	}*/

    private void generateDocument(BoEntity data) {

        Document doc = new Document();
        Integer intValue;
        Float floatValue;

        // 以下4个字段值一天内只增不减， 每天开始的数据如果<=0.0则保留前一天的最后值,
        // 直到有 > 0.0的数据到达， 以第一条有效数据为基准开始一天的统计
        floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_WORKTIME, Float.class);
        if (null == floatValue) {
            data.put(OpDataConstants.OPDATA_TOTAL_WORKTIME, 0.0f);
        } else {
            if (floatValue < lastCounter.getWorkTime()) {
                data.put(OpDataConstants.OPDATA_TOTAL_WORKTIME, lastCounter.getWorkTime());
            }
        }

        floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_IDLE_TIME, Float.class);
        if (null == floatValue) {
            data.put(OpDataConstants.OPDATA_TOTAL_IDLE_TIME, 0.0f);
        } else {
            if (floatValue < lastCounter.getIdleTime()) {
                data.put(OpDataConstants.OPDATA_TOTAL_IDLE_TIME, lastCounter.getIdleTime());
            }
        }

        floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION, Float.class);
        if (null == floatValue) {
            data.put(OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION, 0.0f);
        } else {
            if (floatValue < lastCounter.getFuelConsume()) {
                data.put(OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION, lastCounter.getFuelConsume());
            }
        }

        floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION, Float.class);
        if (null == floatValue) {
            data.put(OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION, 0.0f);
        } else {
            if (floatValue < lastCounter.getIdleFuelConsume()) {
                data.put(OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION, lastCounter.getIdleFuelConsume());
            }
        }

        doc.put(OpDataConstants.MONGO_MACHINE_PROVIDER_CODE, "1");
        doc.put(OpDataConstants.MONGO_DATA_VERSION, data.get(OpDataConstants.OPDATA_DATA_VERSION));
        doc.put(OpDataConstants.MONGO_TOTAL_WORKTIME, data.get(OpDataConstants.OPDATA_TOTAL_WORKTIME));
        doc.put(OpDataConstants.MONGO_WORKTIME, counter.getWorkTime());
        doc.put(OpDataConstants.MONGO_TOTAL_IDLE_TIME, data.get(OpDataConstants.OPDATA_TOTAL_IDLE_TIME));
        doc.put(OpDataConstants.MONGO_IDLETIME, counter.getIdleTime());
        doc.put(OpDataConstants.MONGO_TOTAL_FUEL_CONSUME, data.get(OpDataConstants.OPDATA_TOTAL_FUEL_CONSUMPTION));
        doc.put(OpDataConstants.MONGO_TOTAL_IDLE_FUEL_CONSUME,
                data.get(OpDataConstants.OPDATA_TOTAL_IDLE_FUEL_CONSUMPTION));
        doc.put(OpDataConstants.MONGO_FUEL_COMSUME, counter.getFuelConsume());
        doc.put(OpDataConstants.MONGO_UPDATE_TIME, data.get(OpDataConstants.OPDATA_RECEIVE_TIME));
        doc.put(OpDataConstants.MONGO_IDLE_FUEL_CONSUME, counter.getIdleFuelConsume());

        doc.put(OpDataConstants.MONGO_WORKTIME_BASE, counterBase.getWorkTime());
        doc.put(OpDataConstants.MONGO_IDLETIME_BASE, counterBase.getIdleTime());
        doc.put(OpDataConstants.MONGO_FUEL_CONSUME_BASE, counterBase.getFuelConsume());
        doc.put(OpDataConstants.MONGO_IDLE_FUEL_CONSUME_BASE, counterBase.getIdleFuelConsume());
        doc.put(OpDataConstants.MONGO_LONGITUDE, data.get(OpDataConstants.OPDATA_LONGITUDE));
        doc.put(OpDataConstants.MONGO_LATITUDE, data.get(OpDataConstants.OPDATA_LATITUDE));
        doc.put(OpDataConstants.MONGO_IS_WORK, counter.getWorkTime() >= 1.0f ? 1 : 0);
        doc.put(OpDataConstants.MONGO_WORKTIME_START_OPERATION,
                counter.getWorkTime() >= 1.0f ? counter.getWorkTime() : 0.0f);
        doc.put(OpDataConstants.MONGO_IS_ONLINE, 1);
        doc.put(OpDataConstants.MONGO_NOT_LOGIN_HOURS, 0.0f);
        doc.put(OpDataConstants.MONGO_NOT_LOGIN_DAYS, 0.0f);

        floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_FUEL_LEVEL, Float.class);
        if (floatValue != null) {
            doc.put(OpDataConstants.MONGO_FUEL_LEVEL, floatValue);
        }

        floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_ENGINE_SPEED, Float.class);
        if (floatValue != null) {
            doc.put(OpDataConstants.MONGO_ENGINE_SPEED, floatValue);

        }

        intValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_LOCK_LEVEL, Integer.class);
        if (intValue != null) {
            doc.put(OpDataConstants.MONGO_LOCK_LEVEL, intValue);
        }

        floatValue = OpDataUtil.getColumnValue(data, OpDataConstants.OPDATA_LOCK_TIME_LEFT, Float.class);
        if (floatValue != null) {
            doc.put(OpDataConstants.MONGO_LOCK_TIME_LEFT, floatValue);
        }

        doc.put(OpDataConstants.MONGO_IS_STATISTICS,
                (counter.getWorkTime() < 0.0f || counter.getWorkTime() > 24.0f) ? 0 : 1);

        doc.put(OpDataConstants.MONGO_IS_LOCK_FUTILE, isLockFutile ? 1 : 0);

        long receive_time = ((Date) data.get(OpDataConstants.OPDATA_RECEIVE_TIME_LOCAL)).getTime();

        if (receive_time >= startTimeOfAM && receive_time <= endTimeOfAM) {
            workTimeAM = counter.getWorkTime();

            if (workTimeAM < 0.0f || workTimeAM > 12.0f) {
                workTimeAM = 0.0f;
            }
        }

        doc.put(OpDataConstants.MONGO_AM_WORKTIME, workTimeAM);

        workTimeLineTotal = 0.0f;

        if (timeLine.size() > 0) {
            List<Document> timeline = new ArrayList<>();
            Document docTime = null;

            long timeLineTotal = 0L;
            int validCount = 0;

            Date beginTime;
            Date endTime;

            long beginTimeMS = 0;
            long endTimeMS = 0;
            long timeDiff = 0;

            Calendar cal = Calendar.getInstance();

            Iterator<Date> iter = timeLine.iterator();// have to use iterator,not "pop" since it's referenced
            while (iter.hasNext()) {

                beginTime = iter.next();
                endTime = iter.next();

                if (!beginTime.after(endTime)) {
                    docTime = new Document();
                    docTime.put(OpDataConstants.MONGO_WORK_TIMELINE_BEGIN, OpDataUtil.formatHHMM(beginTime));
                    docTime.put(OpDataConstants.MONGO_WORK_TIMELINE_END, OpDataUtil.formatHHMM(endTime));
                    timeline.add(docTime);

                    cal.setTime(beginTime);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    beginTimeMS = cal.getTimeInMillis();

                    cal.setTime(endTime);
                    cal.add(Calendar.MINUTE, 1);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    endTimeMS = cal.getTimeInMillis();

                    timeDiff = endTimeMS - beginTimeMS;

                    timeLineTotal += timeDiff;

                    ++validCount;
                }
            }

            workTimeLineTotal = (float) (timeLineTotal * 0.001) / 3600;

            if (validCount > 0) {
                doc.put(OpDataConstants.MONGO_WORK_TIMELINE, timeline);
            }
        }

        /*新增怠速时间段解析入mongo*/
        if (idleTimeLine.size() > 0) {
            List<Document> timeline = new ArrayList<>();
            Document docTime = null;

            long timeLineTotal = 0L;
            int validCount = 0;

            Date beginTime;
            Date endTime;

            long beginTimeMS = 0;
            long endTimeMS = 0;
            long timeDiff = 0;

            Calendar cal = Calendar.getInstance();

            Iterator<Date> iter = idleTimeLine.iterator();
            while (iter.hasNext()) {

                beginTime = iter.next();
                endTime = iter.next();

                if (!beginTime.after(endTime)) {
                    docTime = new Document();
                    docTime.put(OpDataConstants.MONGO_WORK_TIMELINE_BEGIN, OpDataUtil.formatHHMM(beginTime));
                    docTime.put(OpDataConstants.MONGO_WORK_TIMELINE_END, OpDataUtil.formatHHMM(endTime));
                    timeline.add(docTime);

                    cal.setTime(beginTime);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    beginTimeMS = cal.getTimeInMillis();

                    cal.setTime(endTime);
                    cal.add(Calendar.MINUTE, 1);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    endTimeMS = cal.getTimeInMillis();

                    timeDiff = endTimeMS - beginTimeMS;

                    timeLineTotal += timeDiff;

                    ++validCount;
                }
            }

            if (validCount > 0) {
                doc.put(OpDataConstants.MONGO_IDLE_TIMELINE, timeline);
            }
        }
        /*lastIdlingUpdateTime字段*/
        doc.put(OpDataConstants.MONGO_IDLING_UPDATETIME, lastIdlingUpdateTime);

        doc.put(OpDataConstants.MONGO_WORK_TIMELINE_TOTAL, workTimeLineTotal);

        doc.put(OpDataConstants.MONGO_TIME_ZONE, this.timeZone);

        doc.put(OpDataConstants.MONGO_TOTAL_WORKTIME_JUMP_UP, countTWTJumpUp);
        doc.put(OpDataConstants.MONGO_TOTAL_WORKTIME_JUMP_DOWN, countTWTJumpDown);

        if (!mongoDocuments.isEmpty() && null == mongoDocuments.lastElement()) {
            createTime = Calendar.getInstance().getTime();
        }

        if (createTime != null) {
            doc.put(OpDataConstants.MONGO_CREATE_TIME, createTime);
        }

        doc.put(OpDataConstants.MONGO_DAY_FUEL_LEVEL_START, this.fuelLevelState.getDayFuelLevelStart());
        doc.put(OpDataConstants.MONGO_DAY_FUEL_LEVEL_END, this.fuelLevelState.getDayFuelLevelEnd());
        doc.put(OpDataConstants.MONGO_DAY_FUEL_CHARGING, this.fuelLevelState.getDayFuelCharging());

        floatValue = dayMaxState.getMaxHydraulicOilTemperature();
        if (floatValue != null) {
            doc.put(OpDataConstants.MONGO_MAX_HYDRAULIC_OIL_TEMPERATURE, floatValue);
        }

        floatValue = dayMaxState.getMaxCoolingWaterTemperature();
        if (floatValue != null) {
            doc.put(OpDataConstants.MONGO_MAX_COOLING_WATER_TEMPERATURE, floatValue);
        }

        Bson filter = Filters.eq(OpDataConstants.MONGO_LOGIN_ID, data.get(OpDataConstants.OPDATA_LOGIN_ID));
        filter = Filters.and(filter, Filters.eq(OpDataConstants.MONGO_REPORT_TIME, reportTime));

        if (!mongoDocuments.isEmpty()) {
            mongoDocuments.pop();
        }

        mongoDocuments
                .push(new UpdateOneModel<Document>(filter, new Document().append("$set", doc), MONGO_UPDATE_OPTIONS));
    }

    public void setLastValidLongitude(float lastValidLongitude) {
        this.lastValidLongitude = lastValidLongitude;
    }

    public void setLastValidLatitude(float lastValidLatitude) {
        this.lastValidLatitude = lastValidLatitude;
    }

    public RealTimeStateFuelLevel getFuelLevelState() {
        return fuelLevelState;
    }

    public RealTimeStateDayMax getDayMaxState() {
        return dayMaxState;
    }

    public Float getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(Float timeZone) {
        this.timeZone = timeZone;
    }

}
