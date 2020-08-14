package com.sany.evi.data.kafka.producer;

import cn.sany.evi.gateway.data.bean.RealDataEntity.RealDataInfoEntity;
import cn.sany.evi.gateway.data.entity.frame.FcpType;
import cn.sany.evi.gateway.data.entity.frame.TransportDataType;
import cn.sany.evi.gateway.data.utils.ProtoBufConvertUtils;
import com.google.gson.Gson;
import com.opencsv.bean.CsvToBeanBuilder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.sany.evi.data.alarm.AlarmClear;
import com.sany.evi.data.alarm.DeviceAlarm;
import com.sany.evi.opdata.OpDataTranslator;
import com.sany.evi.opdata.OpDataTranslatorLocal;
import com.sany.evi.opdata.enums.EnumRuleCategory;
import com.sany.evi.opdata.listener.ProtocolsRuntimeListener;
import com.sany.evi.opdata.listener.event.ProtocolsRuntimeEvent;
import com.sany.evi.opdata.protocol.runtime.ProtocolsRuntime;
import com.sany.evi.opdata.translate.CRC16;
import com.witsight.platform.model.BoEntity;
import com.witsight.platform.util.io.IOUtil;
import com.witsight.platform.util.lang.StringUtil;
import com.witsight.platform.util.tools.JsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.awt.geom.Point2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * Hello world!//
 */
public class App2 {
    private final Producer<String, BoEntity> producer;
    private final Producer<String, RealDataInfoEntity> producerRaw;
    private final Producer<String, String> producerAlarm;
    public final static String TOPIC = "topic-tangj";
    public final static String TOPIC_ALARM = "alarm-test-tj";

    private App2() {
        Properties props = new Properties();
//		props.put("bootstrap.servers", "10.11.192.191:9092");
        props.put("bootstrap.servers", "10.100.0.235:1025");
        props.put("acks", "1");
        props.put("retries", 1);
        props.put("batch.size", 5);
        props.put("linger.ms", 10 * 1000);
        props.put("buffer.memory", 1024 * 1024);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", MyEncoder.class.getName());

        Properties propsRaw = new Properties();
        propsRaw.put("bootstrap.servers", "10.11.192.191:9092");
        propsRaw.put("acks", "1");
        propsRaw.put("retries", 1);
        propsRaw.put("batch.size", 5);
        propsRaw.put("linger.ms", 10 * 1000);
        propsRaw.put("buffer.memory", 1024 * 1024);
        propsRaw.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propsRaw.put("value.serializer", MyEncoderRaw.class.getName());

        Properties propsAlarm = new Properties();
        propsAlarm.put("bootstrap.servers", "10.11.192.191:9092");
        propsAlarm.put("acks", "1");
        propsAlarm.put("retries", 1);
        propsAlarm.put("batch.size", 5);
        propsAlarm.put("linger.ms", 10 * 1000);
        propsAlarm.put("buffer.memory", 1024 * 1024);
        propsAlarm.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propsAlarm.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
        producerRaw = new KafkaProducer<>(propsRaw);
        producerAlarm = new KafkaProducer<>(propsAlarm);
    }

    void produce() throws InterruptedException{
        float interval = 0.0166665f;
        float idle = 0.0f;
        List<Float> times = new ArrayList<>();
        times.add(0.0f);
        times.add(0.0f);
        times.add(0.0f);
        times.add(1.0f);
        times.add(1.0f);
        String id = UUID.randomUUID().toString().substring(0, 5);
        String topic1 = "dl-test-01";
        List idles = new ArrayList<Float>();
        String normal5min = "====++++";
        String normal10min = "==++++===";

        String testmode = normal5min;
//        String loginId = topic1 + "-" + id;
        String loginId = "dl-test-01-5b45a";
        while (true) {

            for (char c : testmode.toCharArray()) {
                if (c == '=') {
                    idles.add("#=");
                    for (int i = 0; i <= 5; i++) {

                        idles.add(idle);
                        producer.send(new ProducerRecord<>(topic1, "key", getTranslatedData(loginId, idle)));
                        System.out.println(loginId+ "  = " + String.valueOf(idle));
                        System.out.println(testmode);
                        System.out.println(idles);
                        Thread.sleep(10000);
                    }
                }
                else if(c == '+') {
                    idle += interval;
                    idles.add("#+");
                    for (int i = 0; i <= 5; i++) {
                        idles.add(idle);
                        producer.send(new ProducerRecord<>(topic1, "key", getTranslatedData(loginId, idle)));
                        System.out.println(loginId + "  = " + String.valueOf(idle));
                        System.out.println(testmode);
                        System.out.println(idles);
                        Thread.sleep(10000);
                    }
                }
            }
            System.out.println("end");

            Thread.sleep(80000000);
        }
    }

    static OpDataTranslator translator = null;

    {
        try {
            List<String> lines = IOUtil.readLines(new FileReader(new File("c:/protocol_test.json")));
            StringBuilder sb = new StringBuilder();
            for (String line : lines) {
                sb.append(line);
            }
            ProtocolsRuntime protocols = ProtocolsRuntime.load(sb.toString());

            new ProtocolsRuntimeListener().onApplicationEvent(new ProtocolsRuntimeEvent(protocols));
            translator = OpDataTranslatorLocal.get();
            System.out.println(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static BoEntity getTranslatedDataWithRule() {
        BoEntity result = null;
        try {
            if (translator.translate(getData())) {
                result = translator.getOpData();
                result = translator.getRealDataByFiltered(new EnumRuleCategory[]{ /* EnumRuleCategory.BLACK_LIST, */
                        EnumRuleCategory.FILTER, EnumRuleCategory.DISPLAY});
                result = (BoEntity) result.get(EnumRuleCategory.FILTER.name());

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    private static List<String> getAlarm() {
        BoEntity result = null;
        List<String> alarms = null;
        try {
            if (translator.translate(getDataV18("1", 999f))) {
                result = translator.getOpData();
                result = translator.getRealDataByFiltered(new EnumRuleCategory[]{EnumRuleCategory.BLACK_LIST,
                        EnumRuleCategory.FILTER, EnumRuleCategory.ALARM});
                alarms = (List<String>) result.get(EnumRuleCategory.ALARM.name());

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return alarms;
    }

    private static BoEntity getTranslatedData(String id, Float idle) {
        BoEntity result = null;
        try {
            if (translator.translate(getDataV129(id, idle))) {
                result = translator.getRealDataByFiltered(new EnumRuleCategory[]{EnumRuleCategory.BLACK_LIST,
                        EnumRuleCategory.FILTER, EnumRuleCategory.ALARM});
                if (null == result.get(EnumRuleCategory.FILTER.name())) {
                    result = (BoEntity) result.get(EnumRuleCategory.DEFAULT.name());
                } else {
                    result = (BoEntity) result.get(EnumRuleCategory.FILTER.name());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

//		result.put("display_local_date", new Date());		
//
//		result.put("login_id", "ST01019000007");
//		result.put("serial_no", "ABCDEF123456");
        return result;
    }

    private static BoEntity getTranslatedData2() {
        BoEntity result = null;
        try {
            if (translator.translate(getDataV245_2())) {
                result = translator.getRealDataByFiltered(new EnumRuleCategory[]{EnumRuleCategory.FILTER});
                result = (BoEntity) result.get(EnumRuleCategory.FILTER.name());
                result.put("login_id", "ST01019000004");
                result.put("serial_no", "ABCDEF123456");
                System.out.println("receive time = " + result.get("receive_time"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

//	void produce() {
//		while (true) {
//			try {
//				Thread.sleep(2000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//
//			BoEntity data;
//			data = getTranslatedData("");
//			producer.send(new ProducerRecord<>("dl-realtime-test1", "100000001", data));
////			 data = getTranslatedData2();
////			 producer.send(new ProducerRecord<>(TOPIC, "100000001", data));
//		}
//
//		// System.out.println("Done");
//
//	}


    static void produceMQ() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("10.11.192.191");
        factory.setPort(5672);

        try {
            Connection upConnection = factory.newConnection();
            Channel channelUp = upConnection.createChannel();

            AlarmClear clear = new AlarmClear();
            clear.setLoginId("1000000001");
            clear.setAlarmCode("1000000001");

            channelUp.basicPublish("alarm_clear.topic", "alarm.clear", true, MessageProperties.TEXT_PLAIN, JsonUtil.toJsonWithType(clear).getBytes());
            System.out.println("done!!!");
        } catch (IOException | TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    void produceRaw() {
        while (true) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            producerRaw.send(new ProducerRecord<>(TOPIC, "100000001", getDataV7()));
            System.out.println("send 1 record to kafka topic:" + TOPIC);
        }

        // System.out.println("Done");

    }

    void produceAlarm() {

        String loginId = "101010101";

        DeviceAlarm da = new DeviceAlarm();
        da.setAlarmCode("A007");
        da.setDeviceId(loginId);
        da.setSerialno("S0000001");

        int count = 0;
        while (true) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            // producerAlarm.send(new ProducerRecord<>(TOPIC_ALARM, loginId,JsonUtil.toJson(da)));
            // if (true)
            // continue;
            String code = "A";
            ++count;
            da.setAlarmCode(code + count);
            da.setStartTime(new Date());
            producerAlarm.send(new ProducerRecord<>(TOPIC_ALARM, loginId, JsonUtil.toJson(da)));
            da.setStartTime(new Date());
            producerAlarm.send(new ProducerRecord<>(TOPIC_ALARM, loginId, JsonUtil.toJson(da)));
            da.setStartTime(new Date());
            producerAlarm.send(new ProducerRecord<>(TOPIC_ALARM, loginId, JsonUtil.toJson(da)));

        }

        // System.out.println("Done");

    }

    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        new App2().produce();
//		 new App2().produceRaw();
//		 new App2().produceAlarm();
//		 new App2().produceFromCsv();
//		produceMQ();
//		testMisc();
    }

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

    @SuppressWarnings("unchecked")
    public static <T> T getColumnValue(BoEntity data, String name, Class<T> t) {
        if (validateColumn(data, name, t)) {
            return (T) data.get(name);
        }

        return null;
    }

    private static void testMisc() {

//		double[][] test = new double[][] {{1,2},{3,4}};
//		System.out.println(JsonUtil.toJsonWithType(test));

//		int x = 521909879;
//		System.out.println(String.format("%x", x));
//		float aaa = Float.intBitsToFloat(x);
//		System.out.println(String.format("%x", Float.floatToIntBits(aaa)));
        checkLnL();
    }

    static int flag = 0;

    static float fuelLevel = 20.0f;

    public static RealDataInfoEntity getDataV10(String loginId) {
        byte[] data = new byte[3 + 1 + 1 + 32 * 2 + 32 * 4 + 2];

        String id = loginId;

        data[0] = 0x02;
        data[2] = 0x04;
        data[3] = 10;
        data[4] = 32;

        int vInt;

        int offset;
        int slot;
        int index;

        // int
        offset = 5;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // float
        offset += data[4] * 2;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);
            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        fuelLevel += 0.1f;

        // float
        // slot 6 total_worktime speed
        slot = 6;
        index = offset + (slot - 1) * 4;

        vInt = Float.floatToIntBits(fuelLevel);
        data[index] = (byte) (0xff & vInt);
        data[++index] = (byte) (0xff & (vInt >>> 8));
        data[++index] = (byte) (0xff & (vInt >>> 16));
        data[++index] = (byte) (0xff & (vInt >>> 24));

        // float
        // slot 9 engine speed
        slot = 9;
        index = offset + (slot - 1) * 4;

        vInt = Float.floatToIntBits(800.1f);
        data[index] = (byte) (0xff & vInt);
        data[++index] = (byte) (0xff & (vInt >>> 8));
        data[++index] = (byte) (0xff & (vInt >>> 16));
        data[++index] = (byte) (0xff & (vInt >>> 24));

        // slot 10 fuel level
        slot = 10;
        index = offset + (slot - 1) * 4;

        vInt = Float.floatToIntBits(fuelLevel);
        data[index] = (byte) (0xff & vInt);
        data[++index] = (byte) (0xff & (vInt >>> 8));
        data[++index] = (byte) (0xff & (vInt >>> 16));
        data[++index] = (byte) (0xff & (vInt >>> 24));

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.DAY_OF_MONTH, 1);
        // cal.add(Calendar.HOUR_OF_DAY, 1);

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    public static RealDataInfoEntity getDataV17() {
        byte[] data = new byte[3 + 1 + 1 + 32 * 2 + 32 * 4 + 2];

        String id = "100000003";

        data[0] = 0x02;
        data[2] = 0x04;
        data[3] = 17;
        data[4] = 32;

        int vInt;

        int offset;
        int slot;
        int index;

        // int
        offset = 5;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // float
        offset += data[4] * 2;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);
            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        // float
        // slot 15 machine timestap
        slot = 15;
        index = offset + (slot - 1) * 4;
        vInt = (int) ((Calendar.getInstance().getTimeInMillis() - TimeZone.getDefault().getRawOffset()) * 0.001);
        data[index] = (byte) (0xff & vInt);
        data[++index] = (byte) (0xff & (vInt >>> 8));
        data[++index] = (byte) (0xff & (vInt >>> 16));
        data[++index] = (byte) (0xff & (vInt >>> 24));

        // float
        // slot 25 longitude
        slot = 25;
        index = offset + (slot - 1) * 4;
        vInt = Float.floatToIntBits(121.61f);
        // vInt = Float.floatToIntBits(119.6333158036f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 26 latitde
        slot = 26;
        index = offset + (slot - 1) * 4;
        vInt = Float.floatToIntBits(31.21f);
        // vInt = Float.floatToIntBits(23.5701375704f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 31 mask1
        slot = 31;
        index = offset + (slot - 1) * 4;
        vInt = -1;
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 32 mask2
        slot = 32;
        index = offset + (slot - 1) * 4;
        vInt = -1;
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, -1);

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    static float twt = 999.0f;
    static float tit = 999.0f;

    public static RealDataInfoEntity getDataV18(String id, Float idle) {
        byte[] data = new byte[3 + 1 + 1 + 32 * 2 + 32 * 4 + 2];

//		String id = "ST01019000001";
//		List<String> ids = new ArrayList<>();
//		ids.add("id_031");
//		ids.add("id_031");
//		ids.add("id_031");
//
//		int in = (int)(Math.random() * (4 - 1 ));
//		String id = ids.get(in);
        data[0] = 0x02;
        data[2] = 0x04;
        data[3] = 18;
        data[4] = 32;

        int vInt = 0;

        int offset;
        int slot;
        int index;

        // int
        offset = 5;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // slot 9
        slot = 9;
        index = offset + (slot - 1) * 2;

        vInt = 4013;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // float
        offset += data[4] * 2;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);
//			if (6 == i) {
//				vInt = Float.floatToIntBits(twt += 0.1f);
//			}
            if (7 == i) {
                vInt = Float.floatToIntBits(0.0f);
            }
            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        // float
        // slot 1 engine speed
        slot = 1;
        index = offset + (slot - 1) * 4;
        vInt = Float.floatToIntBits(999.0f);
        data[index] = (byte) (0xff & vInt);
        data[++index] = (byte) (0xff & (vInt >>> 8));
        data[++index] = (byte) (0xff & (vInt >>> 16));
        data[++index] = (byte) (0xff & (vInt >>> 24));

        //8 total_idle_time
        slot = 8;
        index = offset + (slot - 1) * 4;
        // vInt = Float.floatToIntBits(120.96f);
        vInt = Float.floatToIntBits(idle);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 15 machine timestap
        slot = 15;
        index = offset + (slot - 1) * 4;
        vInt = (int) ((Calendar.getInstance().getTimeInMillis() - TimeZone.getDefault().getRawOffset()) * 0.001);
        data[index] = (byte) (0xff & vInt);
        data[++index] = (byte) (0xff & (vInt >>> 8));
        data[++index] = (byte) (0xff & (vInt >>> 16));
        data[++index] = (byte) (0xff & (vInt >>> 24));

        // float
        // slot 25 longitude
        slot = 25;
        index = offset + (slot - 1) * 4;
        // vInt = Float.floatToIntBits(120.96f);
        vInt = Float.floatToIntBits(118.34473f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 26 latitde
        slot = 26;
        index = offset + (slot - 1) * 4;
        vInt = Float.floatToIntBits(32.160057f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 31 mask1
        slot = 31;
        index = offset + (slot - 1) * 4;
        vInt = -1;
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 32 mask2
        slot = 32;
        index = offset + (slot - 1) * 4;
        vInt = -1;
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, -1);

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    public static RealDataInfoEntity getDataV7() {
        byte[] data = new byte[3 + 1 + 1 + 32 * 2 + 32 * 4 + 2];

        String id = "100000002";

        data[0] = 0x02;
        data[2] = 0x04;
        data[3] = 7;
        data[4] = 32;

        int vInt;

        int offset;
        int slot;
        int index;

        // int
        offset = 5;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // float
        offset += data[4] * 2;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);
            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        // float
        // slot 25 longitude
        slot = 1;
        index = offset + (slot - 1) * 4;
        // vInt = Float.floatToIntBits(120.96f);
        vInt = Float.floatToIntBits(118.96f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 26 latitude
        slot = 2;
        index = offset + (slot - 1) * 4;
        vInt = Float.floatToIntBits(31.97f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, -1);

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    public static RealDataInfoEntity getDataV245() {
        byte[] data = new byte[8 + 32 * 2 + 32 * 4 + 4];

        String id = "ST01019000007";

        data[0] = (byte) 0xa5;
        data[1] = 0x3c;

        int vInt;

        int offset;
        int slot;
        int index;

        // int
        offset = 8;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // float
        offset += 32 * 2;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);
            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        Date now = Calendar.getInstance().getTime();

        // float
        // slot 1 display local time
        slot = 1;
        index = offset + (slot - 1) * 4;
        vInt = ((Double) ((now.getTime()) * 0.001d)).intValue();
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 5 longitude
        slot = 5;
        index = offset + (slot - 1) * 4;
        vInt = Float.floatToIntBits(113.713f);
//		vInt = Float.floatToIntBits(100.0f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 6 latitde
        slot = 6;
        index = offset + (slot - 1) * 4;
//		vInt = Float.floatToIntBits(24.552f);
        vInt = Float.floatToIntBits(30.0f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, -1);

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();

        builder.setDeviceId(id);
        builder.setProtocolVersion(245);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ONE.value());
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    public static RealDataInfoEntity getDataV245_2() {
        byte[] data = new byte[8 + 32 * 2 + 32 * 4 + 4];

        String id = "ST01019000004";

        data[0] = (byte) 0xa5;
        data[1] = 0x3c;

        int vInt;

        int offset;
        int slot;
        int index;

        // int
        offset = 8;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // float
        offset += 32 * 2;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);
            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        Date now = Calendar.getInstance().getTime();

        // float
        // slot 1 display local time
        slot = 1;
        index = offset + (slot - 1) * 4;
        vInt = ((Double) ((now.getTime() - TimeZone.getDefault().getRawOffset()) * 0.001d)).intValue();
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 5 longitude
        slot = 5;
        index = offset + (slot - 1) * 4;
        vInt = Float.floatToIntBits(101.0f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 6 latitde
        slot = 6;
        index = offset + (slot - 1) * 4;
        vInt = Float.floatToIntBits(31.0f);
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, -1);

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();

        builder.setDeviceId(id);
        builder.setProtocolVersion(245);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ONE.value());
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    public static RealDataInfoEntity getDataV11() {
        byte[] data = new byte[3 + 1 + 1 + 32 * 2 + 32 * 4 + 2];

        String id = "100000001";

        data[0] = 0x02;
        data[2] = 0x04;
        data[3] = 11;
        data[4] = 32;

        int vInt;

        int offset;
        int slot;
        int index;

        // int
        offset = 5;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // float
        offset += data[4] * 2;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);
            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        Calendar cal = Calendar.getInstance();

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        RealDataInfoEntity rd = builder.build();

        return rd;
    }

    public static RealDataInfoEntity getDataV13() {
        byte[] data = new byte[3 + 1 + 1 + 32 * 2 + 32 * 4 + 2];

        String id = "100000001";

        data[0] = 0x02;
        data[2] = 0x04;
        data[3] = 13;
        data[4] = 32;

        int vInt;

        int offset;
        int slot;
        int index;

        // int
        offset = 5;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // float
        offset += data[4] * 2;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);
            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        Calendar cal = Calendar.getInstance();

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        RealDataInfoEntity rd = builder.build();

        return rd;
    }

    static int count = 0;

    private static RealDataInfoEntity getDataV14() {
        byte[] data = new byte[3 + 1 + 1 + 32 * 2 + 32 * 4 + 2];

        String id = "100000001";

        data[0] = 0x02;
        data[2] = 0x04;
        data[3] = 14;
        data[4] = 32;

        int vInt = 12345;

        int offset;
        int slot;
        int index;

        offset = 5;

        // int
        // slot 1
        slot = 1;
        index = offset + (slot - 1) * 2;

        vInt = 1;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // int
        // slot 2
        slot = 2;
        index = offset + (slot - 1) * 2;

        vInt = 2;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // int
        // slot 3
        slot = 3;
        index = offset + (slot - 1) * 2;

        vInt = 3;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // int
        // slot 4
        slot = 4;
        index = offset + (slot - 1) * 2;

        vInt = 4;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // slot 5
        slot = 5;
        index = offset + (slot - 1) * 2;

        vInt = 2019;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // slot 6
        slot = 6;
        index = offset + (slot - 1) * 2;

        vInt = 12;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // slot 7
        slot = 7;
        index = offset + (slot - 1) * 2;

        vInt = 31;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // slot 8
        slot = 8;
        index = offset + (slot - 1) * 2;

        vInt = 23;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // slot 9
        slot = 9;
        index = offset + (slot - 1) * 2;

        vInt = 59;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // slot 10
        slot = 10;
        index = offset + (slot - 1) * 2;

        vInt = 19;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        // slot 11
        slot = 11;
        index = offset + (slot - 1) * 2;

        vInt = ++count;
        data[index] = (byte) (vInt & 0xff);
        data[++index] = (byte) ((vInt >> 8) & 0xff);

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();
        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, 10);

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        builder.setProtocolVersion(14);
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    public static RealDataInfoEntity getDataV129(String id, Float idle) {
        int blockSize = 8 + 32 * 2 + 32 * 4 + 4;
        byte[] data = new byte[blockSize * 4];

//        String id = "100000001";
        // a53cc50004002020
        data[0] = (byte) 0xa5;
        data[1] = 0x3c;

        int vInt;
        int offset;
        int slot;
        int index;

        for (int nBlock = 0; nBlock < 4; ++nBlock) {
            //xiugai idle
            if(nBlock == 2){
                // int
                offset = nBlock * 204 + 8;
                for (int i = 0; i < 32; ++i) {
                    index = offset + i * 2;
                    vInt = nBlock * 10000 + 101 + i;
                    data[index] = (byte) (vInt & 0xff);
                    data[++index] = (byte) ((vInt >> 8) & 0xff);
                }

                // float
                offset += 32 * 2;
                for (int i = 0; i < 32; ++i) {
                    if(i == 19 ){
                        index = offset + i * 4;
                        vInt = Float.floatToIntBits(idle);
                        data[index] = (byte) (0xff & vInt);
                        data[++index] = (byte) (0xff & (vInt >>> 8));
                        data[++index] = (byte) (0xff & (vInt >>> 16));
                        data[++index] = (byte) (0xff & (vInt >>> 24));
                    }
                    else{
                        index = offset + i * 4;
                        vInt = Float.floatToIntBits(nBlock * 10000 + 1001.99f + i);
                        data[index] = (byte) (0xff & vInt);
                        data[++index] = (byte) (0xff & (vInt >>> 8));
                        data[++index] = (byte) (0xff & (vInt >>> 16));
                        data[++index] = (byte) (0xff & (vInt >>> 24));
                    }

                }
            }
            else{
                // int
                offset = nBlock * 204 + 8;
                for (int i = 0; i < 32; ++i) {
                    index = offset + i * 2;
                    vInt = nBlock * 10000 + 101 + i;
                    data[index] = (byte) (vInt & 0xff);
                    data[++index] = (byte) ((vInt >> 8) & 0xff);
                }

                // float
                offset += 32 * 2;
                for (int i = 0; i < 32; ++i) {
                    index = offset + i * 4;
                    vInt = Float.floatToIntBits(nBlock * 10000 + 1001.99f + i);
                    data[index] = (byte) (0xff & vInt);
                    data[++index] = (byte) (0xff & (vInt >>> 8));
                    data[++index] = (byte) (0xff & (vInt >>> 16));
                    data[++index] = (byte) (0xff & (vInt >>> 24));
                }
            }

        }

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();
        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, 10);

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ONE.value());
        builder.setProtocolVersion(129);
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    private static RealDataInfoEntity getData() {

        // String data =
        // "a53cc50004002020b80b0f00f0000a000000d007d0073075409c0200d0070000000000000000c80050000500000064000a000100240003000300040005001c001d001e001f0020006666f042cdccf8413333f4423333c84233b3314366e694430000c842000052436606e144cdcc2642000000000000f8419a993141000087429a1905433333f44233338e429a990141cdcc8c3f66660e42003005456686444466667e419ae10a4566065e449a998541666660420000fa433333b0426666ba41e17afa41ae4701427e023ac5a53cc50004012020b80b0f00f0000a000000d007d0073075409c0200d0070000000000000000c80050000500000064000a000100240003000300040005001c001d001e001f0020006666f042cdccf8413333f4423333c84233b3314366e694430000c842000052436606e144cdcc2642000000000000f8419a993141000087429a1905433333f44233338e429a990141cdcc8c3f66660e42003005456686444466667e419ae10a4566065e449a998541666660420000fa433333b0426666ba41e17afa41ae470142b0203ac5a53cc50004022020b80b0f00f0000a000000d007d0073075409c0200d0070000000000000000c80050000500000064000a000100240003000300040005001c001d001e001f0020006666f042cdccf8413333f4423333c84233b3314366e694430000c842000052436606e144cdcc2642000000000000f8419a993141000087429a1905433333f44233338e429a990141cdcc8c3f66660e42003005456686444466667e419ae10a4566065e449a998541666660420000fa433333b0426666ba41e17afa41ae470142e2473ac5a53cc50004032020b80b0f00f0000a000000d007d0073075409c0200d0070000000000000000c80050000500000064000a000100240003000300040005001c001d001e001f0020006666f042cdccf8413333f4423333c84233b3314366e694430000c842000052436606e144cdcc2642000000000000f8419a993141000087429a1905433333f44233338e429a990141cdcc8c3f66660e42003005456686444466667e419ae10a4566065e449a998541666660420000fa433333b0426666ba41e17afa41ae4701422c653ac5";
        // String data =
        // "02c50407200100521f00000100bd0bf2041027000000000100000084fc0400000000000400000018180001000000000000000400000000000000000000000000000b000000f813e4423d9bb2410000d242000000000000000000409f440000484200000000000000000000be42cdcc34c100002c42caff93420000807f00e00a46bb1a0141000000000000000083491a4200000000000040400000000000000000000000000000000014f01c408f191e40d3cccc3dd0cc4cbe0000000000000080000000804678";

        // V13
        // String data =
        // "02c5040d20000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000304100003041cdcccc3d00000000cdcccc3dcdcccc3d000048440000000000808944cdcccc3d00000000000000000000803f000000000000000000000000000000000000000000000000000000000000000000000000cdcccc3d0000000000000000000000000000000000000000000000000000000000000000000000000208";
        // data =
        // "02c5040d20000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000204100002041ec51b83d00000000ec51b83dec51b83d000048440000000000808944ec51b83d00000000000000006666663f000000000000000000000000000000000000000000000000000000000000000000000000ec51b83d000000000000000000000000000000000000000000000000000000000000000000000000d90e";

        // String data =
        // "a53cc50004002020b80b0f00f0000a000000d007d0073075409c0200d0070000000000000000c80050000500000064000a000100240003000300040005001c001d001e001f0020006666f042cdccf8413333f4423333c84233b3314366e694430000c842000052436606e144cdcc2642000000000000f8419a993141000087429a1905433333f44233338e429a990141cdcc8c3f66660e42003005456686444466667e419ae10a4566065e449a998541666660420000fa433333b0426666ba41e17afa41ae4701427e023ac5a53cc50004012020b80b0f00f0000a000000d007d0073075409c0200d0070000000000000000c80050000500000064000a000100240003000300040005001c001d001e001f0020006666f042cdccf8413333f4423333c84233b3314366e694430000c842000052436606e144cdcc2642000000000000f8419a993141000087429a1905433333f44233338e429a990141cdcc8c3f66660e42003005456686444466667e419ae10a4566065e449a998541666660420000fa433333b0426666ba41e17afa41ae470142b0203ac5a53cc50004022020b80b0f00f0000a000000d007d0073075409c0200d0070000000000000000c80050000500000064000a000100240003000300040005001c001d001e001f0020006666f042cdccf8413333f4423333c84233b3314366e694430000c842000052436606e144cdcc2642000000000000f8419a993141000087429a1905433333f44233338e429a990141cdcc8c3f66660e42003005456686444466667e419ae10a4566065e449a998541666660420000fa433333b0426666ba41e17afa41ae470142e2473ac5a53cc50004032020b80b0f00f0000a000000d007d0073075409c0200d0070000000000000000c80050000500000064000a000100240003000300040005001c001d001e001f0020006666f042cdccf8413333f4423333c84233b3314366e694430000c842000052436606e144cdcc2642000000000000f8419a993141000087429a1905433333f44233338e429a990141cdcc8c3f66660e42003005456686444466667e419ae10a4566065e449a998541666660420000fa433333b0426666ba41e17afa41ae4701422c653ac5";

        // v10 FCP=0
        // String data =
        // "02c5040a202100580003005800030058002100580021005800210058002100580021004d00210007002c0007002c00422c06002c422c4237004237060e00003700420005000000c842000034420000c8420000c8420000c8420000c842084cc8420000c84253a9ee420000c8420000c8420000c842e17ac3420000c8420000c8420000c8420000c8420000c8420000c84264006400640064006400640064006400640064006400640064006400640021000000c84200000000000000000000000000000000275e";
        // V16 FCp=1
        // String data =
        // "a53cc50004002020000000007f0000000000000016001600c40b0800000002050026f31f0000000000000a000000000000000000000000000000000063002900000000000000000000e0cb4447619343dc5c494200000000000000000000c84277b87a43cd4cf44100000000000000000000000000000000000000000000000000000000000000000000000023017a436a2e1f43a7b07e41ffffef4143c194430000000000000000911ef242403bfb416666e3420000c842b92dc04100000000000000000000000000003ac5";
        // V11
        // String data =
        // "02c5040b207c7c7600780308009a0264059c041c879c680600280ae2073930020031d47c01a05b0787de1ada1e03d91a3ab50fb45073871700440c09000c00220038004e00cdccf0429a99f9419a99b14200009441cdcca441cd0c2d450040ef4300201644008009450000c4423333f142666680430000a0429a992843cdcc31430000f642cdcc7c419a99f94000007042c800704a1800984a6400344a6e00824a9600aa4abe00d24a00003642333305420040fa4300008c410000d0409a9931419a99b141cfca";

        // FCP=1.v17
        String data = "02c50480200100b10f00000600b90fc3022c1a0000000002000800a34d031e0000000000000000191900010000000000000a04100000002d0000000000000000004b0000004227e8427cd020420000304233330142efee6e3e0000c64100004844000000000000fa44000090413333de4200003042c8ff9742ab2a86410000f242000000004268903f00000000888c1442000000000040c9435245114106ce29415c713f400000000063be21408a2122400000000067669ec10000000050cac4c0875c35c3dc30";
        // FCP=1.v245
        // String data =
        // "a53cc50004002020010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000227af0420a71f6410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000091473ac5";

        // FCP=1.v245
        // String data =
        // "a53cc500040020207b004b087b001b100500eb1700000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f9b2915d001b0d47000060416666c6409a99c940cdcccc400000d040343332313534333236353433393837363333d3406666d6409a99d940cdccdc400000e0403333e3406666e6409a99e940cdccec400000f0403333f34045443938686736354a4939306d4948306442915d6666f6409a99f940cdccfc40000000000000000018e63ac5";

        Calendar cal = Calendar.getInstance();

        byte[] buffer = hexString2ByteArray(data);
        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();
        builder.setDeviceId("100000001");
        builder.setProtocolVersion(128);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(buffer));
        RealDataInfoEntity rd = builder.build();

        return rd;
    }

    private static RealDataInfoEntity getData(String loginId, Date receiveTime, String data) {
        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();
        builder.setDeviceId(loginId);
        builder.setProtocolVersion(245);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(receiveTime));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ONE.value());
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(hexString2ByteArray(data)));
        RealDataInfoEntity rd = builder.build();

        return rd;
    }

    public static RealDataInfoEntity getDataV20() {
        byte[] data = new byte[3 + 1 + 1 + 32 * 2 + 38 * 4 + 2];

        String id = "100000001";

        data[0] = 0x02;
        data[2] = 0x04;
        data[3] = 20;
        data[4] = 32;

        int vInt;

        int offset;
        int slot;
        int index;

        // int
        offset = 5;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            if (i == 8) {
                vInt = 4070;
            }
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // float
        offset += data[4] * 2;
        for (int i = 0; i < 38; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);
            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        // float
        // slot 15 machine timestap
        slot = 15;
        index = offset + (slot - 1) * 4;
        vInt = (int) ((Calendar.getInstance().getTimeInMillis() - TimeZone.getDefault().getRawOffset()) * 0.001);
        data[index] = (byte) (0xff & vInt);
        data[++index] = (byte) (0xff & (vInt >>> 8));
        data[++index] = (byte) (0xff & (vInt >>> 16));
        data[++index] = (byte) (0xff & (vInt >>> 24));

        // float
        // slot 31 mask1
        slot = 31;
        index = offset + (slot - 1) * 4;
        vInt = -1;
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 32 mask2
        slot = 32;
        index = offset + (slot - 1) * 4;
        vInt = -1;
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();
        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, 10);

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    public static RealDataInfoEntity getDataV19() {
        byte[] data = new byte[3 + 1 + 1 + 32 * 2 + 38 * 4 + 2];

        String id = "100000002";

        data[0] = 0x02;
        data[2] = 0x04;
        data[3] = 19;
        data[4] = 32;

        int vInt;

        int offset;
        int slot;
        int index;

        // int
        offset = 5;
        for (int i = 0; i < 32; ++i) {
            index = offset + i * 2;
            vInt = 101 + i;
            if (i == 8) {
                vInt = 4070;
            }
            data[index] = (byte) (vInt & 0xff);
            data[++index] = (byte) ((vInt >> 8) & 0xff);
        }

        // float
        offset += data[4] * 2;
        for (int i = 0; i < 38; ++i) {
            index = offset + i * 4;
            vInt = Float.floatToIntBits(1001.99f + i);

            if (15 == i) {
                vInt = Float.floatToIntBits(8.0f);
            }

            data[index] = (byte) (0xff & vInt);
            data[++index] = (byte) (0xff & (vInt >>> 8));
            data[++index] = (byte) (0xff & (vInt >>> 16));
            data[++index] = (byte) (0xff & (vInt >>> 24));
        }

        // float
        // slot 15 machine timestap
        slot = 15;
        index = offset + (slot - 1) * 4;
        vInt = (int) ((Calendar.getInstance().getTimeInMillis() - TimeZone.getDefault().getRawOffset()) * 0.001);
        data[index] = (byte) (0xff & vInt);
        data[++index] = (byte) (0xff & (vInt >>> 8));
        data[++index] = (byte) (0xff & (vInt >>> 16));
        data[++index] = (byte) (0xff & (vInt >>> 24));

        // float
        // slot 31 mask1
        slot = 31;
        index = offset + (slot - 1) * 4;
        vInt = -1;
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        // float
        // slot 32 mask2
        slot = 32;
        index = offset + (slot - 1) * 4;
        vInt = -1;
        data[index] = (byte) (vInt);
        data[++index] = (byte) (vInt >>> 8);
        data[++index] = (byte) (vInt >>> 16);
        data[++index] = (byte) (vInt >>> 24);

        byte[] crc = new byte[2];
        CRC16.GetCRC(data, 3, data.length - 5, crc);
        data[data.length - 1] = crc[0];
        data[data.length - 2] = crc[1];

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();
        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, 10);

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ZERO.value());
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    public static RealDataInfoEntity getDataV19FCP1() {
        int blockSize = 8 + 32 * 2 + 32 * 4 + 4;
        byte[] data = new byte[blockSize * 2];
        data[0] = (byte) 0xa5;
        data[1] = 0x3c;

        String id = "100000001";
        // a53cc50004002020
        int vInt;
        int offset;
        int slot;
        int index;

        for (int nBlock = 0; nBlock < 2; ++nBlock) {
            // int
            offset = nBlock * 204 + 8;

            for (int i = 0; i < 32; ++i) {
                index = offset + i * 2;
                vInt = nBlock * 10000 + 101 + i;
                data[index] = (byte) (vInt & 0xff);
                data[++index] = (byte) ((vInt >> 8) & 0xff);
            }

            // float
            offset += 32 * 2;
            for (int i = 0; i < 32; ++i) {
                index = offset + i * 4;
                vInt = Float.floatToIntBits(nBlock * 10000 + 1001.99f + i);
                if (30 == i || 31 == i) {
                    vInt = -1;
                }

                if (15 == i) {
                    vInt = Float.floatToIntBits(8.0f);
                }

                data[index] = (byte) (0xff & vInt);
                data[++index] = (byte) (0xff & (vInt >>> 8));
                data[++index] = (byte) (0xff & (vInt >>> 16));
                data[++index] = (byte) (0xff & (vInt >>> 24));
            }
        }

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();
        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, 10);

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ONE.value());
        builder.setProtocolVersion(19);
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    public static RealDataInfoEntity getDataV97FCP1() {
        int blockSize = 8 + 32 * 2 + 32 * 4 + 4;
        byte[] data = new byte[blockSize];
        data[0] = (byte) 0xa5;
        data[1] = 0x3c;

        String id = "100000001";
        // a53cc50004002020
        int vInt;
        int offset;
        int slot;
        int index;

        for (int nBlock = 0; nBlock < 1; ++nBlock) {
            // int
            offset = nBlock * 204 + 8;

            for (int i = 0; i < 32; ++i) {
                index = offset + i * 2;
                vInt = nBlock * 10000 + 101 + i;
                data[index] = (byte) (vInt & 0xff);
                data[++index] = (byte) ((vInt >> 8) & 0xff);
            }

            // float
            offset += 32 * 2;
            for (int i = 0; i < 32; ++i) {
                index = offset + i * 4;
                vInt = Float.floatToIntBits(nBlock * 10000 + 1001.99f + i);
                if (12 == i || 13 == i || 20 == i) {
                    vInt = -1;
                }
                data[index] = (byte) (0xff & vInt);
                data[++index] = (byte) (0xff & (vInt >>> 8));
                data[++index] = (byte) (0xff & (vInt >>> 16));
                data[++index] = (byte) (0xff & (vInt >>> 24));
            }
        }

        RealDataInfoEntity.Builder builder = RealDataInfoEntity.newBuilder();
        Calendar cal = Calendar.getInstance();
        // cal.add(Calendar.HOUR_OF_DAY, 10);

        builder.setDeviceId(id);
        builder.setDataTime(ProtoBufConvertUtils.dateTOTimeStamp(cal.getTime()));
        builder.setRealData(ProtoBufConvertUtils.byteArrayToByteString(data));
        builder.setDataType(TransportDataType.REALDATA.value());
        builder.setFcpType(FcpType.ONE.value());
        builder.setProtocolVersion(97);
        RealDataInfoEntity rd = builder.build();

        return rd;

    }

    static String hex = "0123456789abcdef";

    private static byte[] hexString2ByteArray(String data) {
        byte[] buffer = new byte[(int) (data.length() * 0.5)];

        int index = 0;
        for (int i = 0; i < data.length(); i += 2) {
            int b1 = hex.indexOf(data.charAt(i));
            int b2 = hex.indexOf(data.charAt(i + 1));

            buffer[index] = (byte) (b1 * 16 + b2);

            ++index;
        }

        return buffer;

    }

    public byte[] getByteArray() {
        String data = "a53cc5000400202000000900000000000000000000000000c10b0000000000000104ff1f0000000000000000e703000000000000000000000000000014001400000000000000000000000000000000000000000000000000000000000080a643339bfa44000000000000000000000000000020c2000000000000000000000000000000000000000000000000ab9afa440000000000000000000000000000000000000000000000000000000000000000000000000000c84200000000000020c2ffffffffffffffff6e0c3ac5";
        return hexString2ByteArray(data);
    }

    static void checkLnL() {
        List<CsvLnLData> rows = null;
        try {
            List<Point2D.Double> points = getBoundary();

            rows = new CsvToBeanBuilder<CsvLnLData>(new FileReader("d:/output.csv")).withType(CsvLnLData.class).build()
                    .parse();


            boolean result;
            float longitude, latitude;
            for (CsvLnLData row : rows) {
                longitude = Float.parseFloat(row.longitude);
                latitude = Float.parseFloat(row.latitude);
//				result = BoundaryUtil.isInCircle(15.0, 24.560883, 113.72015, latitude, longitude);
                result = BoundaryUtil.isInPolygon(new Point2D.Double(longitude, latitude), points);
//				if(result)
                {
                    System.out.println(row.id + ", " + row.updatetime + ", " + (result ? result + "<<>>" : result));
                }
            }

            System.out.println("done = " + rows.size());
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    static List<Point2D.Double> getBoundary() {
        List<Point2D.Double> list = new ArrayList<>();

        Gson test = new Gson();

        try {
            String json = "[\r\n" +
                    "				[\r\n" +
                    "					\"113.71991600000001\",\r\n" +
                    "					\"24.562183999999995\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71989800001255\",\r\n" +
                    "					\"24.562269000001542\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71988\",\r\n" +
                    "					\"24.562354\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.7198245000241\",\r\n" +
                    "					\"24.562406000010405\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71976900000001\",\r\n" +
                    "					\"24.562458000000003\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71976850000407\",\r\n" +
                    "					\"24.562507000000156\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.719768\",\r\n" +
                    "					\"24.562556\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71976872489817\",\r\n" +
                    "					\"24.562569697368815\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71976944980764\",\r\n" +
                    "					\"24.562583394737565\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71976872490949\",\r\n" +
                    "					\"24.562597697368794\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71976800000002\",\r\n" +
                    "					\"24.562611999999994\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71975750000587\",\r\n" +
                    "					\"24.562679500000666\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71974700000001\",\r\n" +
                    "					\"24.562746999999998\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71981049998683\",\r\n" +
                    "					\"24.562770500013418\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71987400000002\",\r\n" +
                    "					\"24.562793999999993\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.7198920000011\",\r\n" +
                    "					\"24.562783000001133\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71991000000001\",\r\n" +
                    "					\"24.562772\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71997800001448\",\r\n" +
                    "					\"24.56274200001552\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72004600000001\",\r\n" +
                    "					\"24.56271199999999\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72003049998848\",\r\n" +
                    "					\"24.562687999987013\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.720015\",\r\n" +
                    "					\"24.562663999999998\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72003150001595\",\r\n" +
                    "					\"24.5626304999768\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.720048\",\r\n" +
                    "					\"24.562596999999997\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72009400000707\",\r\n" +
                    "					\"24.562574500007138\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72014\",\r\n" +
                    "					\"24.562552\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.7201575000181\",\r\n" +
                    "					\"24.56251099997122\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72017500000001\",\r\n" +
                    "					\"24.56247\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72018600001353\",\r\n" +
                    "					\"24.56244799997676\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.720197\",\r\n" +
                    "					\"24.562426000000002\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72018499998448\",\r\n" +
                    "					\"24.562364999949942\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72017300000002\",\r\n" +
                    "					\"24.562304\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72016449998588\",\r\n" +
                    "					\"24.562282999968396\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72015600000002\",\r\n" +
                    "					\"24.562261999999997\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72013599999549\",\r\n" +
                    "					\"24.562243499999482\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.720116\",\r\n" +
                    "					\"24.562224999999994\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72009499999923\",\r\n" +
                    "					\"24.562217000001503\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.720074\",\r\n" +
                    "					\"24.562209\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72004499999917\",\r\n" +
                    "					\"24.562202500002815\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.72001600000002\",\r\n" +
                    "					\"24.562196\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.7199964999993\",\r\n" +
                    "					\"24.56218700000138\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.719977\",\r\n" +
                    "					\"24.562178\"\r\n" +
                    "				],\r\n" +
                    "				[\r\n" +
                    "					\"113.71994650000138\",\r\n" +
                    "					\"24.562181000003076\"\r\n" +
                    "				]\r\n" +
                    "			]";

            double[][] points = test.fromJson(json, double[][].class);
            for (double[] point : points) {
                list.add(new Point2D.Double(point[0], point[1]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    @SuppressWarnings("unchecked")
    void produceFromCsv() {
        List<CsvData> rows = null;
        try {
            rows = new CsvToBeanBuilder<CsvData>(new FileReader("d:/opdata.csv")).withType(CsvData.class).build()
                    .parse();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            Calendar cal = Calendar.getInstance();
            Date date = null;

            Date receiveTime;
            for (CsvData row : rows) {

                date = sdf.parse(row.updatetime);
                cal.setTime(date);
                cal.add(Calendar.HOUR_OF_DAY, 8);
                date = cal.getTime();

                RealDataInfoEntity data = getData(row.id, date, row.data);

                BoEntity result = null;
                try {
                    if (translator.translate(data)) {
                        result = translator.getRealDataByFiltered(new EnumRuleCategory[]{EnumRuleCategory.BLACK_LIST,
                                EnumRuleCategory.FILTER, EnumRuleCategory.ALARM});
                        if (null == result.get(EnumRuleCategory.FILTER.name())) {
                            result = (BoEntity) result.get(EnumRuleCategory.DEFAULT.name());
                        } else {
                            result = (BoEntity) result.get(EnumRuleCategory.FILTER.name());
                        }

                        if (result != null) {
                            result.put("serial_no", "ST01019000512");
                            producer.send(new ProducerRecord<>(TOPIC, row.id, result));
                            ++count;
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            System.out.println("Total sent to kafka = " + count);

        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
