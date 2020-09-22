package com.zhongka;

import com.sqlresource.JdbcReaderPublic;
import com.sqlresource.selectSqlSource;
import com.util.NumberUtil;
import com.util.Properties_Util;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by huyunmian on 2019/6/20.
 */
public class CarPositionDataHandler {
    //私有变量，用来存储设备基本信息
    public static  Map<String,Tuple3<String,String,String>>  result = new HashMap<>();
    //私有变量，用来存储重卡所选的参数信息
    public static Map<String,String> selectParamMap = new HashMap<String,String>();
    public static void  main(String []agrs) throws Exception{
        //获取flink流数据上下文
        final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //从数据库获取重卡所选参数的基本信息
        DataStreamSource<Map<String,String>> selectParamStream = env.addSource(new selectSqlSource());
        //将配置信息广播，并进行map存到全局变量
        selectParamStream.broadcast().map(new MapFunction<Map<String, String>, Object>() {
            @Override
            public Object map(Map<String, String> stringStringMap) throws Exception {
                selectParamMap = stringStringMap;
                return null;
            }
        });

        //设置基本信息为信息广播，并进行Map存到全局变量
        DataStreamSource<Map<String, Tuple3<String, String, String>>> mapDataStreamSource = env.addSource(new JdbcReaderPublic());
        mapDataStreamSource.broadcast().map(new MapFunction<Map<String,Tuple3<String,String,String>>, Object>() {

            @Override
            public Object map(Map<String, Tuple3<String, String, String>> stringTuple3Map) throws Exception {
                result = stringTuple3Map;
                return  null;
            }
        });

        //读取kafka基本信息
        String bootStrap_server = Properties_Util.getProperties_String_value("config.properties","bootstrap_servers");
        String zookeeper_connect = Properties_Util.getProperties_String_value("config.properties","zookeeper_connect");
        String group_id = Properties_Util.getProperties_String_value("config.properties","group_id");
        String topic_id = Properties_Util.getProperties_String_value("config.properties","topic_id");
        //輸出kafka 主題
        String outKafka_server = Properties_Util.getProperties_String_value("config.properties","outKafka_server");
        String out_topic_id = Properties_Util.getProperties_String_value("config.properties","outKafka_topic");
        //连接kafka设置
        Properties props = new Properties();
        props.put("bootstrap.servers",bootStrap_server);
        props.put("group.id",group_id);
        props.put("zookeeper.connect",zookeeper_connect);
        FlinkKafkaConsumer010 flinkKafkaConsumer09 = new FlinkKafkaConsumer010(topic_id,new SimpleStringSchema(),props);
        DataStreamSource<String> dataStreamSource = env.addSource(flinkKafkaConsumer09);

        SingleOutputStreamOperator<Object> process = dataStreamSource.map(new RichMapFunction<String, JSONObject>() {
            private transient Counter count;

            @Override
            public void open(Configuration parameters) throws Exception {
                count = getRuntimeContext().getMetricGroup().counter("mycounter");
            }

            @Override
            public JSONObject map(String value) throws Exception {
                count.inc();
                return JSONObject.fromObject(value);
            }
        }).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.get("DeviceID").toString();
            }
        }).process(new KeyedProcessFunction<String, JSONObject, Object>() {
            //声明一个value类型的状态
            private transient ValueState<Tuple2<Double, Double>> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor descriptor = new ValueStateDescriptor("state", TypeInformation.of(new TypeHint<Tuple2<Double, Double>>() {
                }), new Tuple2<Double, Double>(0.0, 0.0));
                state = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<Object> collector) throws Exception {
                //计算实时百公里油耗（公式：（本次油耗-状态油耗）/（本次里程-状态里程）*100）
                String realFule = "";
                double lastOil = state.value().f0;//上次油耗
                double lastKm = state.value().f1;//上次里程
                JSONObject param = JSONObject.fromObject(jsonObject.get("Parameter").toString());
                //判断是否有百公里油耗需求
                double thisOil = 0.0;
                double thisKm = 0.0;
                if(selectParamMap.containsKey("bglyh")) {
                    thisOil = NumberUtil.getDoubleNumber(
                            param.get("J_0001_00_250_Timing@double") == null ? "0.0" : JSONObject.fromObject(JSONArray.fromObject(param.get("J_0001_00_250_Timing@double").toString()).get(0).toString()).get("Value"));
                    thisKm = NumberUtil.getDoubleNumber(
                            param.get("J_0001_EE_917_Timing@double") == null ? "0.0" : JSONObject.fromObject(JSONArray.fromObject(param.get("J_0001_EE_917_Timing@double").toString()).get(0).toString()).get("Value"));
                }
                //计算实时百公里油耗
                String realFuel = "";
                if (lastOil == 0 || lastKm == 0 || (thisKm - lastKm) == 0 || thisKm < lastKm) {
                    realFuel = "";
                } else {
                    realFuel = NumberUtil.getDouble2String((thisOil - lastOil) / (thisKm - lastKm) * 100, 2);
                }
                //保存计算结果
                jsonObject.put("realTimeFuel", realFuel);
                //更新状态（里程，油耗更新为最新状态）
                state.update(new Tuple2<>(thisOil, thisKm));
                //将处理结果放入返回流
                collector.collect(jsonObject);
            }
        });

       process.flatMap(new FlatMapFunction<Object, String>() {
            @Override
            public void flatMap(Object o, Collector<String> collector) throws Exception {
                    JSONObject getJSONObject = JSONObject.fromObject(o);
                    //取到当前记录的设备ID
                    String deviceId = getJSONObject.get("DeviceID").toString().substring(4);
                    //数据转换重新封装
                    JSONObject object = (JSONObject) getJSONObject.get("Parameter");
                    //门状态
                    String   mkzt =  object.get("TY_0002_00_16@int") == null ? "" : JSONObject.fromObject(JSONArray.fromObject(object.get("TY_0002_00_16@int").toString()).get(0).toString()).get("Value").toString();
                    //发动机
                    String fdjzt = object.get("TY_0002_00_15@int") == null ? "" : JSONObject.fromObject(JSONArray.fromObject(object.get("TY_0002_00_15@int").toString()).get(0).toString()).get("Value").toString();
                    //钥匙状态
                    String yszt = object.get("TY_0002_00_12@int") == null ? "" :JSONObject.fromObject(JSONArray.fromObject(object.get("TY_0002_00_12@int").toString()).get(0).toString()).get("Value").toString();
                    //百公里油耗
                    String realFuel = getJSONObject.get("realTimeFuel")==null ? "":getJSONObject.get("realTimeFuel").toString();
                    if(object.containsKey("TY_0002_00_4@string")&&object.containsKey("TY_0002_00_4_GeoAltitude@double")){
                        JSONArray object1 = (JSONArray) object.get("TY_0002_00_4@string");
                        JSONArray object2 = (JSONArray) object.get("TY_0002_00_4_GeoAltitude@double");

                        JSONObject object1_1 = (JSONObject) object1.get(0);
                        JSONObject object2_1 = (JSONObject) object2.get(0);

                        JSONArray object1_3 = (JSONArray) object1_1.get("Value"); //含有经纬度，速度，方向信息
                        JSONArray object2_3 = (JSONArray) object2_1.get("Value"); //含有海拔信息
                        if(object1_3.size()==object2_3.size()){
                            for (int i=0;i<object1_3.size();i++){
                                JSONObject res = new JSONObject();
                                //根据deviceid，查询设备号和终端号
                                String imei ="";
                                String plate = "";
                                if(result.containsKey(deviceId)){
                                    imei = result.get(deviceId).f2;//终端号
                                    plate = result.get(deviceId).f1;//设备号
                                }
                                res.put("imei", imei); //终端号
                                res.put("plate",plate);//设备号

                                if(selectParamMap.containsKey("sjc")) {//数据发送时间
                                    String pstnTime = ((JSONObject) object1_3.get(i)).get("PstnTime").toString();
                                    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                                    sf.setTimeZone(TimeZone.getTimeZone("UTC"));
                                    res.put(MapUtils.getString(selectParamMap,"sjc","date"),sf.format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(pstnTime)));
                                }
                                // res.put("date", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(sf.parse(pstnTime)));//数据发送时间
                                if(selectParamMap.containsKey("GPSjd")) {
                                    double lon  = Double.valueOf(((JSONObject)object1_3.get(i)).get("Lo").toString());
                                    res.put(MapUtils.getString(selectParamMap,"GPSjd","lon"),Double.valueOf(new DecimalFormat("0.000000").format(lon)));//经度
                                }
                                if(selectParamMap.containsKey("GPSwd")) {
                                    double lat  = Double.valueOf(((JSONObject)object1_3.get(i)).get("La").toString());
                                    res.put(MapUtils.getString(selectParamMap,"GPSwd","lat"),Double.valueOf(new DecimalFormat("0.000000").format(lat)));//纬度
                                }
                                if(selectParamMap.containsKey("hb")) {
                                    double alt  = Double.valueOf(((JSONObject)object2_3.get(i)).get("Altitude").toString());
                                    res.put(MapUtils.getString(selectParamMap,"hb","alt"),Double.valueOf(new DecimalFormat("0.0").format(alt)));//纬度
                                }
                                if(selectParamMap.containsKey("GPSfx")) {
                                    double ang  = Double.valueOf(((JSONObject)object1_3.get(i)).get("Direction").toString());
                                    res.put(MapUtils.getString(selectParamMap,"GPSfx","ang"), Double.valueOf(new DecimalFormat("0.0").format(ang)));//GPS方向
                                }
                                if(selectParamMap.containsKey("GPSsd")) {
                                    double v  = Double.valueOf(((JSONObject)object1_3.get(i)).get("Speed").toString());
                                    res.put(MapUtils.getString(selectParamMap,"GPSsd","v"), Double.valueOf(new DecimalFormat("0.0").format(v)));//GPS车速
                                }
                                if(selectParamMap.containsKey("GPScjsj")) {
                                    String pstnTime = ((JSONObject) object1_3.get(i)).get("PstnTime").toString();
                                    res.put(MapUtils.getString(selectParamMap,"GPScjsj","gpstime"), pstnTime);//GPS采集时间
                                }
                                if(selectParamMap.containsKey("sbsd")) {
                                    res.put(MapUtils.getString(selectParamMap,"sbsd","dv"), 0);//设备速度
                                }
                                if(selectParamMap.containsKey("hxjsd")) {
                                    res.put(MapUtils.getString(selectParamMap,"hxjsd","xAcc"), 0);//横向加速度
                                }
                                if(selectParamMap.containsKey("zxjsd")) {
                                    res.put(MapUtils.getString(selectParamMap,"zxjsd","yAcc"), 0);//纵向加速度
                                }
                                if(selectParamMap.containsKey("czjsd")) {
                                    res.put(MapUtils.getString(selectParamMap,"sdsd","zAcc"), 0);//垂直加速度
                                }
                                if(selectParamMap.containsKey("fdjzs")) {
                                    res.put(MapUtils.getString(selectParamMap,"fdjzs","rpm"), 0);//发动机转速
                                }
                                if(selectParamMap.containsKey("fdjsw")) {
                                    res.put(MapUtils.getString(selectParamMap,"fdjsw","wTemp"), 0);//发动机水温
                                }
                                if(selectParamMap.containsKey("syyl")) {
                                    res.put(MapUtils.getString(selectParamMap,"syyl","rFuel"), 0);//剩余油量
                                }
                                if(selectParamMap.containsKey("xczyh")) {
                                    res.put(MapUtils.getString(selectParamMap,"xczyh","tFuel"), 0);//行程总油耗
                                }
                                if(selectParamMap.containsKey("ssyh")) {
                                    res.put(MapUtils.getString(selectParamMap,"ssyh","dFuel"), 0);//瞬时油耗
                                }
                                if(selectParamMap.containsKey("lc")) {
                                    res.put(MapUtils.getString(selectParamMap,"lc","odometer"), 0);//里程
                                }
                                if(selectParamMap.containsKey("fdjzh")) {
                                    res.put(MapUtils.getString(selectParamMap,"fdjzh","lv"), 0);//发动机载荷
                                }
                                if(selectParamMap.containsKey("ymxdwz")) {
                                    res.put(MapUtils.getString(selectParamMap,"ymxdwz","throttle"), 0);//油门相对位置
                                }
                                if(selectParamMap.containsKey("fdjyxsj")) {
                                    res.put(MapUtils.getString(selectParamMap,"fdjyxsj","ert"), 0);//发动机运行时间
                                }
                                if(selectParamMap.containsKey("mkzt")) {
                                    res.put(MapUtils.getString(selectParamMap,"mkzt","gateState"), mkzt); //门控状态
                                }
                                if(selectParamMap.containsKey("fdjzt")) {
                                    res.put(MapUtils.getString(selectParamMap,"fdjzt","engine"), fdjzt);//发动机
                                }
                                if(selectParamMap.containsKey("yszt")) {
                                    res.put(MapUtils.getString(selectParamMap, "yszt", "acc"), yszt);//钥匙状态
                                }
                                if(selectParamMap.containsKey("bglyh")) {
                                    res.put(MapUtils.getString(selectParamMap, "bglyh", "realTimeFuel"), realFuel);//百公里油耗
                                }
                                collector.collect(res.toString());
                            }
                        }

                    }

            }
        }).addSink(new FlinkKafkaProducer010<String>(outKafka_server,out_topic_id,new SimpleStringSchema())).name("zhongKaOut");
                //addSink(new FlinkKafkaProducer010<String>(outKafka_server,out_topic_id,new SimpleStringSchema()))
        env.execute();
    }
}
