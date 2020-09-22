package com.cci;

import com.sqlresource.JDBCWriter;
import com.util.DeSerialization;
import com.util.TimeUtil;
import net.sf.json.JSONArray;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import ty.pub.TransPacket;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KmxCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.0 配置KafkaConsumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.35.60:9092");
        props.put("group.id", "test1");
        props.put("enable.auto.commit", "true");  //自动commit
        props.put("auto.commit.interval.ms", "1000"); //定时commit的周期
        props.put("session.timeout.ms", "10000"); //consumer活性超时时间

        // 1.1 把kafka设置为source
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs
        FlinkKafkaConsumer010 consumerCCI = new FlinkKafkaConsumer010<TransPacket>("TY_KTP_CCI_DataSubcribe", new DeSerialization(), props);
        consumerCCI.setStartFromLatest();



        DataStreamSource dataStreamCCI = env.addSource(consumerCCI);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        //入库的sql语句
        String sql="if EXISTS (select * from ccikmxcountstatixtic where flag = ?)" +
                "  begin update ccikmxcountstatixtic set amount =amount+1 where flag = ?" +
                " end else begin insert into ccikmxcountstatixtic(vclid,datetime,type,amount,flag)values(?,?,?,1,?) end ";
        //首先将HB和FC数据过滤出来
         dataStreamCCI.filter(new FilterFunction<TransPacket>() {
            @Override
            public boolean filter(TransPacket o) throws Exception {
                String kmxTimeStamp3 = "";
                if (o.getBaseInfoMap().containsKey("kmxTimeStamp3")) {
                    kmxTimeStamp3 = o.getBaseInfoMap().get("kmxTimeStamp3");
                    if (Integer.valueOf(kmxTimeStamp3) == 18 || Integer.valueOf(kmxTimeStamp3) > 10000) {
                        return true;
                    } else {
                        return false;
                    }

                } else {
                    return false;
                }

            }
        }).flatMap(new FlatMapFunction<TransPacket, Map>() {
            @Override
            public void flatMap(TransPacket o, Collector collector) throws Exception {
                //发生时间
                String time = TimeUtil.transferLongToDate("yyyy-MM-dd", Long.valueOf(o.getBaseInfoMap().get("MsgTime@long")));
                //设备id
                String deviceId = o.getDeviceId();
                //第三时间戳
                String kmxTimeStamp3 = o.getBaseInfoMap().get("kmxTimeStamp3");
                //等到工况内容
                String GK = o.getWorkStatusMap().get("TY_0002_00_1193_2@string").get(0);
                //为FC，将list拆分成多条
                Map<String, Object> result = null;
                if (Integer.valueOf(kmxTimeStamp3) == 18) {
                    JSONArray fromObject = JSONArray.fromObject(GK);
                    for (int i = 0; i < fromObject.size(); i++) {
                        result = new HashMap<>();
                        result.put("vclid", deviceId);
                        result.put("date", time);
                        result.put("type", "FC");
                        result.put("flag", deviceId + time + "FC");
                        collector.collect(result);
                    }

                } else {
                    result = new HashMap<>();
                    result.put("vclid", deviceId);
                    result.put("date", time);
                    result.put("type", "HB");
                    result.put("flag", deviceId + time + "HB");
                    collector.collect(result);
                }
            }
        }).setParallelism(3).addSink(new JDBCWriter(sql));
        env.execute();

    }
}
