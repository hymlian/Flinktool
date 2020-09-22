package com.cci;

import com.sqlresource.JDBCWriter;
import com.util.DeflaterUtil;
import com.util.TimeUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

public class TopicCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.0 配置KafkaConsumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "111.11.4.100:15005");
        props.put("group.id", "ccivital");
        //props.put("group.id", "test1");
        props.put("enable.auto.commit", "true");  //自动commit
        props.put("auto.commit.interval.ms", "1000"); //定时commit的周期
        props.put("session.timeout.ms", "10000"); //consumer活性超时时间
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 1.1 把kafka设置为source
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs
        //国五国六HB
        FlinkKafkaConsumer010 consumerHB = new FlinkKafkaConsumer010<String>("G6HB_NEW", new SimpleStringSchema(), props);
        consumerHB.setStartFromGroupOffsets();
        DataStreamSource dataStreamHB = env.addSource(consumerHB);
        //国五国六FLT
        FlinkKafkaConsumer010 consumerFC = new FlinkKafkaConsumer010<String>("G6FLT_NEW", new SimpleStringSchema(), props);
        consumerFC.setStartFromGroupOffsets();
        DataStreamSource dataStreamFC = env.addSource(consumerFC);
/*        //国四HB
        FlinkKafkaConsumer010 consumerG4HB = new FlinkKafkaConsumer010<String>("G4HB_NEW", new SimpleStringSchema(), props);
        consumerG4HB.setStartFromGroupOffsets();
        DataStreamSource dataStreamG4HB = env.addSource(consumerG4HB);
        //国四flt
        FlinkKafkaConsumer010 consumerG4FLT = new FlinkKafkaConsumer010<String>("G4FLT_NEW", new SimpleStringSchema(), props);
        consumerG4FLT.setStartFromGroupOffsets();
        DataStreamSource dataStreamG4FC = env.addSource(consumerG4FLT);*/
        //日期格式化类
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        //连接四个流合并为一个流,暂时使用两个，国四未上线
        //dataStreamHB.union(dataStreamFC).union(dataStreamG4HB).union(dataStreamG4FC).flatMap(new FlatMapFunction<String,String>() {
        dataStreamHB.union(dataStreamFC).flatMap(new FlatMapFunction<String,String>() {
            @Override
            public void flatMap(String o, Collector collector) throws Exception {
                List<String> jsonStrList=DeflaterUtil.unCompressed(o);
                JSONArray fromObject = JSONArray.fromObject(jsonStrList.toString());
                for(int i =0;i<fromObject.size();i++) {
                    JSONObject fromObject2 = JSONObject.fromObject(fromObject.get(i));
                    //设备Id
                    String vclid = (String)fromObject2.get("Equipment_ID");
                    //信息类型，HB或FC
                    String type = (String)fromObject2.get("Message_Type");
                    //发生时间
                    String date = (String)fromObject2.get("Occurrence_Date_Time");
                    Date parse = simpleDateFormat.parse(date);
                    String datetime = TimeUtil.dateToString(parse, "yyyy-MM-dd");
                    //版本号，区分国五国六国四
                    String version = (String)fromObject2.get("Notification_Version");
                    String gj = "";
                    if("2.004.004".equals(version)){
                        gj="G5";
                    }
                    if("2.601.000".equals(version)){
                        gj="G6";
                    }
                    if("2.602.002".equals(version)){
                        gj="G4";
                    }
                    StringBuffer sb = new StringBuffer();
                    //HB
                    if("HB".equals(type)){
                        //sqlserver
         /*             sb.append("if EXISTS (select * from ccicountstatixtic where flag ='");
                        sb.append(vclid+datetime+"HB')");
                        sb.append(" begin update ccicountstatixtic set amount =amount+1 where flag = '");
                        sb.append(vclid+datetime+"HB'");
                        sb.append(" end else begin insert into ccicountstatixtic(vclid,datetime,type,amount,flag,GJ)values(");
                        sb.append("'"+vclid+"','"+datetime+"','"+"HB',1"+",'"+vclid+datetime+"HB','"+gj+"') end");*/
                      //mysql
                        sb.append("insert into ccicountstatixtic(vclid,datetime,type,amount,flag,GJ)values(");
                        sb.append("'"+vclid+"','"+datetime+"','"+"HB',1"+",'"+vclid+datetime+"HB','"+gj+"') ");
                        sb.append("on  DUPLICATE key update amount=amount+1");
                        collector.collect(sb.toString());
                    }else{
                        //FC
                        //sqlserver
             /*         sb.append("if EXISTS (select * from ccicountstatixtic where flag ='");
                        sb.append(vclid+datetime+"FC')");
                        sb.append(" begin update ccicountstatixtic set amount =amount+1 where flag = '");
                        sb.append(vclid+datetime+"FC'");
                        sb.append(" end else begin insert into ccicountstatixtic(vclid,datetime,type,amount,flag,GJ)values(");
                        sb.append("'"+vclid+"','"+datetime+"','"+"FC',1"+",'"+vclid+datetime+"FC','"+gj+"') end");*/
                        //mysql
                        sb.append("insert into ccicountstatixtic(vclid,datetime,type,amount,flag,GJ)values(");
                        sb.append("'"+vclid+"','"+datetime+"','"+"FC',1"+",'"+vclid+datetime+"FC','"+gj+"') ");
                        sb.append("on  DUPLICATE key update amount=amount+1");
                        collector.collect(sb.toString());
                    }
                }
            }
            //}).print();
        }).setParallelism(3).addSink(new FlinkKafkaProducer010<String>("192.168.35.60:9092","TYP_KTP_CCI_VITAL",new SimpleStringSchema()));
        env.execute("Flink-CCI推送统计-正式-胡云冕");
    }
}
