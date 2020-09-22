package com.sqlresource;

import com.util.Properties_Util;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by huyunmian on 2019/6/19.
 */

public class JdbcReaderPublic extends RichSourceFunction<Map<String,Tuple3<String,String,String>>>{
    public JdbcReaderPublic(){
    }
    private  boolean isRunning = true;
    private Connection connection = null;
    private PreparedStatement ps = null;
    String username = Properties_Util.getProperties_String_value("config.properties","sqlserver_userName");
    String password = Properties_Util.getProperties_String_value("config.properties","sqlserver_password");
    String url = Properties_Util.getProperties_String_value("config.properties","sqlserver_url");
    String driver = Properties_Util.getProperties_String_value("config.properties","sqlserver_driver");
    private String sql ="select Vcl_ID as DeviceID,Vcl_No as plate,TmnlIL_Tmnl_ID as imei from Routing_View_5_info" ;
    public void open (Configuration configuration) throws  Exception{
        super.open(configuration);
        Class.forName(driver);
        Connection con = DriverManager.getConnection(url,username,password);
        ps = con.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext <Map<String,Tuple3<String,String,String>>> sourceContext) throws Exception   {
        while (isRunning){
            Map<String,Tuple3<String,String,String>> deviceMap = new HashedMap();
            ResultSet resultSet = ps.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<Map<String,Object>> rsList = new ArrayList<Map<String,Object>>(1);
            while(resultSet.next()){
                 String deviceID = resultSet.getString("DeviceID");
                 String plate = resultSet.getString("plate");
                String imei = resultSet.getString("imei");
                deviceMap.put(deviceID,new Tuple3<String,String,String>(deviceID,plate,imei));
            }
            sourceContext.collect(deviceMap);
            rsList.clear();
            Thread.sleep(1000*60*30);
        }
    }
    @Override
    public void cancel() {
        try {
            super.close();
            isRunning = false;
            if(connection !=null){
                connection.close();
            }
            if(ps!=null){
                ps.close();
            }
        }catch (Exception e){

        }

    }
}
