package com.sqlresource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import com.util.Properties_Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class   selectSqlSource extends RichSourceFunction<Map<String,String>> {

    private volatile boolean isRunning = true;
    private Connection connection = null;
    private PreparedStatement ps = null;
    private String driver = "";
    private String url = "";
    private String userName = "";
    private String password = "";
    private String appid = "";

    public selectSqlSource(){
        connection = null;
        ps = null;
        driver = Properties_Util.getProperties_String_value("config.properties","sqlserver_driver_select");
        url = Properties_Util.getProperties_String_value("config.properties","sqlserver_url_select");
        userName = Properties_Util.getProperties_String_value("config.properties","sqlserver_userName_select");
        password = Properties_Util.getProperties_String_value("config.properties","sqlserver_password_select");
        appid = Properties_Util.getProperties_String_value("config.properties","appid");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getSqlConnection();
        String sql = "select tpd_key,tus_sendParam from Timing_param_dict inner join Timing_user_set on tpd_id=tus_tpd_id where tus_appId='"+appid+"'";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
        while(isRunning){
            Map<String, String> deviceMap = new HashMap<String,String>();
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String key = rs.getString("tpd_key");
                String param = rs.getString("tus_sendParam");
                deviceMap.put(key,param);
            }
            sourceContext.collect(deviceMap);//发送结果
            deviceMap.clear();
            Thread.sleep(1000*60*30);
        }
    }

    @Override
    public void cancel() {

    }

    public Connection getSqlConnection(){
        try {
            //加载驱动
            Class.forName(driver);
            //创建连接
            connection = DriverManager.getConnection(url,userName,password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }
}
