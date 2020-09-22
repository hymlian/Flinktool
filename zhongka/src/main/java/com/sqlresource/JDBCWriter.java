package com.sqlresource;

import com.util.Properties_Util;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by huyunmian on 2019/7/19.
 */
public class JDBCWriter extends RichSinkFunction<Map<String,Object>> {
    private static int count = 0;
    private Connection connection;
    private PreparedStatement preparedStatement;

    String userName = "wddev";
    String password = "wddev";
    String url = "jdbc:sqlserver://192.168.30.55:1433;DatabaseName=CTY_MS";
    String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";



    private String sql="";
    public JDBCWriter(String sql){
       this.sql = sql;
    }

    @Override
    public void open (Configuration configuration) throws  Exception{
        super.open(configuration);
        Class.forName(driver);
        Connection con = DriverManager.getConnection(url,userName,password);
        preparedStatement = con.prepareStatement(sql);
    }

    @Override
    public void close() {
        try {
            super.close();
            if(preparedStatement!=null){
                preparedStatement.close();
            }
            if(connection !=null){
                connection.close();
            }
        }catch (Exception e){

        }

    }

    @Override
    public void invoke(Map<String,Object> value, Context context) throws Exception {
        String vclid = (String) value.get("vclid");
        String date = (String) value.get("date");
            String type = (String) value.get("type");
        String flag = (String) value.get("flag");
        System.out.println(value);
        preparedStatement.setString(1,flag);
        preparedStatement.setString(2,flag);
        preparedStatement.setString(3,vclid);
        preparedStatement.setString(4,date);
        preparedStatement.setString(5,type);
        preparedStatement.setString(6,flag);
        preparedStatement.addBatch();
        if(count==100){
            preparedStatement.executeBatch();
            count=0;
        }else{
            count++;
        }
    }
}
