package com.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class Properties_Util {
    public static String getProperties_String_value(String fileName,String key){
        String value = "";
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(fileName);
            value = config.getString(key);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return value;
    }

    public static int getProperties_int_value(String fileName,String key){
        String value = "";
        int result = 0;
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(fileName);
            value = config.getString(key);
            result = Integer.parseInt(value);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args){
        String value = Properties_Util.getProperties_String_value("config.properties","group_id");
        System.out.println(value);
    }
}
