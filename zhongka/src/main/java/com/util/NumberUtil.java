package com.util;

import java.util.regex.Pattern;

public class NumberUtil {

    public static Double getDoubleNumber(Object obj){
        if(isNumber(obj)){
            return Double.parseDouble(obj.toString());
        }else{
            return 0.0;
        }
    }

    public static boolean isNumber(Object obj){
        if (obj == null)
            return false;
        Pattern pattern = Pattern.compile("^-?\\d+(\\.\\d+)?$");
        return pattern.matcher(obj.toString()).matches();
    }

    public static String getDouble2String(double d,int i){
        return String.format("%."+i+"f", d);
    }

    public static void main(String[] args){
        System.out.println(getDouble2String(12.8597,6));
    }
}
