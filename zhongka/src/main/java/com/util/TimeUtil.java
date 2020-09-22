package com.util;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * 
 * @author dangwei
 *
 */
public class TimeUtil {
	public static SimpleDateFormat myFormatter11 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static String toString(Date d, String fmt) {
		if (null == d)
			return "";

		try {
			SimpleDateFormat sdf = new SimpleDateFormat(fmt);
			return sdf.format(d);
		} catch (Exception e) {

		}
		return d.toString();
	}

	public static Date toDate(String d, String fmt) {
		if (null == d)
			return new Date();

		try {
			SimpleDateFormat sdf = new SimpleDateFormat(fmt);
			return sdf.parse(d);
		} catch (Exception e) {
			return new Date();
		}
	}
	
	
	public static String dateToString(Date date, String fmt) {
		String timeStr = "";
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(fmt);
			timeStr = sdf.format(date);
		} catch (Exception ex) {

		}
		return timeStr;
	}
	
	/**
	 * 按时区
	 * */
	public static String dateToStringWithArea(Date date, String fmt) {
		String timeStr = "";
		try {
			SimpleDateFormat f = new SimpleDateFormat(fmt);
	        f.setTimeZone(TimeZone.getTimeZone("UTC"));
	        timeStr = f.format(date);
		} catch (Exception ex) {

		}
		return timeStr;
	}
	

	/**
	 * 把毫秒转化成日期
	 * 
	 * @param dateFormat(日期格式，例如：MM/
	 *            dd/yyyy HH:mm:ss)
	 * @param millSec(毫秒数)
	 * @return
	 */
	public static String transferLongToDate(String dateFormat, Long millSec) {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		Date date = new Date(millSec);
		return sdf.format(date);
	}

	/**
	 * 获取当前时间前N小时的时间
	 */
	public static Date getCurHourTimeBeforeN(int n){
		Calendar ca = Calendar.getInstance();
		ca.add(Calendar.HOUR, -n);
		return ca.getTime();
	}
	
	
	/**
	 * 获取指定时间前N小时的时间
	 */
	public static String getCurHourTimeBeforeN(String date, int n){
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date datetime = null;
		try {
			datetime = formatter.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar ca = Calendar.getInstance();
		ca.setTime(datetime);
		ca.add(Calendar.HOUR, n);
		String time = formatter.format(ca.getTime());
		return time;
	}
	
	public static String addDay(String date, int days) {
		SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar calendar = new GregorianCalendar();
		Date trialTime = new Date();
		try {
			trialTime = myFormatter.parse(date);
		} catch (ParseException e) {
		}
		calendar.setTime(trialTime);
		calendar.add(Calendar.DAY_OF_MONTH, days);
		return myFormatter.format(calendar.getTime());
	}
	public static String getLongTime(String date) {
		try {
			return String.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date;
	}

	public static void main(String[] args) throws ParseException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		Date parse = dateFormat.parse("2020-05-11T20:49:25.000Z");
		/*String curHourTimeBeforeN = getCurHourTimeBeforeN(dateToString(parse, "yyyy-MM-dd HH:mm:ss"), 8);
		System.out.println(curHourTimeBeforeN);*/
		String s = dateToString(parse, "yyyy-MM-dd");
		System.out.println(s);
	}
	
}
