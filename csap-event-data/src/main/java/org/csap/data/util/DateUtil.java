package org.csap.data.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtil {
	private static Logger logger = LoggerFactory.getLogger(DateUtil.class);
	
	public static Date convertUserDateToJavaDate(String userInterfaceDate){
		logger.debug("date String {}",userInterfaceDate);
		Date date = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
			sdf.setTimeZone(TimeZone.getTimeZone("CST"));
			date = sdf.parse(userInterfaceDate);
		} catch (ParseException e) {
			logger.error("Exception while parsing date",e);
		}
		logger.debug("Date: {}", date.toString() ) ;
		return date;
	}
	
	
	public static String convertUserDateToMongoCreatedDate(String userInterfaceDate){
		return convertJavaDateToMongoCreatedDate (
				convertUserDateToJavaDate( userInterfaceDate )
		) ;
	}
	
	public static String convertJavaDateToMongoCreatedDate(Date date){
		String formatedDate = new SimpleDateFormat("yyyy-MM-dd").format(date.getTime());
		return formatedDate;
	}
	
	
	public static String getFormatedDate(Calendar calendar){
		String formatedDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		return formatedDate;
	}
	public static String getFormatedTime(Calendar calendar){
		String formatedTime = new SimpleDateFormat("HH:mm:ss").format(calendar.getTime());
		return formatedTime;
	}
	public static Date getOffSetDate(int offSet){
		Date date = null;
		return date;
	}
	public static Date getExpirationDate(int offSet) {
		Date date = null;
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("CST"));
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		calendar.add(Calendar.MONTH, offSet);
		date = calendar.getTime();
		return date;
	}
	
	
}
