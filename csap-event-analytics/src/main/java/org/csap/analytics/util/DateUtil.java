package org.csap.analytics.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtil {

	private static Logger logger = LoggerFactory.getLogger( DateUtil.class );

	public static String convertJavaDateToMongoCreatedDate(Date date){
		String formatedDate = new SimpleDateFormat("yyyy-MM-dd").format(date.getTime());
		return formatedDate;
	}
	
	public static String convertJavaCalendarToMongoCreatedDate(Calendar calendar) {
		String formatedDate = new SimpleDateFormat( "yyyy-MM-dd" ).format( calendar.getTime() );
		return formatedDate;
	}

	public static String getFormatedTime(Calendar calendar) {
		String formatedTime = new SimpleDateFormat( "H-m-s" ).format( calendar.getTime() );
		return formatedTime;
	}

	public static String getFormatedHour(Calendar calendar) {
		String formatedHour = new SimpleDateFormat( "H" ).format( calendar.getTime() );
		return formatedHour;
	}

	public static String buildMongoCreatedDateFromOffset(int offSet) {
		Calendar dateToUpdateWithOffset = Calendar.getInstance();
		logger.debug( "Now: {} ", (new SimpleDateFormat( "yyyy-MM-dd" ).format( dateToUpdateWithOffset.getTime() )).toString() );

		dateToUpdateWithOffset.add( Calendar.DAY_OF_YEAR, -(offSet) );
		String formatedDate = new SimpleDateFormat( "yyyy-MM-dd" ).format( dateToUpdateWithOffset.getTime() );

		logger.debug( "Update with offset {} to be: {} ", Integer.toString( offSet ), 
				formatedDate );
		return formatedDate;
	}

	public static Calendar getDateFromString(String date) {
		Calendar calendar = Calendar.getInstance();
		if ( null != date ) {
			String[] dateArr = date.split( "-" );
			if ( dateArr.length == 3 ) {
				calendar.set( Calendar.YEAR, Integer.parseInt( dateArr[0] ) );
				calendar.set( Calendar.MONTH, (Integer.parseInt( dateArr[1] ) - 1) );
				calendar.set( Calendar.DAY_OF_MONTH, Integer.parseInt( dateArr[2] ) );
				calendar.set( Calendar.HOUR_OF_DAY, 0 );
				calendar.set( Calendar.MINUTE, 0 );
				calendar.set( Calendar.SECOND, 0 );
				calendar.set( Calendar.MILLISECOND, 0 );
			}
		}
		return calendar;
	}

	public static Calendar getDateWithOffSet(int offSet) {
		Calendar calendar = Calendar.getInstance();
		calendar.add( Calendar.DAY_OF_YEAR, -(offSet) );
		calendar.set( Calendar.HOUR_OF_DAY, 0 );
		calendar.set( Calendar.MINUTE, 0 );
		calendar.set( Calendar.SECOND, 0 );
		calendar.set( Calendar.MILLISECOND, 0 );
		return calendar;

	}

	public static String getAnalyticsStartDate() {
		return "2014-01-01";
	}

}
