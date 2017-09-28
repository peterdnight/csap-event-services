package org.csap.analytics.http.ui.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Calendar;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.csap.analytics.CsapAnalyticsApplication;
import org.csap.analytics.db.MetricsDataHandler;
import org.csap.docs.CsapDoc;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(CsapAnalyticsApplication.API_URL)
@CsapDoc(title = "CSAP Performance Metrics API", type = CsapDoc.PUBLIC,
		notes = {"Provides performance data via JSONP to various UIS", "<a class='pushButton' target='_blank' href='https://github.com/csap-platform/csap-core/wiki'>learn more</a>"})
public class MetricsController {

	final Logger logger = LoggerFactory.getLogger( getClass() );

	@Inject
	private MetricsDataHandler metricsDataHandler;

	@RequestMapping("/metrics/{hostName}/{id}")
	@CsapDoc(notes = "Time series performance data suitable for graphing",
			linkTests = {"service30Second", "resource300Second", "service300Second","CsAgent300Second","data300Second","jmx300Second","jmxCustom300Second","service30Second:jsonp"},
			linkGetParams = {
					"hostName=csap-dev01,id=service_30,numberOfDays=1",
					"hostName=csap-dev01,id=resource_300,numberOfDays=1",
					"hostName=csap-dev01,id=service_300,numberOfDays=1",
					"hostName=csap-dev01,serviceName=CsAgent,id=service_300,numberOfDays=1",
					"hostName=csap-dev01,serviceName=data,id=service_300,numberOfDays=1",
					"hostName=csap-dev01,id=jmx_300,numberOfDays=1",
					"hostName=csap-dev01,id=jmxCsAgent_300,numberOfDays=1,serviceName=CsAgent",
					"hostName=csap-dev01,id=service_30,numberOfDays=1,callback=myFunctionCall"},
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/javascript"})
	public void performanceData(@PathVariable(value = "hostName") String hostName,
			@PathVariable(value = "id") String collectionSetId,
			@RequestParam(value = "numberOfDays", defaultValue = "1") Integer numberOfDaysToRetrieve,
			@RequestParam(value = "dateOffSet", defaultValue = "0") Integer numDaysOffsetFromToday,
			@RequestParam(value = "serviceName", defaultValue = "") String[] serviceNameArray,
			@RequestParam(value = "callback", defaultValue = "false") String callback,
			@RequestParam(value = "hosts", defaultValue = "") String[] hosts,
			@RequestParam(value = "bucketSize", defaultValue = "1") int bucketSize,
			@RequestParam(value = "bucketSpacing", defaultValue = "0") int bucketSpacing,
			@RequestParam(value = "appId", defaultValue = "null") String appId,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "showDaysFrom", defaultValue = "false") boolean showDaysFrom,
			@RequestParam(value = "padLatest", defaultValue = "true") boolean padLatest,
			HttpServletResponse response) throws IOException {
		


		if ( (collectionSetId.endsWith( "_30" ) && numberOfDaysToRetrieve > 7)
				|| (collectionSetId.endsWith( "_300" ) && numberOfDaysToRetrieve > 20)
				|| (collectionSetId.endsWith( "_3600" ) && numberOfDaysToRetrieve > 90) ) {
			logger.warn( "Large dataset being loaded for host: {}, Application: {}, Data: {}, Number Of days: {} ",
					hostName, appId, collectionSetId, numberOfDaysToRetrieve.toString() );
		}
		
		if ( collectionSetId.startsWith( "app-") ) {
			// more semantically correct - data is stored under jmx so it is converted
			collectionSetId = "jmx" + collectionSetId.substring( 4 );
		}

		Split timerAll = SimonManager.getStopwatch( "performanceData.ALL" ).start();
		Split timerByResourceType = SimonManager.getStopwatch( "performanceData.type." + collectionSetId ).start();
		Split timerByAppId = SimonManager.getStopwatch( "performanceData.appid." + appId ).start();
		Split timerByLifecycle = SimonManager.getStopwatch( "performanceData.life." + life ).start();

		logger.debug( "In metrics controller" );
		if ( callback.equals( "false" ) ) {
			response.setContentType( MediaType.APPLICATION_JSON_VALUE );
		} else {
			response.setContentType( "application/javascript" );
		}
		PrintWriter writer = response.getWriter();
		if ( !callback.equals( "false" ) ) {

			writer.print( callback + "(" );
		}
		writer.print( "{" );
		
		if ( hostName.equals( "PNIGHTIN-2X1X0") ) {
			logger.warn("Desktop top hook for host, forcing csap-dev01") ;
			writer.print( " \"desktopTesting\": \"Peters Desktop detected, forcing csap-dev01\"," );
			hostName = "csap-dev01" ;
		}
		// build unique cache key for time interval
		Calendar dayBeingRetrieved = Calendar.getInstance();
		dayBeingRetrieved.add( Calendar.DAY_OF_YEAR, -(numDaysOffsetFromToday) );
		int reportDayOfYear = dayBeingRetrieved.get( Calendar.DAY_OF_YEAR );
		int year = dayBeingRetrieved.get( Calendar.YEAR );
		
		String uniqueCacheKey = Arrays.asList( serviceNameArray ).toString() + year + "-" + reportDayOfYear ;

		if ( numDaysOffsetFromToday == 0 ) {

			// note current day cache is getting updated every 15/30 minutes
			writer.print(
					metricsDataHandler
					.buildPerformanceGraphDataForToday(
							hostName, collectionSetId,
							uniqueCacheKey,
							numberOfDaysToRetrieve, numDaysOffsetFromToday,
							bucketSize, bucketSpacing,
							serviceNameArray, appId, life, showDaysFrom, padLatest )
			);
			
		} else {
			
			// We keep historical data around much longer

			writer.print(
					metricsDataHandler
					.buildPerformanceGraphData(
							hostName, collectionSetId,
							uniqueCacheKey,
							numberOfDaysToRetrieve, numDaysOffsetFromToday,
							bucketSize, bucketSpacing,
							serviceNameArray, appId, life, showDaysFrom, padLatest )
			);
			
		}
		writer.print( "}" );
		if ( !callback.equals( "false" ) ) {
			writer.print( ")" );
		}
		timerByResourceType.stop();
		timerByAppId.stop();
		timerByLifecycle.stop();
		timerAll.stop();

	}

}
