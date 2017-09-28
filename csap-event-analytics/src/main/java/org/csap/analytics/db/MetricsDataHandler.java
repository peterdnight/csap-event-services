package org.csap.analytics.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.ArrayUtils;
import org.bson.Document;
import org.csap.analytics.CsapAnalyticsApplication;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.util.JSON;

public class MetricsDataHandler {

	private Logger logger = LoggerFactory.getLogger( getClass() );
	@Inject
	private MetricsDataReader metricsDataReader;
	@Inject
	private AnalyticsHelper analyticsHelper;

	private ObjectMapper jacksonMapper = new ObjectMapper();

	
	// storing JSON or BSON arrays in memory can get VERY expensive on large datasets (millions of Objects created)
	// Only the unparsed string output is cached 
	@Cacheable(value = CsapAnalyticsApplication.METRICS_REPORT_CACHE,
			key = "{#hostName,#collectionId,#numberOfDays,#dayOfYearAndServiceCacheKey,#appId,#life,#showDaysFrom,#padLatest}")
	public String buildPerformanceGraphDataForToday(String hostName, String collectionId,
			String dayOfYearAndServiceCacheKey,
			int numberOfDays, int numDaysOffsetFromToday,
			int bucketSize, int bucketSpacing,
			String[] serviceNameArray, String appId, String life,
			boolean showDaysFrom, boolean padLatest) {

		return getData( hostName, collectionId, dayOfYearAndServiceCacheKey,
				numberOfDays, numDaysOffsetFromToday,
				bucketSize, bucketSpacing, serviceNameArray,
				appId, life, showDaysFrom, padLatest );
	}

	
	@Cacheable(value = CsapAnalyticsApplication.HISTORICAL_REPORT_CACHE,
			key = "{#hostName,#collectionId,#numberOfDays,#dayOfYearAndServiceCacheKey,#appId,#life,#showDaysFrom,#padLatest}")
	public String buildPerformanceGraphData(String hostName, String collectionId,
			String dayOfYearAndServiceCacheKey, 
			int numberOfDays, int numDaysOffsetFromToday,
			int bucketSize, int bucketSpacing,
			String[] serviceNameArray, String appId, String life,
			boolean showDaysFrom, boolean padLatest) {

		return getData( hostName, collectionId, dayOfYearAndServiceCacheKey,
				numberOfDays, numDaysOffsetFromToday,
				bucketSize, bucketSpacing, serviceNameArray,
				appId, life, showDaysFrom, padLatest );
	}

	
	
	private String getData(String hostName, String collectionId,
			String dayOfYearAndServiceCacheKey, int numberOfDays, int numDaysOffsetFromToday,
			int bucketSize, int bucketSpacing,
			String[] serviceNameArray, String appId, String life,
			boolean showDaysFrom, boolean padLatest) {

		logger.debug( "No cache entry found or it expired - adding entry for Host: {}, type: {}, dayOfYearAndServiceCacheKey: {}, numberOfDays: {}",
				hostName, collectionId, dayOfYearAndServiceCacheKey, numberOfDays );

		Split timerAll = SimonManager.getStopwatch( "performanceData.ALL.build" ).start();
		Split timerByResourceType = SimonManager.getStopwatch( "performanceData.type." + collectionId + ".build" ).start();
		Split timerByAppId = SimonManager.getStopwatch( "performanceData.appid." + appId + ".build" ).start();
		Split timerByLifecycle = SimonManager.getStopwatch( "performanceData.life." + life + ".build" ).start();

		Document attributeObject = metricsDataReader.getMetricsAttribute( hostName, collectionId, numberOfDays, numDaysOffsetFromToday, showDaysFrom );
		if ( null == attributeObject ) {
			return " \"data\" : \"Error getting data\"";
		}

		StringBuilder result = new StringBuilder();
		//Cache this and then add it back
		long numDaysAvailable = metricsDataReader.retrieveNumDaysOfMetrics( hostName );
		String attribJson = attributeObject.toJson();
		Document attribObject = (Document) Document.parse( attribJson );
		List<String> allGraphDataPoints = retrieveDataPointNames( attribObject );
		allGraphDataPoints.add( "timeStamp" );
		allGraphDataPoints.add( "totalCpu" );
		filterForService( collectionId, serviceNameArray, attribObject, hostName );
		attribObject.append( "numDaysAvailable", numDaysAvailable );
		result.append( "\"attributes\":" );
		result.append( JSON.serialize( attribObject ) );
		result.append( "," );

		List<String> graphDataPoints = retrieveDataPointNames( attribObject );
		graphDataPoints.add( "timeStamp" );
		graphDataPoints.add( "totalCpu" );

		result.append( "\"data\":{" );
		Map<String, List> allGraphData = metricsDataReader.constructGraphData(
				hostName, collectionId, numberOfDays, numDaysOffsetFromToday,
				appId, life, showDaysFrom, padLatest, allGraphDataPoints );

		logger.debug( "graph data points{}", graphDataPoints );
		Map<String, List> graphData = new HashMap<>();
		int size = graphDataPoints.size();
		for ( int i = 0; i < size; i++ ) {
			String attributeName = graphDataPoints.get( i );
			graphData.put( attributeName, allGraphData.get( attributeName ) );
		}

		int graphMapSize = graphData.size();
		int counter = 0;
		Set<String> keySet = graphData.keySet();
		for ( String key : keySet ) {
			if ( key.startsWith( "attributes" ) ) {
				continue;
			}
			if ( "totalCpu".equalsIgnoreCase( key ) && null == graphData.get( key ) ) {
				continue;
			}
			if ( counter != 0 ) {
				result.append( "," );
			}
			result.append( "\"" + key + "\":" );
			if ( !key.equalsIgnoreCase( "timeStamp" ) ) {
				result.append( getGraphData( key, graphData, bucketSize, bucketSpacing ) );
			} else {
				try {
					result.append( jacksonMapper.writeValueAsString( getGraphData( key, graphData, bucketSize, bucketSpacing ) ) );
				} catch ( Exception e ) {
					logger.error( "Exception while writing time stamp data", e );
				}
			}

			counter++;
		}
		//clearing map
		graphData.clear();
		result.append( "}" );

		timerByResourceType.stop();
		timerByAppId.stop();
		timerByLifecycle.stop();
		timerAll.stop();

		return result.toString();
	}

	private List getGraphData(String key, Map<String, List> graphData, int bucketSize, int bucketSpacing) {
		//logger.info("Key::{}",key);
		List grapDataList = graphData.get( key );
		List skipDataList = new ArrayList();
		if ( bucketSize > 0 && bucketSpacing >= 1 ) {
			int start = 0;
			int end = bucketSize - 1;
			boolean reInitializeRange = false;
			for ( int k = 0; k < grapDataList.size(); k++ ) {
				if ( k >= start && k <= end ) {
					reInitializeRange = true;
					skipDataList.add( grapDataList.get( k ) );
				} else if ( reInitializeRange ) {
					start = k + bucketSpacing;
					end = (k + bucketSpacing + bucketSize - 1);
					reInitializeRange = false;
				}
			}
		} else {
			skipDataList = grapDataList;
		}
		return skipDataList;
	}

	private void filterForService(String id, String[] serviceNameArray, Document attribObject, String host) {
		String[] servicesRequested = constructServiceArray( id, serviceNameArray, attribObject, host );
		if ( ArrayUtils.isNotEmpty( servicesRequested ) ) {
			List servicesRequestedDbObject = new ArrayList();
			for ( String serviceName : servicesRequested ) {
				servicesRequestedDbObject.add( serviceName );
			}
			attribObject.put( "servicesRequested", servicesRequestedDbObject );

			Document graphObject = (Document) attribObject.get( "graphs" );
			Set<String> keySet = graphObject.keySet();
			for ( String graphKey : keySet ) {
				Document graphNames = (Document) graphObject.get( graphKey );
				String graphNamesJson = graphNames.toJson();
				Document graphCopyObject = Document.parse( graphNamesJson );
				Set<String> graphNameVals = graphNames.keySet();
				for ( String graphName : graphNameVals ) {
					if ( !isServiceGraphRequired( id, servicesRequested, graphName ) ) {
						graphCopyObject.remove( graphName );
					}
				}
				graphObject.put( graphKey, graphCopyObject );
			}
		}
	}

	private boolean isServiceGraphRequired(String id, String[] serviceNameArray, String graphName) {
		for ( String serviceName : serviceNameArray ) {
			if ( serviceName.trim().length() > 0 && graphName.contains( serviceName ) ) {
				return true;
			}
		}
		if ( "timeStamp".equalsIgnoreCase( graphName ) || "totalCpu".equalsIgnoreCase( graphName ) || ((id.startsWith( "jmx" ) && !id.startsWith( "jmx_" ))) ) {
			return true;
		}
		return false;
	}

	private String[] constructServiceArray(String id, String[] serviceNameArray, Document attribObject, String host) {
		String[] newServiceArray = null;
		logger.debug( "serviceNameArray" + serviceNameArray.length );
		if ( id.startsWith( "resource" ) || ((id.startsWith( "jmx" ) && !id.startsWith( "jmx_" ))) ) {
			return newServiceArray;
		} else if ( ArrayUtils.isNotEmpty( serviceNameArray ) ) {
			return serviceNameArray;
		} else {

			List topServices = analyticsHelper.getTopServices( host );
			logger.debug( "top services{}", topServices );
			if ( topServices.size() == 0 ) {
				topServices = (List) attribObject.get( "servicesAvailable" );
			}
			//BasicDBList availableServices = (BasicDBList) attribObject.get("servicesAvailable");
			if ( null != topServices ) {
				if ( topServices.size() < 5 ) {
					newServiceArray = new String[topServices.size()];
				} else {
					newServiceArray = new String[5];
				}
				for ( int i = 0; i < newServiceArray.length; i++ ) {
					newServiceArray[i] = (String) topServices.get( i );
				}
			}

		}
		return newServiceArray;
	}

	private List<String> retrieveDataPointNames(Document attributeDbObject) {
		Document graphObject = (Document) attributeDbObject.get( "graphs" );
		Set<String> keySet = graphObject.keySet();
		List<String> graphDataPoints = new ArrayList<>();
		for ( String key : keySet ) {
			Document graphNames = (Document) graphObject.get( key );
			Set<String> graphNameVals = graphNames.keySet();
			graphDataPoints.addAll( graphNameVals );
		}
		return graphDataPoints;
	}

}
