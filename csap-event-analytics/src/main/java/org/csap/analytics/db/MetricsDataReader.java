package org.csap.analytics.db;

import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static org.csap.analytics.misc.MetricsJsonConstants.ATTRIBUTES;
import static org.csap.analytics.misc.MetricsJsonConstants.CREATED_ON;
import static org.csap.analytics.misc.MetricsJsonConstants.DATA;
import static org.csap.analytics.misc.MetricsJsonConstants.HOST_NAME;
import static org.csap.analytics.misc.MetricsJsonConstants.ID;
import static org.csap.analytics.misc.MetricsJsonConstants.LAST_UPDATED_ON;
import static org.csap.analytics.misc.MongoConstants.EVENT_COLLECTION_NAME;
import static org.csap.analytics.misc.MongoConstants.EVENT_DB_NAME;
import static org.csap.analytics.misc.MongoConstants.METRICS_ATTRIBUTES_COLLECTION_NAME;
import static org.csap.analytics.misc.MongoConstants.METRICS_DATA_COLLECTION_NAME;
import static org.csap.analytics.misc.MongoConstants.METRICS_DB_NAME;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.csap.analytics.CsapAnalyticsApplication;
import org.csap.analytics.misc.EventJsonConstants;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;

import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class MetricsDataReader {

	private Logger logger = LoggerFactory.getLogger( getClass() );

	@Inject
	private MongoClient mongoClient;

	@Cacheable(value = CsapAnalyticsApplication.NUM_DAYS_CACHE)
	public long retrieveNumDaysOfMetrics(String hostName) {
		Split split = SimonManager.getStopwatch( "numDaysOfMetrics" ).start();
		long numDaysAvailable = 0;
		Document query = new Document( "attributes.hostName", hostName );
		Document sortOrder = new Document( "createdOn.date", 1 );
		Document metricsDataObject = getMetricsMongoCollection().find( query )
				.sort( sortOrder )
				.limit( 1 )
				.projection( fields( include( "createdOn" ), excludeId() ) )
				.first();
		if ( null != metricsDataObject ) {
			Document createdOnDbObject = (Document) metricsDataObject.get( CREATED_ON );
			Date earliestDate = (Date) createdOnDbObject.get( "mongoDate" );
			logger.debug( "Earliest Date ::--> {}", earliestDate );
			long diff = Calendar.getInstance().getTimeInMillis() - earliestDate.getTime();
			numDaysAvailable = TimeUnit.DAYS.convert( diff, TimeUnit.MILLISECONDS );
		}
		split.stop();
		return numDaysAvailable;

	}

	@Cacheable(CsapAnalyticsApplication.ATTRIBUTES_CACHE)
	public Document getMetricsAttribute(String hostName, String id,
			int numberOfDaysToRetreive, int numDaysOffsetFromToday, boolean showDaysFrom) {

		Document attributeOject = null;
		Document query = constructAttributeQueryUsingLatestAttributesOnDayTarget(
				hostName, id, numberOfDaysToRetreive, numDaysOffsetFromToday, showDaysFrom );

		Document sortOrder = new Document();
		sortOrder.append( CREATED_ON + "." + LAST_UPDATED_ON, -1 );

		FindIterable<Document> findResult = getMetricsAttributeMongoCollection().find( query )
				.sort( sortOrder )
				.limit( 1 )
				.projection( fields( exclude( "createdOn", "expiresAt" ), excludeId() ) );

		MongoCursor<Document> attributeCursor = findResult.iterator();
		if ( attributeCursor.hasNext() ) {
			while (attributeCursor.hasNext()) {
				attributeOject = (Document) attributeCursor.next();
			}
		} else {
			logger.warn( "Did not find attributes in date range. Using latest" );
			query = constructAttributeQuery( hostName, id );
			//cursor = dbCollection.find(query,keys).sort(sortOrder);
			findResult = getMetricsAttributeMongoCollection().find( query )
					.sort( sortOrder )
					.projection( fields( exclude( "createdOn", "expiresAt" ), excludeId() ) );
			attributeCursor = findResult.iterator();
			while (attributeCursor.hasNext()) {
				logger.debug( "Found in latest" );
				attributeOject = (Document) attributeCursor.next();
			}
		}

		return attributeOject;
	}

	// Significant objects stored in memory DO NOT CACHE PARSED OBJECTS
	public Map<String, List> constructGraphData(
			String hostName, String id,
			int numberOfDaysToRetreive, int numDaysOffsetFromToday,
			String appId, String life, boolean showDaysFrom,
			boolean padLatest, List<String> graphDataPoints) {

		MongoCursor<Document> cursor = getMetricsData( hostName, id, numberOfDaysToRetreive, numDaysOffsetFromToday, showDaysFrom );
		Map<String, List> graphData = new HashMap<>();

		int size = graphDataPoints.size();
		int capacity = getListCapacity( id, numberOfDaysToRetreive );
		Date latestReportDate = null;
		//for(DBObject dbObject : cursor){
		while (cursor.hasNext()) {
			Document dbObject = cursor.next();
			if ( null == latestReportDate ) {
				Document createdOn = (Document) dbObject.get( "createdOn" );
				latestReportDate = (Date) createdOn.get( "mongoDate" );
			}
			Document dataObject = (Document) dbObject.get( DATA );
			for ( int i = 0; i < size; i++ ) {
				String attributeName = graphDataPoints.get( i );
				List atttributeDataObject = (List) dataObject.get( attributeName );
				if ( null != atttributeDataObject ) {
					if ( null != graphData.get( attributeName ) ) {
						graphData.get( attributeName ).addAll( atttributeDataObject );
					} else {
						ArrayList dataList = new ArrayList();
						dataList.ensureCapacity( capacity );
						dataList.addAll( atttributeDataObject );
						graphData.put( attributeName, dataList );
					}
				}
			}
		}
		cursor.close();
		if ( padLatest ) {
			if ( numDaysOffsetFromToday == 0 && StringUtils.isNotEmpty( life ) ) {
				addLatestDataUploadedFrom30SecondCollection( appId, life, id, hostName, latestReportDate, size, graphDataPoints, capacity, graphData );
			} else {
				logger.warn( "Padding is being skipped because lifecycle was empty :: {}", life );
			}
		}

		return graphData;
	}

	// For performance - only 30 second data is uploaded every 30 minutes
	// so we append it to other collections
	private void addLatestDataUploadedFrom30SecondCollection(String appId, String life, String id, String hostName,
			Date latestReportDate, int size, List<String> graphDataPoints, int capacity, Map<String, List> graphData) {
		Map<String, List> currentDayGraphData = new HashMap<>();
		Map<String, List> metricsInterval = getMetricsInterval( appId, life );
		if ( !isIntervalSmallest( metricsInterval, id ) ) {
			//now get lowest interval report from latest report date
			String metricsId = getSmallestIntervalId( metricsInterval, id );
			MongoCursor<Document> currentDaycursor = getMetricsData( hostName, metricsId, latestReportDate );
			while (currentDaycursor.hasNext()) {
				Document dbObject = currentDaycursor.next();
				Document createdOn = (Document) dbObject.get( "createdOn" );
				Date reportDate = (Date) createdOn.get( "mongoDate" );
				Document dataObject = (Document) dbObject.get( DATA );
				for ( int i = 0; i < size; i++ ) {
					String attributeName = graphDataPoints.get( i );
					List atttributeDataObject = (List) dataObject.get( attributeName );
					if ( null != atttributeDataObject ) {
						if ( null != currentDayGraphData.get( attributeName ) ) {
							currentDayGraphData.get( attributeName ).addAll( atttributeDataObject );
						} else {
							ArrayList dataList = new ArrayList();
							dataList.ensureCapacity( capacity );
							dataList.addAll( atttributeDataObject );
							currentDayGraphData.put( attributeName, dataList );
						}
					}
				}
			}
		}
		Set<String> keySet = currentDayGraphData.keySet();
		for ( String key : keySet ) {
			graphData.get( key ).addAll( 0, currentDayGraphData.get( key ) );
		}
	}

	private String getSmallestIntervalId(Map<String, List> metricsInterval, String id) {
		String[] idArray = id.split( "_" );
		if ( null != idArray && idArray.length > 1 ) {
			String idString = idArray[0];
			if ( id.startsWith( "jmx" ) ) {
				idString = "jmx";
			}
			List intervals = metricsInterval.get( idString );
			if ( null != intervals && intervals.size() > 0 ) {
				return idArray[0] + "_" + intervals.get( 0 );
			}
		}
		return id;
	}

	private boolean isIntervalSmallest(Map<String, List> metricsInterval, String id) {
		String[] idArray = id.split( "_" );
		if ( null != idArray && idArray.length > 1 ) {
			String idString = idArray[0];
			if ( id.startsWith( "jmx" ) ) {
				idString = "jmx";
			}
			List intervals = metricsInterval.get( idString );
			if ( null != intervals && intervals.size() > 0 ) {
				if ( !idArray[1].equalsIgnoreCase( "" + intervals.get( 0 ) ) ) {
					return false;
				}
			}
		}
		return true;
	}

	private Map<String, List> getMetricsInterval(String appId, String life) {
		Document query = new Document();
		query
				.append( "appId", appId )
				.append( "lifecycle", life )
				.append( "category", EventJsonConstants.CSAP_MODEL_SUMMAY_CATEGORY );

		Document sortOrder = new Document();
		sortOrder.append( CREATED_ON + "." + LAST_UPDATED_ON, -1 );
		Document dbObj = getMongoEventCollection().find( query )
				.sort( sortOrder )
				.limit( 1 )
				.projection( fields( include( "data.packages" ), excludeId() ) )
				.first();

		Map<String, List> metricsIntervals = new HashMap<>();
		if ( null != dbObj ) {
			Document dataObj = (Document) dbObj.get( "data" );
			List packages = (List) dataObj.get( "packages" );
			for ( Object pack : packages ) {
				Document dbPackObj = (Document) pack;
				Document metricsObj = (Document) dbPackObj.get( "metrics" );
				if ( null != metricsObj ) {
					Set<String> keySet = metricsObj.keySet();
					for ( String key : keySet ) {
						List metricsArray = (List) metricsObj.get( key );
						metricsIntervals.put( key, metricsArray );
					}
				}
			}

		}
		logger.debug( "Metrics intervals {} ", metricsIntervals );
		return metricsIntervals;
	}

	private int getListCapacity(String id, int numberOfDays) {
		int capacity = 300;
		String[] idArr = id.split( "_" );
		if ( id.length() > 1 ) {
			capacity = ((24 * 60 * 60) * numberOfDays) / (Integer.parseInt( idArr[1] ));
		}
		capacity++;
		return capacity;
	}

	private MongoCursor<Document> getMetricsData(String hostName, String id, Date startDate) {
		Document query = new Document();
		query.append( ATTRIBUTES + "." + HOST_NAME, hostName );
		query.append( ATTRIBUTES + "." + ID, id );
		if ( null == startDate ) {
			startDate = Calendar.getInstance().getTime();
		}
		long startTime = startDate.getTime() + (10 * 60 * 1000);
		startDate.setTime( startTime );
		query.append( CREATED_ON + "." + LAST_UPDATED_ON, new Document( "$gt", startDate ) );
		Document sortOrder = new Document();
		sortOrder.append( CREATED_ON + "." + LAST_UPDATED_ON, -1 );
		FindIterable<Document> findResult = getMetricsMongoCollection().find( query )
				.sort( sortOrder )
				.projection( fields( include( "data", "createdOn" ), excludeId() ) );
		return findResult.iterator();
	}

	public MongoCursor<Document> getMetricsData(
			String hostName, String id,
			int numberOfDays, int numDaysOffsetFromToday, boolean showDaysFrom) {

		Document query = constructQuery( hostName, id, numberOfDays, numDaysOffsetFromToday, showDaysFrom );
		Document sortOrder = new Document();
		sortOrder.append( CREATED_ON + "." + LAST_UPDATED_ON, -1 );
		FindIterable<Document> findResult = getMetricsMongoCollection().find( query )
				.sort( sortOrder )
				.projection( fields( include( "data", "createdOn" ), excludeId() ) );
		return findResult.iterator();

	}

	private Document constructQuery(
			String hostName, String id,
			int numDays, int numDaysOffsetFromToday, boolean showDaysFrom) {

		Document query = new Document();
		query.append( ATTRIBUTES + "." + HOST_NAME, hostName );
		query.append( ATTRIBUTES + "." + ID, id );

		Calendar startTime = Calendar.getInstance();
		Calendar endTime = Calendar.getInstance();
		if ( numDaysOffsetFromToday <= 0 ) {
			int fromDateOffSet = numDays + numDaysOffsetFromToday;
			int toDateOffSet = numDaysOffsetFromToday;
			startTime.add( Calendar.DAY_OF_YEAR, -(fromDateOffSet) );
			endTime.add( Calendar.DAY_OF_YEAR, -(toDateOffSet) );
		} else {
			int fromDateOffSet = 0;
			int toDateOffSet = 0;
			if ( showDaysFrom ) {
				fromDateOffSet = numDaysOffsetFromToday;
				toDateOffSet = numDaysOffsetFromToday - numDays;
			} else {
				fromDateOffSet = (numDaysOffsetFromToday + numDays) - 1;
				toDateOffSet = numDaysOffsetFromToday - 1;
			}
			startTime.add( Calendar.DAY_OF_YEAR, -(fromDateOffSet) );
			endTime.add( Calendar.DAY_OF_YEAR, -(toDateOffSet) );

			endTime.set( Calendar.HOUR_OF_DAY, 0 );
			endTime.set( Calendar.MINUTE, 0 );
			endTime.set( Calendar.SECOND, 0 );
			endTime.set( Calendar.MILLISECOND, 0 );

			startTime.set( Calendar.HOUR_OF_DAY, 0 );
			startTime.set( Calendar.MINUTE, 0 );
			startTime.set( Calendar.SECOND, 0 );
			startTime.set( Calendar.MILLISECOND, 0 );
		}

		query.append( CREATED_ON + "." + LAST_UPDATED_ON, new Document( "$gte", startTime.getTime() ).append( "$lte", endTime.getTime() ) );
		//logger.debug("Query::"+query);
		return query;
	}

	private Document constructAttributeQuery(String hostName, String id) {
		Document query = new Document();
		query.append( HOST_NAME, hostName );
		query.append( ID, id );
		//Calendar calendar = Calendar.getInstance();
		//calendar.add(Calendar.DAY_OF_YEAR, (-numDays));
		//query.append(CREATED_ON+"."+LAST_UPDATED_ON, new BasicDBObject("$gte",calendar.getTime()));
		//logger.debug("Query::"+query);
		return query;
	}

	//
	// Attributes are only uploaded if a host restarts or they have changed
	// --- get the LAST attributes uploader, prior to the interval requested by numDaysOffsetFromToday
	private Document constructAttributeQueryUsingLatestAttributesOnDayTarget(String hostName, String id,
			int numberOfDaysToRetreive, int numDaysOffsetFromToday, boolean showDaysFrom) {
		Document query = new Document();
		query.append( HOST_NAME, hostName );
		query.append( ID, id );
		Calendar startTime = Calendar.getInstance();
		Calendar endTime = Calendar.getInstance();
		if ( numDaysOffsetFromToday <= 0 ) {
			int fromDateOffSet = numberOfDaysToRetreive + numDaysOffsetFromToday;
			int toDateOffSet = numDaysOffsetFromToday;
			startTime.add( Calendar.DAY_OF_YEAR, -(fromDateOffSet) );
			endTime.add( Calendar.DAY_OF_YEAR, -(toDateOffSet) );
		} else {
			int fromDateOffSet = 0;
			int toDateOffSet = 0;
			if ( showDaysFrom ) {
				fromDateOffSet = numDaysOffsetFromToday;
				toDateOffSet = numDaysOffsetFromToday - numberOfDaysToRetreive;
			} else {
				fromDateOffSet = numDaysOffsetFromToday + numberOfDaysToRetreive;
				toDateOffSet = numDaysOffsetFromToday;
			}
			startTime.add( Calendar.DAY_OF_YEAR, -(fromDateOffSet) );
			endTime.add( Calendar.DAY_OF_YEAR, -(toDateOffSet) );

			endTime.set( Calendar.HOUR_OF_DAY, 0 );
			endTime.set( Calendar.MINUTE, 0 );
			endTime.set( Calendar.SECOND, 0 );
			endTime.set( Calendar.MILLISECOND, 0 );

			startTime.set( Calendar.HOUR_OF_DAY, 0 );
			startTime.set( Calendar.MINUTE, 0 );
			startTime.set( Calendar.SECOND, 0 );
			startTime.set( Calendar.MILLISECOND, 0 );
		}
		query.append( CREATED_ON + "." + LAST_UPDATED_ON, new Document( "$lte", endTime.getTime() ) );
		return query;
	}

	public MongoCollection<Document> getMongoEventCollection() {
		return mongoClient.getDatabase( EVENT_DB_NAME )
				.getCollection( EVENT_COLLECTION_NAME )
				.withReadPreference( ReadPreference.secondaryPreferred() );
	}

	private MongoCollection<Document> getMetricsMongoCollection() {
		return mongoClient.getDatabase( METRICS_DB_NAME )
				.getCollection( METRICS_DATA_COLLECTION_NAME )
				.withReadPreference( ReadPreference.secondaryPreferred() );
	}

	private MongoCollection<Document> getMetricsAttributeMongoCollection() {
		return mongoClient.getDatabase( METRICS_DB_NAME )
				.getCollection( METRICS_ATTRIBUTES_COLLECTION_NAME )
				.withReadPreference( ReadPreference.secondaryPreferred() );
	}

}
