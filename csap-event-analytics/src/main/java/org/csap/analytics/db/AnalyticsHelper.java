package org.csap.analytics.db;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.descending;
import static org.csap.analytics.misc.MetricsJsonConstants.APP_ID;
import static org.csap.analytics.misc.MetricsJsonConstants.CATEGORY;
import static org.csap.analytics.misc.MetricsJsonConstants.CREATED_ON;
import static org.csap.analytics.misc.MetricsJsonConstants.DATE;
import static org.csap.analytics.misc.MetricsJsonConstants.HOST;
import static org.csap.analytics.misc.MetricsJsonConstants.LAST_UPDATED_ON;
import static org.csap.analytics.misc.MetricsJsonConstants.LIFE_CYCLE;
import static org.csap.analytics.misc.MetricsJsonConstants.MONGO_DATE;
import static org.csap.analytics.misc.MetricsJsonConstants.PROJECT;
import static org.csap.analytics.misc.MongoConstants.EVENT_COLLECTION_NAME;
import static org.csap.analytics.misc.MongoConstants.EVENT_DB_NAME;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.csap.analytics.CsapAnalyticsApplication;
import org.csap.analytics.misc.BusinessProgramDisplayInfo;
import org.csap.analytics.misc.EventJsonConstants;
import org.csap.analytics.util.DateUtil;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.utils.SimonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;

public class AnalyticsHelper {

	private Logger logger = LoggerFactory.getLogger( AnalyticsHelper.class );
	@Inject
	private MongoClient mongoClient;

	public Document constructQuery(
	                               String appId, 
	                               String project, 
	                               String life, 
	                               int numDays) {
		
		Document query = new Document();
		if ( StringUtils.isNotBlank( appId ) ) {
			query.append( APP_ID, appId );
		}
		if ( StringUtils.isNotBlank( project ) ) {
			query.append( PROJECT, project );
		}
		if ( StringUtils.isNotBlank( life ) ) {
			query.append( LIFE_CYCLE, life );
		}
		Calendar fromTime = Calendar.getInstance( TimeZone.getTimeZone( "CST" ) );
		if ( numDays > 0 ) {
			fromTime.add( Calendar.DAY_OF_MONTH, -numDays );
		}
//		if ( numHours > 0 ) {
//			fromTime.add( Calendar.HOUR_OF_DAY, -numHours );
//		}
		query.append( CREATED_ON + "." + DATE, new BasicDBObject( "$gte", 
			DateUtil.convertJavaCalendarToMongoCreatedDate( fromTime ) ) );
		//query.append( CREATED_ON + "." + LAST_UPDATED_ON, new BasicDBObject( "$gte", fromTime.getTime() ) );
		return query;
	}

	public String getHostName() {
		try {
			InetAddress addr = InetAddress.getLocalHost();
			String hostName = addr.getHostName();
			return hostName;
		} catch ( UnknownHostException e ) {
			logger.error( "Exception while getting host name", e );
		}
		return null;
	}

	
	@Cacheable(value = CsapAnalyticsApplication.ATTRIBUTES_CACHE, unless = "#result == null")
	public Set<String> findReportDocumentAttributes(String appId, String project, String life, String category, String serviceName) {
		Set<String> attributeNames = null;
		
		Document query = constructQueryForReport( appId, project, life, category, serviceName );
		
		Document latestDocumentUploaded = getMongoEventCollection().find( query )
				.sort( descending( "createdOn.lastUpdatedOn" ) )
				.limit( 1 )
				.first();
		logger.debug( "Using {} to find latestDocumentUploaded  {} ", serviceName, latestDocumentUploaded );
		
		if ( null != latestDocumentUploaded ) {
			Document dataNode = (Document) latestDocumentUploaded.get( "data" );
			Object summaryNode = dataNode.get( "summary" );
			logger.debug( "summary node {} ", summaryNode );
			if ( summaryNode instanceof List ) {
				//This is for service and jmx report 
				List summaryList = (List) summaryNode;
				logger.debug( "service name {}", serviceName );
				if ( null == serviceName || serviceName.trim().length() == 0 ) {
					//This is for summary trending report
					Document serviceNode = (Document) summaryList.get( 0 );
					logger.debug( "service node {} ", serviceNode );
					attributeNames = serviceNode.keySet();
				} else {
					for ( Object serviceNodeObj : summaryList ) {
						Document serviceNode = (Document) serviceNodeObj;
						if ( serviceName.equalsIgnoreCase( (String) serviceNode.get( "serviceName" ) ) ) {
							attributeNames = serviceNode.keySet();
							break;
						}
					}
				}
			} else if ( summaryNode instanceof Document ) {
				//This is for vm report
				Document summaryNodeAsDoucment = (Document) summaryNode;
				attributeNames = summaryNodeAsDoucment.keySet();
			}
		} else {
			logger.warn("Did not find a document for {}, {}", appId, category) ;
		}
		logger.debug( "attributeNames {}", attributeNames );
		if ( null == attributeNames ) {
			logger.warn( "attributes retrived are null" );
			logger.warn( "appId {} project {} life {} category {} serviceName {} ", appId, project, life, category, serviceName );
		}
		return attributeNames;
	}

	/**
	 * This query construction will work for service names with pattern data.summary.serviceName If service name is null
	 * then it will just ignore it.
	 *
	 * @param appId
	 * @param project
	 * @param life
	 * @param category
	 * @param serviceName
	 * @return
	 */
	private Document constructQueryForReport(String appId, String project, String life, String category, String serviceName) {
		Document query = new Document();
		if ( StringUtils.isNotBlank( appId ) ) {
			query.append( APP_ID, appId );
		}
		if ( StringUtils.isNotBlank( project ) ) {
			query.append( PROJECT, project );
		}
		if ( StringUtils.isNotBlank( life ) ) {
			query.append( LIFE_CYCLE, life );
		}
		if ( StringUtils.isNotBlank( category ) ) {
			query.append( CATEGORY, category );
		}
		if ( StringUtils.isNotBlank( serviceName ) ) {
			Document serviceNameObj = new Document( "serviceName", serviceName );
			query.append( "data.summary", new Document( "$elemMatch", serviceNameObj ) );
		}
		logger.debug( "Attribute query{}", query );
		return query;
	}

	public BasicDBList getTopServices(String host) {
		BasicDBObject query = new BasicDBObject( HOST, host );
		String formatedDate = DateUtil.buildMongoCreatedDateFromOffset( 0 );
		query.append( CREATED_ON + "." + DATE, formatedDate );
		query.append( CATEGORY, "/csap/reports/process/daily" );
		BasicDBObject match = new BasicDBObject( "$match", query );
		BasicDBObject unwind = new BasicDBObject( "$unwind", "$data.summary" );
		BasicDBObject sortOrder = new BasicDBObject( "data.summary.topCpu", -1 );
		BasicDBObject sort = new BasicDBObject( "$sort", sortOrder );

		DBObject groupFields = new BasicDBObject( "_id", "$_id" );
		groupFields.put( "topServices", new BasicDBObject( "$push", "$data.summary.serviceName" ) );
		//groupFields.put("topCpu", new BasicDBObject( "$push", "$data.summary.topCpu"));
		DBObject group = new BasicDBObject( "$group", groupFields );

		List<DBObject> operations = new ArrayList<>();
		operations.add( match );
		operations.add( unwind );
		operations.add( sort );
		operations.add( group );
		DBCollection dbCollection = getEventCollection();
		AggregationOutput output = dbCollection.aggregate( operations );
		BasicDBList topServiceList = null;
		Iterable<DBObject> results = output.results();
		for ( DBObject resultObj : results ) {
			topServiceList = (BasicDBList) resultObj.get( "topServices" );
			break;
		}
		if ( topServiceList == null ) {
			topServiceList = new BasicDBList();
		}
		return topServiceList;
	}

	/**
	 * @deprecated @return
	 */
	public Map<String, DBObject> getDisplayNames() {
		getAnalyticsSettings();
		DBCollection dbCollection = mongoClient.getDB( EVENT_DB_NAME ).getCollection( "displayNameCollection" );
		DBCursor cursor = dbCollection.find();
		Map<String, DBObject> displayNameMap = new HashMap<>();
		cursor.forEach( dbObject -> displayNameMap.put( (String) dbObject.get( "_id" ), dbObject ) );
		return displayNameMap;
	}

	public Map<String, Document> getAnalyticsSettings() {
		MongoCollection<Document> eventCollection = getMongoEventCollection();
		Document settings = eventCollection.find( eq( "category", "/csap/settings/analytics" ) )
				.sort( descending( "createdOn.lastUpdatedOn" ) )
				.limit( 1 )
				.first();
		Map<String, Document> analyticsSettings = new HashMap<>();
		if ( null != settings && settings.get( "data" ) instanceof List ) {
			List<Document> data = (List<Document>) settings.get( "data" );
			data.forEach( document -> {
				analyticsSettings.put( document.getString( "project" ), document );
			} );
		} else {
			logger.error( "Settings is null or not valid" );
		}
		return analyticsSettings;
	}

	/**
	 * @deprecated @return
	 */
	public DBCollection getEventCollection() {
		return mongoClient.getDB( EVENT_DB_NAME ).getCollection( EVENT_COLLECTION_NAME );
	}

	public List getProjectNames() {
		DBCollection dbCollection = getEventCollection();
		return dbCollection.distinct( "project" );

	}

	public List getLifecyclesForProject(String projectName) {
		DBCollection eventCollection = getEventCollection();
		BasicDBObject query = new BasicDBObject( "project", projectName );
		return eventCollection.distinct( "lifecycle", query, ReadPreference.secondaryPreferred() );
	}

	public Map<String, BusinessProgramDisplayInfo> getDisplayNameInfo() {
		List projectNames = getProjectNames();
		Map<String, BusinessProgramDisplayInfo> displayInfoMap = new HashMap<>();
		projectNames.forEach( projName -> {
			DBObject dbObject = mongoClient.getDB( EVENT_DB_NAME ).getCollection( "displayNameCollection" ).findOne( new BasicDBObject( "_id", projName ) );
			BusinessProgramDisplayInfo displayInfo = new BusinessProgramDisplayInfo();
			displayInfo.setPackageName( (String) projName );
			if ( null != dbObject ) {
				if ( null != dbObject.get( "displayName" ) ) {
					displayInfo.setDisplayName( (String) dbObject.get( "displayName" ) );
				} else {
					displayInfo.setDisplayName( (String) projName );
				}
				if ( null != dbObject.get( "hidden" ) ) {
					displayInfo.setHidden( (Boolean) dbObject.get( "hidden" ) );
				}
				BasicDBList healthEnabledLife = (BasicDBList) dbObject.get( "health" );
				displayInfo.setHealthEnabledLife( healthEnabledLife );
			} else {
				displayInfo.setDisplayName( (String) projName );
			}
			displayInfo.setLifecycle( getLifecyclesForProject( (String) projName ) );
			displayInfoMap.put( (String) projName, displayInfo );
		} );

		return displayInfoMap;
	}

	public void saveOrUpdateHide(String packageName, boolean isHidden) {
		DBCollection collection = mongoClient.getDB( EVENT_DB_NAME ).getCollection( "displayNameCollection" );

		BasicDBObject dbObject = new BasicDBObject();
		dbObject.append( "_id", packageName );
		dbObject.append( "hidden", isHidden );
		BasicDBObject updateDbObject = new BasicDBObject( "$set", dbObject );
		logger.debug( "update db object {} ", updateDbObject );
		BasicDBObject query = new BasicDBObject( "_id", packageName );
		try {
			collection.update( query, updateDbObject, true, false, WriteConcern.ACKNOWLEDGED );
		} catch ( Exception e ) {
			logger.error( "Exception while writing display name", e );
		}
	}

	public void saveOrUpdateHealth(String packageName, String life, boolean saveHealthMessage) {
		DBCollection collection = mongoClient.getDB( EVENT_DB_NAME ).getCollection( "displayNameCollection" );
		BasicDBObject query = new BasicDBObject( "_id", packageName );
		DBObject currentDisplayObject = collection.findOne( query );
		if ( null != currentDisplayObject ) {
			BasicDBList healthList = (BasicDBList) currentDisplayObject.get( "health" );
			if ( null != healthList ) {
				healthList.add( life );
			} else {
				healthList = new BasicDBList();
				healthList.add( life );
				currentDisplayObject.put( "health", healthList );
			}
		} else {
			currentDisplayObject = new BasicDBObject();
			currentDisplayObject.put( "_id", packageName );
			BasicDBList healthList = new BasicDBList();
			healthList.add( life );
			currentDisplayObject.put( "health", healthList );
		}
		BasicDBObject updateDbObject = new BasicDBObject( "$set", currentDisplayObject );
		logger.debug( "update db object {} ", updateDbObject );

		try {
			collection.update( query, updateDbObject, true, false, WriteConcern.ACKNOWLEDGED );
		} catch ( Exception e ) {
			logger.error( "Exception while writing display name", e );
		}
	}

	public void saveOrUpdateDisplayName(String packageName, String displayName) {
		DBCollection collection = mongoClient.getDB( EVENT_DB_NAME ).getCollection( "displayNameCollection" );

		BasicDBObject dbObject = new BasicDBObject();
		dbObject.append( "_id", packageName );
		dbObject.append( "displayName", displayName );
		BasicDBObject updateDbObject = new BasicDBObject( "$set", dbObject );
		logger.debug( "update db object" + updateDbObject );
		BasicDBObject query = new BasicDBObject( "_id", packageName );
		try {
			collection.update( query, updateDbObject, true, false, WriteConcern.ACKNOWLEDGED );
		} catch ( Exception e ) {
			logger.error( "Exception while writing display name", e );
		}
	}

	public List retrieveHostNamesForCluster(String project, String life, String clusterName) {
		BasicDBObject query = constructClusterSummaryQuery( project, life );
		BasicDBObject sortOrder = new BasicDBObject( CREATED_ON + "." + MONGO_DATE, -1 );
		DBCollection dbCollection = getEventCollection();
		DBCursor cursor = dbCollection.find( query ).sort( sortOrder );
		cursor.setReadPreference( ReadPreference.secondaryPreferred() );
		cursor.limit( -1 );
		BasicDBList clusterList = null;
		while (cursor.hasNext()) {
			BasicDBObject clusterSummary = (BasicDBObject) cursor.next();
			DBObject dataObject = (DBObject) clusterSummary.get( "data" );
			BasicDBList packages = (BasicDBList) dataObject.get( "packages" );
			if ( null != packages ) {
				for ( Object packObj : packages ) {
					DBObject packageObject = (DBObject) packObj;
					if ( project.equalsIgnoreCase( (String) packageObject.get( "package" ) ) ) {
						DBObject clustersObject = (DBObject) packageObject.get( "clusters" );
						clusterList = (BasicDBList) clustersObject.get( clusterName );
					}
				}
			}
		}
		logger.debug( "cluster list :: {}", clusterList );
		return clusterList;
	}

	@Deprecated
	public BasicDBObject constructClusterSummaryQuery(Object project, Object life) {
		BasicDBObject query = new BasicDBObject();
		query.append( CATEGORY, EventJsonConstants.CSAP_MODEL_SUMMAY_CATEGORY );
		query.append( "lifecycle", life );
		BasicDBObject projectMatch = new BasicDBObject( "package", project );
		query.append( "data.packages", new BasicDBObject( "$elemMatch", projectMatch ) );
		return query;
	}

	public Document constructModelSummaryQuery(Object project, Object life) {
		Document query = new Document();
		query.append( CATEGORY, EventJsonConstants.CSAP_MODEL_SUMMAY_CATEGORY);
		query.append( "lifecycle", life );
		Document projectMatch = new Document( "package", project );
		query.append( "data.packages", new Document( "$elemMatch", projectMatch ) );
		return query;
	}

	public long getEventCount(String category, int fromDateOffSet, int numDays) {

		Split split = SimonManager.getStopwatch( "reportCounts." + category.replaceAll( "/", ".") ).start();
		BasicDBObject query = new BasicDBObject();
		query.append( "category", category );
		if ( fromDateOffSet > 0 && numDays > 0 ) {
			Calendar from = DateUtil.getDateWithOffSet( fromDateOffSet );
			Calendar to = DateUtil.getDateWithOffSet( (fromDateOffSet - numDays) );
			BasicDBObject dateRange = new BasicDBObject( "$gte", from.getTime() ).append( "$lt", to.getTime() );
			query.append( "createdOn.lastUpdatedOn", dateRange );
		}
		logger.debug( "Count query:: " + query );
		DBCollection eventCollection = getEventCollection();
		split.stop();

		long count = eventCollection.count( query, ReadPreference.secondaryPreferred() );
		
		logger.debug( "Time Taken {}, count: {}",
				SimonUtils.presentNanoTime( split.runningFor() ),
				count );
		
		return count;
		
		
	}

	public MongoCollection<Document> getMongoEventCollection() {
		return mongoClient
				.getDatabase( EVENT_DB_NAME )
				.getCollection( EVENT_COLLECTION_NAME ) 
				.withReadPreference( 
						ReadPreference.primaryPreferred()
				);
	}

}
