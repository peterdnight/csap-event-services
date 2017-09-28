package org.csap.data.db;

import static com.mongodb.client.model.Filters.eq;
import static org.csap.data.EventJsonConstants.APPID;
import static org.csap.data.EventJsonConstants.CATEGORY;
import static org.csap.data.EventJsonConstants.CREATED_ON;
import static org.csap.data.EventJsonConstants.DATA;
import static org.csap.data.EventJsonConstants.DATE;
import static org.csap.data.EventJsonConstants.HOST;
import static org.csap.data.EventJsonConstants.LAST_UPDATED_ON;
import static org.csap.data.EventJsonConstants.LIFE;
import static org.csap.data.EventJsonConstants.PAYLOAD_CATEGORY;
import static org.csap.data.EventJsonConstants.PROJECT;
import static org.csap.data.EventJsonConstants.SUMMARY;
import static org.csap.data.EventJsonConstants.TIME;
import static org.csap.data.EventJsonConstants.UNIXMS;
import static org.csap.data.MongoConstants.EVENT_COLLECTION_NAME;
import static org.csap.data.MongoConstants.EVENT_DB_NAME;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.csap.data.util.DateUtil;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.utils.SimonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoTimeoutException;
import com.mongodb.WriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.util.JSON;
import static com.mongodb.client.model.Filters.and;
import org.springframework.stereotype.Service;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.and;

@Service
public class EventDataWriter {
	public static final String MONGO_IS_TIMING_OUT = "Mongo Not Available";
	private Logger logger = LoggerFactory.getLogger(getClass());
	@Inject
	private MongoClient mongoClient;
	
	@Inject
	private EventDataHelper eventDataHelper;
	
	@Inject
	private HealthEventWriter healthEventWriter;
		
	public long deleteEventByFilter(String filter){
		long docsEffected = 0;
		Bson query = eventDataHelper.convertUserInterfaceQueryToMongoFilter(filter);				
		DeleteResult result = eventDataHelper.getMongoEventCollection().deleteMany(query);
		if(null != result){
			docsEffected = result.getDeletedCount();
		}
		logger.warn("Docs effected {} ",docsEffected);
		return docsEffected;
	}
	
	public long deleteEventById(String objectId){		
		Document query = new Document("_id",new ObjectId(objectId));		
		DeleteResult result = eventDataHelper.getMongoEventCollection().deleteOne(query);
		long docsEffected = 0;
		if(null != result){
			docsEffected = result.getDeletedCount();
			logger.warn("docs effected {}",docsEffected);
		}
		return docsEffected;
	}
		
	public int updateEvent(String objectId, String eventData){
		//DBObject eventDataObject = (DBObject) JSON.parse(eventData);
		DBObject query = new BasicDBObject("_id",new ObjectId(objectId));
		DBCollection eventCollection = getEventCollection();
		BasicDBObject dbObject = (BasicDBObject) eventCollection.findOne(query);
		BasicDBObject dataObject = (BasicDBObject) dbObject.get("data");
		dataObject.put("csapText", eventData);
		//dbObject.put("data", eventDataObject);
		BasicDBObject createdOn = (BasicDBObject) dbObject.get(CREATED_ON);
		Calendar calendar = Calendar.getInstance();
		createdOn.put(LAST_UPDATED_ON, calendar.getTime());
		//setExpiresAt(dbObject);
		WriteResult result = eventCollection.update(query, dbObject);
		int docsEffected = 0;
		if(null != result){
			docsEffected = result.getN();
			logger.debug("docs effected {}",docsEffected);
		}
		return docsEffected;
	}
	
	public String insertEvent(String eventJson,String appId,String project,String life,String summary,String category){
		String key = "";
		if(StringUtils.isBlank(eventJson) || 
				StringUtils.isBlank(appId) || 
				StringUtils.isBlank(project) || 
				StringUtils.isBlank(life) ||
				StringUtils.isBlank(summary) ||
				StringUtils.isBlank(category)){
			logger.error("Invalid data eventData {} appId {} project {} life {} ",eventJson,appId, project,life);
			return "Insufficient data";
		} else {
			try{
				Document eventDocument = new Document();
				eventDocument.put(APPID, appId);
				eventDocument.put(LIFE, life);
				eventDocument.put(PROJECT, project);
				eventDocument.put(HOST, getHostName());
				eventDocument.put(CATEGORY, category);
				eventDocument.put(SUMMARY, summary);
				setMongoDate(eventDocument);
				if(!category.startsWith("/csap/settings")){					 
					 eventDocument.append("expiresAt", getExpirationTime(eventDocument.getString("lifecycle")));
				}				
				if(eventJson.startsWith("{") || eventJson.startsWith("[")){					 
					 eventDocument.put("data", JSON.parse(eventJson));
				} else {
					Document data = new Document();
					 data.put("csapText", eventJson);
					 eventDocument.put("data", data);
				}				
				eventDataHelper.getMongoEventCollection().insertOne(eventDocument);
				key = eventDocument.get("_id").toString();
			} catch(Exception e){
				logger.error("Exception while inserting data ",e);
				key = "Invalid data";
			}
		}
		return key;
	}
	
	/**
	 * This will insert events and/or metrics based on category.
	 * @param eventJson
	 * @param appId
	 * @return
	 */
	public String insertEventData(String eventJson,String appId) {
		String documentKeyUsedInLogs = "";		
		if(StringUtils.isBlank(eventJson)){
			documentKeyUsedInLogs = "EventJson Empty";
			logger.error("APP id when event json is null {}",appId);
			addExceptionEvent("Empty Event json",eventJson,appId);
			return documentKeyUsedInLogs;
		}

		Split split = SimonManager.getStopwatch( "addEvent.insert" ).start();
		try {						
			Document eventDocument = Document.parse(eventJson);
			eventDocument.append("appId", appId);
			setMongoDate(eventDocument);			
			String category = eventDocument.getString(CATEGORY);
			
			
			if (null != category) {
				if(!category.startsWith("/csap/settings")){					
					eventDocument.append("expiresAt", getExpirationTime(eventDocument.getString("lifecycle")));
				}
				Split categroryTimer = SimonManager.getStopwatch(constructSimonString(category)).start();
				
				if (category.startsWith("/csap/metrics")) {					
					documentKeyUsedInLogs = insertPerformanceDocument(eventDocument, appId);
				} else if (category.startsWith("/csap/health")) {
					logger.debug("Troubleshooting: health updates") ;
					documentKeyUsedInLogs = insertOrUpdate(eventDocument);
					try{
						healthEventWriter.writeHealthEvent(eventDocument, eventJson, appId);						
					}catch(Exception e){
						SimonManager.getCounter(  "addEvent.attempt.failed." + e.getClass().getName() ).increase();
						logger.error("Exception while writing health report event",e);
					}
				} else if("/csap/system/memory/low".equalsIgnoreCase(category)){										
					documentKeyUsedInLogs = insertOrUpdate(eventDocument);
				} else if(category.startsWith("/csap/reports")){					
					documentKeyUsedInLogs = insertOrUpdate(eventDocument);
				} else {					
					eventDataHelper.getMongoEventCollection()
									.insertOne(eventDocument);
					documentKeyUsedInLogs = eventDocument.get("_id").toString();					
					
				}
				categroryTimer.stop();
			} else {
				logger.error("category is null. Not inserting data");
			}

		} catch (Exception e) {

			SimonManager.getCounter(  "addEvent.attempt.failed." + e.getClass().getName() ).increase();
			if(e instanceof MongoTimeoutException){
				documentKeyUsedInLogs = MONGO_IS_TIMING_OUT;
			} else {
				logger.error(eventJson);
				documentKeyUsedInLogs = "Failure";
				addExceptionEvent(e.getMessage(),eventJson,appId);
			}
			logger.error("Exception while inserting event record", e);
		}
		split.stop() ;
		return documentKeyUsedInLogs;
	}

	
	private String constructSimonString(String category){
		String simonString = "";
		if(category.startsWith("/csap/metrics") && category.endsWith("data")){
			simonString = "performance.data";
		} else if(category.startsWith("/csap/metrics") && category.endsWith("attributes")){
			simonString = "performance.attributes";
		} else if(category.startsWith("/csap/metrics") ){
			simonString = "performance.eol.format";
		} else if (category.startsWith("/csap/health")){
			simonString = "host.health";
		} else if("/csap/system/memory/low".equalsIgnoreCase(category)){
			simonString = "host.lowMemory";
		} else if(category.startsWith("/csap/reports")){
			simonString = "performance.summary.report";
		} else if(category.startsWith("/csap/ui")){
			simonString = "user.activity";
		} else if(category.startsWith("/csap/system")){
			simonString = "csap.system";
		} else {
			simonString = category.replaceAll( "/", "." );
		}
		return  "addEvent.insert." + simonString;
	}
	
	private Date getExpirationTime(String life){
		Calendar calendar = Calendar.getInstance();
		if ("prod".equalsIgnoreCase(life)) {
			calendar.add(Calendar.MONTH, 24);			
		} else {
			calendar.add(Calendar.MONTH, 12);			
		}
		return calendar.getTime();
	}
			
	private String insertPerformanceDocument(Document eventDocument, String appId){
		String key = "";
		String category = eventDocument.getString("category");
		MongoCollection<Document> metricsCollection = eventDataHelper.getMetricsCollectionByCategory(category);
		String metricsKey = "";
		if(null != metricsCollection){
			Document dataDocument = (Document)eventDocument.get("data");
			dataDocument.append("createdOn", eventDocument.get("createdOn"));
			metricsCollection.insertOne(dataDocument);
			metricsKey = dataDocument.get("_id").toString();
			logger.debug("Metrics Key {}",metricsKey);
		}
		eventDocument.remove("data");
		if(StringUtils.isNotBlank(metricsKey)){
			eventDocument.append("dataKey", metricsKey);
		}
		if(category.startsWith("/csap/metrics") && category.endsWith("attributes")){			
			eventDataHelper.getMongoEventCollection().insertOne(eventDocument);
			key = eventDocument.get("_id").toString();
		} else if(category.startsWith("/csap/metrics") && category.endsWith("data")){
			key = insertOrUpdate(eventDocument);
		} else {
			logger.debug("Category not ending with data or attribute :: {}",category);
			SimonManager.getCounter(  "addEvent.attempt.failed.eol.format." + appId ).increase();
		}
		return key;
	}
	
	private String insertOrUpdate(Document eventDocument){
		Split insertUpdateTimer = SimonManager.getStopwatch( "addEvent.insert.insertOrUpdate" ).start();
		
		String key="";
		
		List<Bson> queryFilters = new ArrayList<>(); 
		//queryFilters.add(eq("lifecycle",eventDocument.getString("lifecycle")));
		queryFilters.add(eq("category",eventDocument.getString("category")));
		//Special hook for analytics project generating one report for each deployed instance
		if(!(("/csap/reports/global/daily").equalsIgnoreCase(eventDocument.getString("category")))){
			queryFilters.add(eq("host",eventDocument.getString("host")));
		}
		
		Document createdOn = (Document) eventDocument.get("createdOn");
		String date = createdOn.getString("date");
		queryFilters.add(eq("createdOn.date",date));
		
		Bson andedQueryFilters = and(queryFilters);
		
		Document replacementEvent = new Document();
		replacementEvent.put("$set", eventDocument);
		replacementEvent.put("$inc", new Document().append("counter", 1));
		
		// if match is NOT found - insert it
		FindOneAndUpdateOptions findOptions = new FindOneAndUpdateOptions();
		findOptions.upsert(true);
		findOptions.returnDocument(ReturnDocument.AFTER);
		
		
		try{
			
			Document result = 
					eventDataHelper.getMongoEventCollection().findOneAndUpdate(andedQueryFilters, replacementEvent, findOptions);
			
			if(null != result){
				key = result.getObjectId("_id").toString();
			}
		}catch(Throwable e){
			logger.error("Exception while inserting health metrics",e);
			throw e;
		}
		
		insertUpdateTimer.stop() ;

		logger.debug( "Time Taken {}, conditions: {}",
			SimonUtils.presentNanoTime( insertUpdateTimer.runningFor() ),
			queryFilters );
		return key;
	}
	
	
	
	
	public void addPrimarySwitchedMessage(String newPrimaryHost,String oldPrimaryHost){
		Document eventDocument = new Document();
		eventDocument.append(CATEGORY,"/csap/mongo/primary");
		eventDocument.append(SUMMARY,"Mongo DB primary switched");
		eventDocument.append(PROJECT,"CsapData");
		eventDocument.append(HOST,getHostName());
		Document dataObject = new Document();
		dataObject.append("newPrimary",newPrimaryHost);
		dataObject.append("oldPrimary",oldPrimaryHost);
		eventDocument.append(DATA,dataObject);
		Document createdObDbObject = new Document();
		Calendar calendar = Calendar.getInstance();
		createdObDbObject.append(UNIXMS,calendar.getTimeInMillis());
		createdObDbObject.append(DATE,DateUtil.getFormatedDate(calendar));
		createdObDbObject.append(TIME,DateUtil.getFormatedTime(calendar));
		eventDocument.append(CREATED_ON,createdObDbObject);
		//setCreationDate(dbObject);
		setMongoDate(eventDocument);
		String life = System.getenv("csapLife");
		if(StringUtils.isBlank(life))life = "prod";
		eventDocument.append(LIFE,life);		
		eventDocument.append(APPID, "csapeng.gen");
		//setExpiresAt(dbObject);
		eventDocument.append("expiresAt", getExpirationTime(eventDocument.getString("lifecycle")));
		try{
			//DBCollection eventCollection = getEventCollection();
			//eventCollection.insert(dbObject);
			eventDataHelper.getMongoEventCollection()
							.insertOne(eventDocument);
			
		}catch(Exception e){
			logger.error("Exception while inserting primary switched event",e);
		}
	}
	
	private String getHostName() {
		try {
			InetAddress addr = InetAddress.getLocalHost();
			String hostName = addr.getHostName();
			return hostName;
		} catch (UnknownHostException e) {
			logger.error("Exception while getting host name",e);
		}
		return null;
	}
	
	private void addExceptionEvent(String exceptionMessage,String eventJson,String appId){
		Document exceptionEventDocument = new Document();
		exceptionEventDocument.append(CATEGORY,PAYLOAD_CATEGORY);
		exceptionEventDocument.append(SUMMARY,"Exception while inserting payload into mongo");
		exceptionEventDocument.append(PROJECT,"CsapData");
		exceptionEventDocument.append(HOST,"MongoDBHost");
		Document dataObject = new Document();
		dataObject.append("exceptionMessage", exceptionMessage);
		if(StringUtils.isNotBlank(appId)){
			dataObject.append("appIdUsedForAuth",appId);
		} else {
			dataObject.append("appIdUsedForAuth","null");
		}
		if(StringUtils.isNotBlank(eventJson)){
			dataObject.append("eventJson",eventJson);
		} else {
			dataObject.append("eventJson","null");
		}
		exceptionEventDocument.append(DATA,dataObject);
		Document createdObDbObject = new Document();
		Calendar calendar = Calendar.getInstance();
		createdObDbObject.append(UNIXMS,calendar.getTimeInMillis());
		createdObDbObject.append(DATE,DateUtil.getFormatedDate(calendar));
		createdObDbObject.append(TIME,DateUtil.getFormatedTime(calendar));
		exceptionEventDocument.append(CREATED_ON,createdObDbObject);
		setMongoDate(exceptionEventDocument);
		
		String life = System.getenv("csapLife");
		logger.debug("life::{}",life);
		if(StringUtils.isBlank(life))life = "prod";
		exceptionEventDocument.append(LIFE,life);		
		exceptionEventDocument.append(APPID, "csapeng.gen");
		exceptionEventDocument.append("expiresAt", getExpirationTime(exceptionEventDocument.getString("lifecycle")));
		
		try{			
			eventDataHelper.getMongoEventCollection()
							.insertOne(exceptionEventDocument);
		}catch(Exception e){
			logger.error("Exception while inserting exception event",e);
		}
	}
	
	private DBCollection getEventCollection() {
		return mongoClient.getDB(EVENT_DB_NAME).getCollection(EVENT_COLLECTION_NAME);
	}
	
	
	
	private void setMongoDate(Document eventDocument){
		Document createdOn = (Document) eventDocument.get("createdOn");
		if (null != createdOn) {
			Calendar calendar = Calendar.getInstance();
			createdOn.append("lastUpdatedOn", calendar.getTime());
			//createdOn.append("eventCreatedOn",calendar.getTime());
			Long unixMs = createdOn.getLong("unixMs");
			if (null != unixMs) {
				Date date = new Date(unixMs);
				createdOn.append("mongoDate", date);
				eventDocument.put("createdOn", createdOn);
			}
		} else {
			createdOn = new Document();
			Calendar calendar = Calendar.getInstance();
			createdOn.append("lastUpdatedOn", calendar.getTime());
			createdOn.append("eventCreatedOn",calendar.getTime());
			createdOn.append("mongoDate", calendar.getTime());
			createdOn.append("date", DateUtil.getFormatedDate(calendar));
			createdOn.append("time", DateUtil.getFormatedTime(calendar));
			createdOn.append("unixMs", calendar.getTimeInMillis());
			eventDocument.put("createdOn", createdOn);
		}
	}
	
}
