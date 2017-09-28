package org.csap.data.db;

import static org.csap.data.EventJsonConstants.CATEGORY;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.bson.Document;
import org.csap.CsapDataApplication;
import org.csap.data.util.DateUtil;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.mongodb.MongoClient;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;

@Component
public class HealthEventWriter {

	private Logger logger = LoggerFactory.getLogger( getClass() );
	@Inject
	private MongoClient mongoClient;

	@Inject
	private EventDataHelper eventDataHelper;

	// CANNOT be invoked from same class unless AspectJ configured. Invoked from
	// Landing Page
	@Async ( CsapDataApplication.HEALTH_EXECUTOR )
	public void printMessage ( String message, int delaySeconds )
			throws Exception {

		Thread.sleep( 5000 );
		logger.info( "Time now: {}, Message Received: {}",
			LocalDateTime.now().format( DateTimeFormatter.ofPattern( "hh:mm:ss" ) ),
			message );
	}

	@Async ( CsapDataApplication.HEALTH_EXECUTOR )
	public void writeHealthEvent ( Document eventDocument, String eventJson, String appIdAuth ) {
		SimonManager.getCounter( "addEvent.insert.health" ).increase();
		String projectName = eventDocument.getString( "project" );
		String life = eventDocument.getString( "lifecycle" );
		// boolean isHealthReportEnabled =
		// eventDataHelper.isHealthReportEnabled(projectName, life);
		logger.debug( "checking project {} life {} ", projectName, life );
		boolean isHealthReportEnabled = eventDataHelper.isSaveHealthReport( projectName, life );
		logger.debug( "Health event enabled: {},  for project {} life {} ",
			projectName, life, isHealthReportEnabled );

		if ( isHealthReportEnabled ) {
			// logger.info("Inside write health event");
			eventDocument.put( CATEGORY, "/csap/reports/health" );
			Document query = eventDataHelper.constructCategoryHostDateDocumentQuery( eventDocument );
			Document healthObject = eventDataHelper.getMongoEventCollection()
				.find( query )
				.maxTime( 1, TimeUnit.SECONDS )
				.limit( 1 )
				.first();
			if ( null == healthObject ) {
				Split newTimer = SimonManager.getStopwatch( "addEvent.insert.health.new" ).start();
				logger.debug( "Health object does not exists" );
				constructDataObject( eventDocument );
				logger.debug( "Health object" + eventDocument );
				updateOneRecordPerDay( eventDocument );
				newTimer.stop();
			} else {
				Split updateTimer = SimonManager.getStopwatch( "addEvent.insert.health.update" ).start();
				logger.debug( "Updating health report for host: {} ", eventDocument.getString( "host" ) );

				boolean isHealthStatusChanged = isHealthStatusChanged( eventDocument, healthObject );
				logger.debug( "isHealthStatusChanged: {} ", isHealthStatusChanged );

				boolean isHealthStatusArrayFull = isHealthStatusArrayFull( healthObject );
				logger.debug( "isHealthStatusArrayFull: {} ", isHealthStatusArrayFull );

				boolean isUnHealthyEventExists = isUnHealthyEventExists( eventDocument );
				logger.debug( "isUnHealthyEventExists: {} ", isUnHealthyEventExists );

				if ( isUnHealthyEventExists ) {
					Document healthDataObject = (Document) healthObject.get( "data" );
					int unHealthyEventCount = (Integer) healthDataObject.get( "UnHealthyEventCount" );
					unHealthyEventCount = unHealthyEventCount + 1;
					healthDataObject.put( "UnHealthyEventCount", unHealthyEventCount );
				}
				if ( isHealthStatusChanged && !isHealthStatusArrayFull ) {
					// Update counter and error message
					addToHealthStatus( eventDocument, healthObject );
					updateOneRecordPerDay( healthObject );
				} else {
					// Update only the counter
					if ( isUnHealthyEventExists ) {
						healthObject.remove( "counter" );
						updateOneRecordPerDay( healthObject );
					}
				}
				updateTimer.stop();
			}

		} else {
			logger.debug( "Health report not enabled for {} and life {} ", projectName, life );
		}
	}

	private boolean isUnHealthyEventExists ( Document eventDBObject ) {
		Document dataObject = (Document) eventDBObject.get( "data" );
		Document errorsObject = (Document) dataObject.get( "errors" );
		if ( null != errorsObject ) {
			return true;
		}
		return false;
	}

	private boolean isHealthStatusChanged ( Document eventDBObject, Document healthObject ) {
		Document dataObject = (Document) eventDBObject.get( "data" );
		Document errorsObject = (Document) dataObject.get( "errors" );
		String hostName = eventDBObject.getString( "host" );
		int currentHealthStatusLength = 0;
		if ( null != errorsObject ) {
			List errorsList = (List) errorsObject.get( hostName );
			String currentHealthStatus = errorsList.toString().trim();
			currentHealthStatusLength = currentHealthStatus.length();
		} else {
			List currentStatusList = new ArrayList();
			currentStatusList.add( "Success" );
			currentHealthStatusLength = currentStatusList.toString().length();
		}
		Document healthDataObject = (Document) healthObject.get( "data" );
		List healthStatusList = (List) healthDataObject.get( "healthStatus" );
		Document lastHealthObj = (Document) healthStatusList.get( (healthStatusList.size() - 1) );
		List lastHealthStatus = (List) lastHealthObj.get( "status" );
		int lastHealthStatusLength = lastHealthStatus.toString().length();
		return currentHealthStatusLength != lastHealthStatusLength;

	}

	private boolean isHealthStatusArrayFull ( Document healthObject ) {
		Document healthDataObject = (Document) healthObject.get( "data" );
		List healthStatusList = (List) healthDataObject.get( "healthStatus" );
		logger.debug( "health staus size {}", healthStatusList.size() );
		return healthStatusList.size() > 20;
	}

	private void addToHealthStatus ( Document eventDBObject, Document healthObject ) {
		healthObject.remove( "counter" );
		Document dataObject = (Document) eventDBObject.get( "data" );
		Document errorsObject = (Document) dataObject.get( "errors" );
		String hostName = eventDBObject.getString( "host" );
		List currentStatusList = null;
		Document healthDataObject = (Document) healthObject.get( "data" );
		if ( null != errorsObject ) {
			currentStatusList = (List) errorsObject.get( hostName );
			// int unHealthyEventCount =
			// (Integer)healthDataObject.get("UnHealthyEventCount");
			// unHealthyEventCount++;
			// healthDataObject.put("UnHealthyEventCount", unHealthyEventCount);
			// logger.info("health data object {}",healthDataObject);
		} else {
			currentStatusList = new ArrayList();
			currentStatusList.add( "Success" );
		}

		List healthStatusList = (List) healthDataObject.get( "healthStatus" );
		Document newHealthStatus = new Document();
		newHealthStatus.append( "time", DateUtil.getFormatedTime( Calendar.getInstance() ) );
		newHealthStatus.append( "status", currentStatusList );
		healthStatusList.add( newHealthStatus );

	}

	private String updateOneRecordPerDay ( Document eventDBObject ) {
		String key = "Failure";
		Document query = eventDataHelper.constructCategoryHostDateDocumentQuery( eventDBObject );
		Document setObject = new Document();
		setObject.put( "$set", eventDBObject );
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.upsert( true );
		options.returnDocument( ReturnDocument.AFTER );
		// DBCollection eventCollection = eventDataHelper.getEventCollection();
		try {
			// eventCollection.update(query, setObject, true, false);
			Document result = eventDataHelper.getMongoEventCollection().findOneAndUpdate( query, setObject, options );
			if ( null != result ) {
				key = result.getObjectId( "_id" ).toString();
			}
		} catch (Exception e) {
			logger.error( "Exception while inserting health metrics", e );
		}
		return key;
	}

	private void constructDataObject ( Document eventDBObject ) {
		eventDBObject.remove( "counter" );
		Document dataObject = (Document) eventDBObject.get( "data" );
		Document errorObject = constructErrorObject( eventDBObject );
		if ( (Boolean) dataObject.get( "Healthy" ) ) {
			dataObject.put( "UnHealthyEventCount", 0 );
		} else {
			dataObject.put( "UnHealthyEventCount", 1 );
		}
		dataObject.remove( "Healthy" );
		dataObject.remove( "errors" );
		List errorList = new ArrayList();
		errorList.add( errorObject );
		dataObject.put( "healthStatus", errorList );
	}

	private Document constructErrorObject ( Document eventDBObject ) {
		Document dataObject = (Document) eventDBObject.get( "data" );
		Document vmObject = (Document) dataObject.get( "vm" );
		Document servicesObject = (Document) dataObject.get( "services" );
		Document errorsObject = (Document) dataObject.get( "errors" );
		String hostName = eventDBObject.getString( "host" );
		Document errorObj = new Document();
		errorObj.append( "time", DateUtil.getFormatedTime( Calendar.getInstance() ) );
		if ( null != errorsObject ) {
			List errorsList = (List) errorsObject.get( hostName );
			String errorAsString = errorsList.toString();
			errorObj.append( "status", errorsList );
		} else {
			List errorsList = new ArrayList();
			errorsList.add( "Success" );
			errorObj.append( "status", errorsList );
		}
		return errorObj;
	}

}
