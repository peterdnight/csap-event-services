package org.csap.data.http.api;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.csap.CsapDataApplication;
import org.csap.data.db.EventDataWriter;
import org.csap.docs.CsapDoc;
import org.csap.security.SpringAuthCachingFilter;
import org.javasimon.SimonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(CsapDataApplication.EVENT_API)
@CsapDoc(title = "CSAP Events API", type = CsapDoc.PUBLIC, notes = {
		"Inserts records into CSAP Events database (Mongo)",
		"<a class='pushButton' target='_blank' href='https://github.com/csap-platform/csap-event-services'>learn more</a>",
		"<img class='csapDocImage' src='CSAP_BASE/images/event.png' />" })
public class EventsPosting_api_event {
	private static final int MONGO_DISABLED_INTERVAL_MS = 25000;

	private Logger logger = LoggerFactory.getLogger( getClass() );

	@Inject
	private EventDataWriter eventDataWriter;

	private volatile long lastMongoTimeoutError;

	/**
	 * This is main event post with 300K per day
	 * 
	 * @param eventJson
	 * @param userid
	 * @param pass
	 * @return
	 * @throws Exception
	 */
	@CsapDoc(notes = { "Post event record" }, linkTests = { "Test1" }, linkPostParams = {
			"eventJson=FILE_eventInsert.json,"+SpringAuthCachingFilter.USERID+"=yourUserid," + SpringAuthCachingFilter.PASSWORD + "=yourPassword" })
	@RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
	public String addEventRecord(@RequestParam(value = "eventJson", required = false) String eventJson,
			@RequestParam(value = SpringAuthCachingFilter.USERID, required = false) String userid,
			@RequestParam(value = SpringAuthCachingFilter.PASSWORD, required = false) String pass,
			HttpServletResponse response) throws Exception {

		// logger.debug("Event post called.... {} , {} , {}
		// ",eventJson,userid,pass);
		String result = "";
		
		if ( eventJson == null || userid == null || pass == null) {
			SimonManager.getCounter(  "addEvent.attempt.failed.nullData" ).increase();
			response.setStatus( HttpStatus.BAD_REQUEST.value() );
			result = "Verify request content contains userid, password and event Data";
			
		} else if ( isMongoTimeoutError() ) {
			logger.error( "Mongo down not making call" );
			response.setStatus( HttpStatus.SERVICE_UNAVAILABLE.value() );
			result = EventDataWriter.MONGO_IS_TIMING_OUT;
			SimonManager.getCounter(  "addEvent.attempt.failed.mongoDown" ).increase();

		} else {

			SimonManager.getCounter( "addEvent.attempt" ).increase();
			result = "Added Event: " + eventDataWriter.insertEventData( eventJson, userid );

			if ( EventDataWriter.MONGO_IS_TIMING_OUT.equalsIgnoreCase( result ) ) {
				SimonManager.getCounter( "addEventRecord.mongoDown" ).increase();
				response.setStatus( HttpStatus.SERVICE_UNAVAILABLE.value() );
				lastMongoTimeoutError = System.currentTimeMillis();
				
			} else {
				logger.debug( "Mongo not down" );
				lastMongoTimeoutError = 0;
			}

		}

		return result;
	}

	// Avoid slamming mongo - wait between intervals
	private boolean isMongoTimeoutError() {
		long currentTime = System.currentTimeMillis();
		logger.debug( "diff {} ", (currentTime - lastMongoTimeoutError) );
		if ( lastMongoTimeoutError == 0 ) {
			logger.debug( "lastMongoTimeoutError " + lastMongoTimeoutError );
			return false;
		} else if ( (currentTime - lastMongoTimeoutError) < MONGO_DISABLED_INTERVAL_MS ) {
			return true;
		} else {
			return false;
		}
	}

}
