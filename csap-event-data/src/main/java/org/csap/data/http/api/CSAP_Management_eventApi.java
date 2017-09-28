package org.csap.data.http.api;

import javax.inject.Inject;
import org.bson.Document;
import org.csap.CsapDataApplication;
import org.csap.data.db.EventDataReader;
import org.csap.data.db.EventDataWriter;
import org.csap.docs.CsapDoc;
import org.csap.integations.CsapSecurityConfiguration;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.AggregateIterable;
import com.mongodb.util.JSON;
import java.util.List;
import org.springframework.cache.annotation.Cacheable;

/**
 * EventsService will eventually move in here.
 *
 *
 */
@RestController
@RequestMapping(CsapDataApplication.API_URL)
@CsapDoc(title = "CSAP Events API", type = CsapDoc.PUBLIC, notes = { "Provides queries for  CSAP Analytics portal",
		"<a class='pushButton' target='_blank' href='https://github.com/csap-platform/csap-event-services'>learn more</a>",
		"<img class='csapDocImage' src='CSAP_BASE/images/event.png' />" })
public class CSAP_Management_eventApi {

	private Logger logger = LoggerFactory.getLogger( getClass() );
	private ObjectMapper jacksonMapper = new ObjectMapper();
	@Inject
	private EventDataReader eventDataReader;

	@Inject
	private EventDataWriter eventDataWriter;

	@Autowired(required = false)
	private CsapSecurityConfiguration securityConfig;

	@CsapDoc(notes = "Get latest event based on category. ", linkTests = { "category='/csap/health',appId=csapeng.gen,life=dev",
			"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction" }, linkGetParams = {
					"category='/csap/health',appId=csapeng.gen,life=dev",
					"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction" }, produces = {
							MediaType.APPLICATION_JSON_VALUE, "application/javascript" })
	@RequestMapping(value = "/latest", produces = MediaType.APPLICATION_JSON_VALUE)
	public Document getLatest (	@RequestParam("category") String category,
								@RequestParam(value = "appId", required = false) String appId,
								@RequestParam(value = "project", required = false) String project,
								@RequestParam(value = "keepMostRecent", required = false, defaultValue = "-1") int keepMostRecent,
								@RequestParam(value = "life", required = false) String life ) {
		Document latestEvent = eventDataReader.getLatestEvent( category, appId, project, life, keepMostRecent );
		return latestEvent;
	}

	@CsapDoc(notes = "Get latest event based on category. ", linkTests = { "category='/csap/health',appId=csapeng.gen,life=dev",
			"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction" }, linkGetParams = {
					"category='/csap/health',appId=csapeng.gen,life=dev",
					"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction" }, produces = {
							MediaType.APPLICATION_JSON_VALUE, "application/javascript" })
	@RequestMapping(value = "/latestCached", produces = MediaType.APPLICATION_JSON_VALUE)
	public Document getLatestCached (	@RequestParam("category") String category,
										@RequestParam(value = "appId", required = false) String appId,
										@RequestParam(value = "project", required = false) String project,
										@RequestParam(value = "keepMostRecent", required = false, defaultValue = "-1") int keepMostRecent,
										@RequestParam(value = "life", required = false) String life ) {
		Document latestEvent = eventDataReader.getLatestCachedEvent( category, appId, project, life, keepMostRecent );
		return latestEvent;
	}

	@CsapDoc(notes = "Get app ids ", linkTests = { "days=4,category='/csap/health'", "callback=myTestFunction" }, linkGetParams = {
			"numDays=4,category='/csap/health'",
			"callback=myTestFunction" }, produces = { MediaType.APPLICATION_JSON_VALUE, "application/javascript" })
	@RequestMapping(value = "/appIds", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<Document> getAppIds (
										@RequestParam(value = "numDays", required = false, defaultValue = "7") int numDays,
										@RequestParam(value = "category", required = false) String category ) {

		return eventDataReader.getAppIdProject( numDays, category );
	}

	@CsapDoc(notes = "Get lifecycle for app ids", linkTests = { "appId=csapeng.gen,numDays=4,category='/csap/health'",
			"appId=csapeng.gen,callback=myTestFunction" }, linkGetParams = { "appId=csapeng.gen,numDays=4,category='/csap/health'",
					"appId=csapeng.gen,callback=myTestFunction" }, produces = { MediaType.APPLICATION_JSON_VALUE,
							"application/javascript" })
	@RequestMapping(value = "/lifecycles", produces = MediaType.APPLICATION_JSON_VALUE)
	public Document getLifeForAppId (	@RequestParam("appId") String appId,
										@RequestParam(value = "numDays", required = false, defaultValue = "7") int numDays,
										@RequestParam(value = "category", required = false) String category ) {
		return eventDataReader.getLifecycles( appId, numDays, category );
	}

	@CsapDoc(notes = "Count the records in db", linkTests = { "days=1,appId=csapeng.gen,life=dev,category='/csap/health'",
			"callback=myTestFunction" }, linkGetParams = { "days=1,appId=csapeng.gen,life=dev,category='/csap/health'",
					"callback=myTestFunction" }, produces = { MediaType.APPLICATION_JSON_VALUE, "application/javascript" })
	@RequestMapping(value = "/count", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode countEvents (	@RequestParam(value = "days", required = false, defaultValue = "1") Integer days,
									@RequestParam(value = "appId", required = false) String appId,
									@RequestParam(value = "project", required = false) String project,
									@RequestParam(value = "life", required = false) String life,
									@RequestParam(value = "category", required = false) String category ) {
		Split split = SimonManager.getStopwatch( "eventApiCount" ).start();
		long result = eventDataReader.countEvents( days, appId, life, category, project );
		ObjectNode node = jacksonMapper.createObjectNode();
		node.put( "count", result );
		split.stop();
		return node;
	}

	@CsapDoc(notes = "Get latest event Json serilized ", linkTests = { "category='/csap/health',appId=csapeng.gen,life=dev",
			"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction" }, linkGetParams = {
					"category='/csap/health',appId=csapeng.gen,life=dev",
					"category='/csap/health',appId=csapeng.gen,life=dev,callback=myTestFunction" }, produces = {
							MediaType.APPLICATION_JSON_VALUE, "application/javascript" })
	@RequestMapping(value = "/latestWithJson", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getLatestEventWithJson (	@RequestParam("category") String category,
											@RequestParam("appId") String appId,
											@RequestParam(value = "project", required = false) String project,
											@RequestParam(value = "keepMostRecent", required = false, defaultValue = "-1") int keepMostRecent,
											@RequestParam("life") String life ) {
		return JSON.serialize( eventDataReader.getLatestEvent( category, appId, project, life, keepMostRecent ) );
	}

}
