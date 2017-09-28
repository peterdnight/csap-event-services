package org.csap.data.http.ui.rest;


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
import org.springframework.cache.CacheManager;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.AggregateIterable;

@RestController
@RequestMapping(CsapDataApplication.API_URL)
@CsapDoc(title = "Events API", type = CsapDoc.PRIVATE, notes = {
		"Supports advanced operations in events dashboard",
		"<a class='pushButton' target='_blank' href='https://github.com/csap-platform/csap-event-services'>learn more</a>",
		"<img class='csapDocImage' src='CSAP_BASE/images/event.png' />" })
public class EventsBrowser_eventApi {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private ObjectMapper jacksonMapper = new ObjectMapper();
	@Inject
	private EventDataReader eventDataReader;

	@Inject
	private EventDataWriter eventDataWriter;

	@Autowired(required = false)
	private CsapSecurityConfiguration securityConfig;
	
	@Inject 
	private CacheManager cacheManager;
	
	
//	@CsapDoc(notes = "Checks if the user is authorized ")
//	@RequestMapping(value = "/isAuthorized", produces = MediaType.APPLICATION_JSON_VALUE)
//	public String isAuthorized() {
//		if (null == securityConfig) {
//			return "{\"isAuthorized\" : " + true + "}";
//		}
//
//		Collection authorities = SecurityContextHolder.getContext()
//				.getAuthentication()
//				.getAuthorities();
//		boolean isAuthorized = authorities.toString().contains(securityConfig.adminGroups);
//		// logger.info("isAuthorized {}",isAuthorized);
//		return "{\"isAuthorized\" : " + isAuthorized + "}";
//	}

	@RequestMapping(value = "/update", produces = MediaType.APPLICATION_JSON_VALUE)
	public String update(@RequestParam("eventData") String eventData,
			@RequestParam("objectId") String objectId) {
		logger.debug("data {}", eventData);
		logger.debug("Object id {}", objectId);
		int docsEffected = eventDataWriter.updateEvent(objectId, eventData);
		return "{\"result\": \"Updated " + docsEffected + " record \"}";
	}

	@RequestMapping(value = "/insert", method = RequestMethod.POST, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
	public String insertEventRecord(@RequestParam("eventJson") String eventJson,
			@RequestParam("appId") String appId,
			@RequestParam("life") String life,
			@RequestParam("project") String project,
			@RequestParam("summary") String summary,
			@RequestParam("category") String category) throws Exception {
		Split split = SimonManager.getStopwatch("eventInsert").start();
		logger.debug("event json --> {} appId {} life {} project {} summary {},category {}", eventJson, appId, life,
				project, summary, category);
		String result = eventDataWriter.insertEvent(eventJson, appId, project, life, summary, category);
		logger.debug("Inserted objid {} ", result);
		String output = "";
		if (result.equalsIgnoreCase("Invalid data")) {
			output = "{\"result\": \"Error inserting. " + result + "\"}";
		} else {
			output = "{\"result\": \"Inserted objid " + result + "\"}";
		}
		split.stop();
		return output;
	}

	@CsapDoc(notes = "Delete using serach criteria ",
				linkPaths = "/removeThis/deleteBySearch",
				linkGetParams = {"search[value]='lifecycle=dev,appId=csapeng.gen,metaData.uiUser=testUser,project=Sample Release Package 2'"}
	)
	@RequestMapping(value = "/deleteBySearch", produces = MediaType.APPLICATION_JSON_VALUE)
	public String deleteBySearchString(@RequestParam("searchString") String searchString) {
		long docsEffected = eventDataWriter.deleteEventByFilter(searchString);
		return "{\"result\": \"Deleted " + docsEffected + " record \"}";
	}
	@CsapDoc(notes = "Delete by object id ",
			linkPaths = "/removeThis/delete",
			linkGetParams = {"objectId=objIdHere"}
			)
	@RequestMapping(value = "/delete", produces = MediaType.APPLICATION_JSON_VALUE)
	public String delete(@RequestParam("objectId") String objectId) {
		long docsEffected = eventDataWriter.deleteEventById(objectId);
		logger.debug("object id {}", objectId);
		return "{\"result\": \"Deleted " + docsEffected + " record \"}";
	}
	@CsapDoc(notes = "Retrieve log rotate report",
			linkTests = {"appId=csapeng.gen,life=dev", "callback=myTestFunction"},
			linkGetParams = {"appId=csapeng.gen,life=dev", "appId=csapeng.gen,life=dev,callback=myTestFunction"},
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/javascript"})
	@RequestMapping(value = "/logRotateReport", produces = MediaType.APPLICATION_JSON_VALUE)
	public AggregateIterable<Document> retrieveCustomLogRotateReport(@RequestParam(value="category",required=false,defaultValue="/csap/reports/logRotate") String category,
												@RequestParam(value="appId",required=false) String appId,
												@RequestParam(value="life",required=false) String life,
												@RequestParam(value="project",required=false) String project,
												@RequestParam(value="fromDate",required=false) String fromDate,
												@RequestParam(value="toDate",required=false) String toDate
			){
		
		return eventDataReader.retrieveLogRotateReport(appId, project, life, category,fromDate,toDate);
		
	}
	
	@CsapDoc(notes = "Get event by id ")
	@RequestMapping(value = "/getById",produces = MediaType.APPLICATION_JSON_VALUE)
	public Document getDataById( @RequestParam(value="id",required=true)  String objectId){
		logger.debug("Id {}",objectId);
		return  eventDataReader.getEventByObjectId(objectId);
	}
	
}
