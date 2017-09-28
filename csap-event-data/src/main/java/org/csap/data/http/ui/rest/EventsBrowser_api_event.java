package org.csap.data.http.ui.rest;

import javax.inject.Inject;

import org.bson.Document;
import org.csap.CsapDataApplication;
import org.csap.data.EventMetaData;
import org.csap.data.db.EventDataReader;
import org.csap.docs.CsapDoc;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.FindIterable;
import com.mongodb.util.JSON;
import org.csap.data.EventJsonConstants;

@RestController
@RequestMapping(CsapDataApplication.EVENT_API)
@CsapDoc(title = "CSAP Events API", type = CsapDoc.PRIVATE, notes = {
	"CSAP events apis provide access to query event records",
	"Used by both user sessions, AGENT use is deprecated but still on some older labs",
	"<a class='pushButton' target='_blank' href='https://github.com/csap-platform/csap-event-services'>learn more</a>",
	"<img class='csapDocImage' src='CSAP_BASE/images/event.png' />"})
public class EventsBrowser_api_event {

	private Logger logger = LoggerFactory.getLogger( getClass() );

	private ObjectMapper jacksonMapper = new ObjectMapper();

	@Inject
	private EventDataReader eventDataReader;

	@CsapDoc(notes = "Search on event",
			linkTests = {"Search with appid etc", "lifecycle appid uiUser search"},
			linkGetParams = {
				"search[value]='appId=csapeng.gen,eventReceivedOn=false,isDataRequired=false'",
				"search[value]='lifecycle=dev,appId=csapeng.gen,metaData.uiUser=paranant,project=Sample Release Package 2,eventReceivedOn=false,isDataRequired=false'"},
			produces = {
				MediaType.APPLICATION_JSON_VALUE
			})
	@RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public String getEventsPaginated(
			@RequestParam(value = "length", required = false, defaultValue = "20") Integer length,
			@RequestParam(value = "start", required = false, defaultValue = "0") Integer start,
			@RequestParam(value = "search[value]", required = false) String searchValue,
			@RequestParam(value = "searchText", required = false) String searchText) {

		logger.debug( "in event records {} , {} , {} , {}", length, start, searchValue, searchText );
		Split split = SimonManager.getStopwatch( "eventGet" ).start();
		String searchString = getSearchString( searchValue, searchText );
		String data = "Error getting data";
		data = getPaginatedEvents( searchString, length, start );
		split.stop();

		return data;
	}

	@CsapDoc(notes = "Count based on filter",
			linkTests = {"Count based on fiter"},
			linkGetParams = {
				"searchText='appId=csapeng.gen,eventReceivedOn=false,isDataRequired=false'"},
			produces = {
				MediaType.APPLICATION_JSON_VALUE
			})
	@RequestMapping(value = "/filteredCount", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getFilteredEventCount(
			@RequestParam(value = "searchText", required = false) String searchString) {

		Split split = SimonManager.getStopwatch( "filteredCount" ).start();
		Split rowCountTimer = SimonManager.getStopwatch( "filteredCount.rowCount" ).start();
		Long filteredCount = eventDataReader.numberOfEvents( searchString );
		rowCountTimer.stop();

		Split dataTimer = SimonManager.getStopwatch( "filteredCount.dataSize" ).start();
		double dataSize = eventDataReader.getDataSize();
		dataTimer.stop();

		Document countResponse = new Document();
		countResponse.put( "recordsFiltered", filteredCount.longValue() );
		if ( filteredCount.longValue() >= 0 ) {
			countResponse.put( "success", true);
		}
		countResponse.put( "dataSize", dataSize );

		split.stop();

		logger.debug( "response: {} ", countResponse );

		return JSON.serialize( countResponse );
	}

	@CsapDoc(notes = "Get data by id ", linkPaths = "/data/idhere")
	@RequestMapping("/data/{id}")
	public String getDataById(@PathVariable("id") String objectId) {
		logger.debug( "Id {}", objectId );
		return JSON.serialize( eventDataReader.getDataForObjectId( objectId ) );
	}

	@CsapDoc(notes = "Get latest event based on category. ", linkTests = {
		"category='/csap/health',appId=csapeng.gen,life=dev"}, linkGetParams = {
		"category='/csap/health',appId=csapeng.gen,life=dev"}, produces = {
		MediaType.APPLICATION_JSON_VALUE})
	@RequestMapping(value = "/latest", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getLatestEvent(@RequestParam(value = "category", required = false) String category,
			@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "keepMostRecent", required = false, defaultValue = "-1") int keepMostRecent,
			@RequestParam(value = "life", required = false) String life) {
		return JSON.serialize( eventDataReader.getLatestEvent( category, appId, project,life, keepMostRecent ) );
	}

	@CsapDoc(notes = "Count the records in db", linkTests = {
		"days=1,appId=csapeng.gen,life=dev,category='/csap/health'"}, linkGetParams = {
		"days=1,appId=csapeng.gen,life=dev,category='/csap/health'"}, produces = {
		MediaType.APPLICATION_JSON_VALUE, "application/javascript"})
	@RequestMapping(value = "/count", produces = MediaType.APPLICATION_JSON_VALUE)
	public String countEvents(
			@RequestParam(value = "days", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "category", required = false) String category) {
		Split split = SimonManager.getStopwatch( "eventCount" ).start();
		String result = "" + eventDataReader.countEvents( days, appId, life, category, project );
		logger.debug( "result {} ", result );
		split.stop();
		return result;
	}

	@CsapDoc(notes = "Count the records in db", linkTests = {"callback=myTestFunction"}, linkGetParams = {
		"callback=myTestFunction"}, produces = {"application/javascript"})
	@RequestMapping(value = "/jcount", produces = "application/javascript")
	public String countEventsJsonp(
			@RequestParam(value = "days", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "category", required = false) String category,
			@RequestParam(value = "callback", required = false, defaultValue = "false") String callback) {
		Split split = SimonManager.getStopwatch( "eventJCount" ).start();
		String result = callback + "(";
		result = result + "{\"count\":" + eventDataReader.countEvents( days, appId, life, category, project ) + " } )";
		split.stop();
		return result;
	}

	@CsapDoc(notes = "Get meta data", linkTests = {"appId=csapeng.gen,life=dev"}, linkGetParams = {
		"appId=csapeng.gen,life=dev"}, produces = {MediaType.APPLICATION_JSON_VALUE})
	@RequestMapping(value = "/metadata", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getEventMetaData(
			@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "fromDate", required = false) String fromDate,
			@RequestParam(value = "toDate", required = false) String toDate
	) {
		logger.debug( "Loading Meta" );
		String result = "";
		try {
			EventMetaData metaData = eventDataReader.getEventMetaData( appId, life, fromDate, toDate, 2 );			
			result = jacksonMapper.writeValueAsString( metaData );
		} catch ( Exception e ) {
			SimonManager.getCounter( EventDataReader.SEARCH_FILTER_KEY + "errors" ).increase();
			logger.error( "Exception while getting meta data", e );
		}
		return result;
	}

	private String getPaginatedEvents(String searchString, int length, int startIndex) {
		String json = "";
		String data = "";

		ObjectNode rootNode = jacksonMapper.createObjectNode();
		// try (DBCursor cursor =
		// eventDataReader.getFilteredEvents(searchString,length,startIndex)) {
		FindIterable<Document> cursor = eventDataReader.getEventsByCriteria( searchString, length, startIndex );
		data = "\"data\": " + JSON.serialize( cursor ) + "";
		try {
			rootNode.put( "aaData", "value" );
			rootNode.put( EventJsonConstants.DATA_TABLES_RECORDS_TOTAL, eventDataReader.getTotalEvents( searchString ) );
			// rootNode.put("recordsFiltered",
			// eventDataReader.getTotalFilteredEvents(searchString));
			// event count can take a while - hard code a dummy value
			rootNode.put( EventJsonConstants.DATA_TABLES_RECORDS_FILTERED, -1 );
			json = jacksonMapper.writeValueAsString( rootNode );
		} catch ( Exception e ) {
			logger.error( "Exception while converting json", e );
		}
		json = json.replace( "\"aaData\":\"value\"", data );
		return json;
	}

	private String getSearchString(String searchValue, String searchText) {
		String searchString = "";
		if ( null != searchValue && searchValue.trim().length() > 0 ) {
			searchString = searchValue;
		} else if ( null != searchText && searchText.trim().length() > 0 ) {
			searchString = searchText;
		}
		return searchString;
	}
}
