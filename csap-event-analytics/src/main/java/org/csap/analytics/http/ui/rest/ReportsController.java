package org.csap.analytics.http.ui.rest;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.csap.analytics.CsapAnalyticsApplication;
import org.csap.analytics.db.AnalyticsDbReader;
import org.csap.analytics.db.AnalyticsHelper;
import org.csap.analytics.db.CsapAdoptionReportBuilder;
import org.csap.analytics.db.GlobalAnalyticsDbReader;
import org.csap.analytics.db.TrendingReportHelper;
import org.csap.analytics.misc.BusinessProgramDisplayInfo;
import org.csap.analytics.misc.GraphData;
import org.csap.docs.CsapDoc;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.utils.SimonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.AggregateIterable;
import com.mongodb.util.JSON;

@RestController
@RequestMapping(CsapAnalyticsApplication.REPORT_URL)
@CsapDoc(title = "CSAP reports API", type = CsapDoc.PUBLIC,
		notes = {"CSAP analytics reports api", "<a class='pushButton' target='_blank' href='https://github.com/csap-platform/csap-core/wiki'>learn more</a>"})
public class ReportsController {

	private static final String REPORT_DATA_ALL_SUMMARY = "reportData.ALL.summary";
	private static final String REPORT_DATA_ALL_TREND = "reportData.ALL.trend";
	private Logger logger = LoggerFactory.getLogger( getClass() );
	public String service = "service";

	@Inject
	private AnalyticsDbReader analyticsDbReader;


	@Inject
	private ObjectMapper jacksonMapper = new ObjectMapper();

	@Inject
	private AnalyticsHelper analyticsHelper;

	@Inject
	private CsapAdoptionReportBuilder adoptionReportBuilder;

	@Inject
	private GlobalAnalyticsDbReader globalAnalyticsReader;

	@CsapDoc(notes = "Get host and service report ",
			linkTests = {"JSON", "JSONP"},
			linkGetParams = {"a=b", "callback=myTestFunction"},
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/javascript"}
	)
	@RequestMapping(value = "/reportCounts", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode reportCounts() {
		return globalAnalyticsReader.reportCounts();
	}

	@CsapDoc(notes = "Get global analytics report")
	@RequestMapping(value = "/adoption/current", produces = MediaType.APPLICATION_JSON_VALUE)
	public String adoptionCurrent(@RequestParam(value = "numDays", required = false, defaultValue = "7") Integer numDays) {
		AggregateIterable<Document> results = globalAnalyticsReader.getAnalyticsData( numDays );
		return JSON.serialize( results );
	}

	@CsapDoc(notes = "graph data for global analytics")
	@RequestMapping(value = "/adoption/trends", produces = MediaType.APPLICATION_JSON_VALUE)
	public GraphData adoptionTrends(@RequestParam(value = "numDays", required = false, defaultValue = "30") Integer numDays) {
		GraphData graphdata = globalAnalyticsReader.buildProjectAdoptionTrends( numDays );
		return graphdata;
	}
	
	@CsapDoc(notes = "Health of system")
	@RequestMapping(value = "/health", produces = MediaType.APPLICATION_JSON_VALUE)
	public Map<String, Map> getHealthMessages() throws Exception {
		Map<String, Map> results = globalAnalyticsReader.getHealthInfo();
		return results;
	}

	@CsapDoc(notes = "Health of project",
			linkTests = {"projectName='CSAP Engineering'"},
			linkGetParams = {"projectName='CSAP Engineering'"},
			produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@RequestMapping(value = "/healthMessage", produces = MediaType.APPLICATION_JSON_VALUE)
	public String getHealthMetrics(@RequestParam(value = "projectName", required = true) String projectName) throws Exception {
		return globalAnalyticsReader.getHealthErrorMessages( projectName );
	}

	@CsapDoc(notes = "Instance information",
			linkTests = {"projectName='CSAP Engineering',appId=csapeng.gen"},
			linkGetParams = {"projectName='CSAP Engineering',appId=csapeng.gen"},
			produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@RequestMapping(value = "/package-summary", produces = MediaType.APPLICATION_JSON_VALUE)
	public Map<String, Document> getInstanceInfo(@RequestParam(value = "projectName", required = true) String projectName,
			@RequestParam(value = "appId", required = true) String appId) throws Exception {

		return globalAnalyticsReader.getPackageSumnmarysByLife( projectName, appId );
	}


	@CsapDoc(notes = "Display names for project")
	@RequestMapping(value = "/displayNames", produces = MediaType.APPLICATION_JSON_VALUE)
	public Map<String, Document> getDisplayNames() {
		return analyticsHelper.getAnalyticsSettings();
	}

	@CsapDoc(notes = "Display information.Deprecated will be removed")
	@RequestMapping(value = "/displayInformation", produces = MediaType.APPLICATION_JSON_VALUE)
	public Map<String, BusinessProgramDisplayInfo> getDisplayNameInformation() {
		return analyticsHelper.getDisplayNameInfo();
	}

	@CsapDoc(notes = "This is deprecated. Will be removed")
	@RequestMapping(value = "/saveShowHide", produces = MediaType.APPLICATION_JSON_VALUE)
	public void setShowHide(
			@RequestParam(value = "packageName", defaultValue = "") String packageName,
			@RequestParam(value = "isHidden", defaultValue = "false") boolean isHidden
	) {
		analyticsHelper.saveOrUpdateHide( packageName, isHidden );
	}

	@CsapDoc(notes = "This is depreaceted will be removed")
	@RequestMapping(value = "/saveHealthMessage", produces = MediaType.APPLICATION_JSON_VALUE)
	public void saveHealthMessagetValue(@RequestParam(value = "packageName", defaultValue = "") String packageName,
			@RequestParam(value = "life", defaultValue = "") String life,
			@RequestParam(value = "saveHealthMessage", defaultValue = "false") boolean saveHealthMessage) {
		analyticsHelper.saveOrUpdateHealth( packageName, life, saveHealthMessage );
	}

	@CsapDoc(notes = "This is deprecated will be removed")
	@RequestMapping(value = "/saveDisplayName", produces = MediaType.APPLICATION_JSON_VALUE)
	public void setDisplayName(@RequestParam(value = "packageName", defaultValue = "") String packageName, @RequestParam(value = "displayName", defaultValue = "") String displayName) {
		analyticsHelper.saveOrUpdateDisplayName( packageName, displayName );
	}

	@CsapDoc(notes = "user id report",
			linkTests = {"project='CSAP Engineering'", "project='CSAP Engineering',trending=true"},
			linkGetParams = {"project='CSAP Engineering'", "project='CSAP Engineering',trending=true"},
			produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	//@Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	//@Cacheable(value = "userIdReports")
	@RequestMapping(value = "/userid", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode userIdReports(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "numHours", required = false, defaultValue = "0") Integer hours,
			@RequestParam(value = "trending", required = false, defaultValue = "false") Boolean trending,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet) {
		ObjectNode rootNode = jacksonMapper.createObjectNode();
		try {
			String data = "";
			if ( trending ) {

				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
				AggregateIterable<Document> results = analyticsDbReader.userActivityTrendingReport( appId, project, life, days, dateOffSet );
				data = JSON.serialize( results );
				timerAll.stop();
				logger.info( "User Trending: {}", SimonUtils.presentNanoTime( timerAll.runningFor() ) );
			} else {

				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_SUMMARY ).start();
				AggregateIterable<Document> results = analyticsDbReader.userActivityReport( appId, project, life, days );
				data = JSON.serialize( results );
				timerAll.stop();
				logger.info( "User Activity: {}", SimonUtils.presentNanoTime( timerAll.runningFor() ) );
				
			}
			JsonNode userArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", userArrayNode );
			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "userreport", null );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		return rootNode;
	}

	@CsapDoc(notes = "vm report",
			linkTests = {"project='CSAP Engineering'", "project='CSAP Engineering',trending=true"},
			linkGetParams = {"project='CSAP Engineering'", "project='CSAP Engineering',trending=true"},
			produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	@RequestMapping(value = "/vm", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode getVmReports(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "resource", required = false, defaultValue = "resource_30") String resource,
			@RequestParam(value = "trending", required = false, defaultValue = "false") Boolean trending,
			@RequestParam(value = "metricsId", required = false) String[] metricsId,
			@RequestParam(value = "perVm", required = false, defaultValue = "false") Boolean perVm,
			@RequestParam(value = "allVmTotal", required = false) String allVmTotal,
			@RequestParam(value = "divideBy", required = false) String[] divideBy,
			@RequestParam(value = "top", required = false, defaultValue = "0") int top,
			@RequestParam(value = "low", required = false, defaultValue = "0") int low
	) {
		ObjectNode rootNode = jacksonMapper.createObjectNode();
		try {
			String data = "";
			if ( trending && ArrayUtils.isNotEmpty( metricsId ) ) {

				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
				AggregateIterable<Document> results = analyticsDbReader.getVmTrendingReport( appId, project, life, metricsId, divideBy, allVmTotal, perVm, top, low, days, dateOffSet );
				data = JSON.serialize( results );
				timerAll.stop();
			} else {
				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_SUMMARY ).start();
				AggregateIterable<Document> results = analyticsDbReader.getVmReport( appId, project, life, days, dateOffSet );
				data = JSON.serialize( results );
				timerAll.stop();
			}

			JsonNode hostArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", hostArrayNode );

			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "hostreport", null );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		return rootNode;
	}

	@Inject
	private TrendingReportHelper trendingReportHelper;

	@CsapDoc(notes = "Top hosts",
			linkTests = {"project='CSAP Engineering',metricsId=process.topCpu,hosts=2,appId=csapeng.gen"},
			linkGetParams = {"project='CSAP Engineering',metricsId=process.topCpu,hosts=2,appId=csapeng.gen"},
			produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	@RequestMapping(value = "/top", produces = MediaType.APPLICATION_JSON_VALUE)
	public List topHosts(@RequestParam(value = "appId", required = true) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "hosts", required = false, defaultValue = "0") int numHosts,
			@RequestParam(value = "metricsId", required = true) String[] metricsId,
			@RequestParam(value = "reportType", required = false, defaultValue = "topReport") String reportType
	) {
		List results = null;
		try {
			Split split = SimonManager.getStopwatch( "topReport" ).start();
			String category = getCategory( metricsId[0] );
			String[] actualMetricsId = getActualMetricsId( metricsId );
			boolean unwindSummary = unwindSummary( metricsId[0] );
			String serviceNameFilter = getServiceNameFilter( metricsId[0] );
			String[] divideBy = getDivideBy( metricsId[0] );
			results = trendingReportHelper.topHosts( appId, project, life,
					actualMetricsId, divideBy, numHosts,
					-1, category, serviceNameFilter, unwindSummary, days, dateOffSet );
			split.stop();
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		return results;
	}

	@CsapDoc(notes = "Low hosts",
			linkTests = {"project='CSAP Engineering',metricsId=process.topCpu,hosts=2,appId=csapeng.gen"},
			linkGetParams = {"project='CSAP Engineering',metricsId=process.topCpu,hosts=2,appId=csapeng.gen"},
			produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	@RequestMapping(value = "/low", produces = MediaType.APPLICATION_JSON_VALUE)
	public List lowHosts(@RequestParam(value = "appId", required = true) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "hosts", required = false, defaultValue = "0") int numHosts,
			@RequestParam(value = "metricsId", required = true) String[] metricsId,
			@RequestParam(value = "reportType", required = false, defaultValue = "lowReport") String reportType
	) {
		List results = null;
		try {
			Split split = SimonManager.getStopwatch( "lowReport" ).start();
			String category = getCategory( metricsId[0] );
			String[] actualMetricsId = getActualMetricsId( metricsId );
			boolean unwindSummary = unwindSummary( metricsId[0] );
			String serviceNameFilter = getServiceNameFilter( metricsId[0] );
			String[] divideBy = getDivideBy( metricsId[0] );
			results = trendingReportHelper.topHosts( appId, project, life,
					actualMetricsId, divideBy, numHosts,
					1, category, serviceNameFilter, unwindSummary, days, dateOffSet );
			split.stop();
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		return results;
	}

	private String[] getDivideBy(String metricsId) {
		if ( metricsId.startsWith( "health." ) ) {
			return null;
		} else {
			return new String[]{"numberOfSamples"};
		}
	}

	private boolean unwindSummary(String metricsId) {
		if ( metricsId.startsWith( "vm." ) || metricsId.startsWith( "health." ) ) {
			return false;
		} else {
			return true;
		}
	}

	private String getServiceNameFilter(String metricsId) {
		String serviceNameFilter = "";
		if ( metricsId.startsWith( "process." ) || metricsId.startsWith( "jmx." ) ) {
			if ( metricsId.contains( "_" ) ) {
				serviceNameFilter = metricsId.substring( metricsId.indexOf( "_" ) + 1 );
			}
		} else if ( metricsId.startsWith( "jmxCustom." ) ) {
			String[] metricsIdArr = metricsId.split( "\\." );
			if ( metricsIdArr.length >= 3 ) {
				serviceNameFilter = metricsIdArr[1];
			}
		}
		return serviceNameFilter;
	}

	private String[] getActualMetricsId(String[] metricsId) {
		String[] actualMetricsId = new String[metricsId.length];
		if ( metricsId[0].startsWith( "vm." ) || metricsId[0].startsWith( "health." ) ) {
			if ( metricsId[0].contains( "coresActive" ) ) {
				actualMetricsId = new String[2];
				actualMetricsId[0] = "totalUsrCpu";
				actualMetricsId[1] = "totalSysCpu";
			} else {
				for ( int i = 0; i < metricsId.length; i++ ) {
					String key = metricsId[i];
					String[] keyArr = key.split( "\\." );
					if ( keyArr.length == 2 ) {
						actualMetricsId[i] = keyArr[1];
					}
				}
			}
		} else if ( metricsId[0].startsWith( "process." ) || metricsId[0].startsWith( "jmx." ) ) {
			// 1. process 2. attribute name 3. _serviceName
			for ( int i = 0; i < metricsId.length; i++ ) {
				String metricsKey = metricsId[i];
				String actualMetricsKey = "";
				if ( metricsKey.contains( "_" ) ) {
					actualMetricsKey = metricsKey.substring( (metricsKey.indexOf( "." ) + 1), metricsKey.indexOf( "_" ) );
				} else {
					actualMetricsKey = metricsKey.substring( metricsKey.indexOf( "." ) + 1 );
				}
				if ( StringUtils.isNotBlank( actualMetricsKey ) ) {
					actualMetricsId[i] = actualMetricsKey;
				}
			}
		} else if ( metricsId[0].startsWith( "jmxCustom." ) ) {
			// 1. jmxCustom 2.service name 3.metrics ID
			for ( int i = 0; i < metricsId.length; i++ ) {
				String metricsKey = metricsId[i];
				String[] metricsKeyArr = metricsKey.split( "\\." );
				if ( metricsKeyArr.length >= 3 ) {
					actualMetricsId[i] = metricsKeyArr[2];
				}
			}
		}
		return actualMetricsId;
	}

	private String getCategory(String metricsId) {
		if ( metricsId.startsWith( "vm." ) ) {
			return "/csap/reports/host/daily";
		} else if ( metricsId.startsWith( "process." ) ) {
			return "/csap/reports/process/daily";
		} else if ( metricsId.startsWith( "jmxCustom." ) ) {
			return "/csap/reports/jmxCustom/daily";
		} else if ( metricsId.startsWith( "jmx." ) ) {
			return "/csap/reports/jmx/daily";
		} else if ( metricsId.startsWith( "health." ) ) {
			return "/csap/reports/health";
		}
		return "";
	}

	@CsapDoc(notes = "Get core report ",
			linkTests = {"appId=csapeng.gen", "appId=csapeng.gen,top=1"},
			linkGetParams = {"appId=csapeng.gen", "appId=csapeng.gen,top=1"},
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_JSON_VALUE}
	)
	@Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	@RequestMapping(value = "/custom/core", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode coreTrending(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "resource", required = false, defaultValue = "resource_30") String resource,
			@RequestParam(value = "trending", required = true, defaultValue = "true") Boolean trending,
			@RequestParam(value = "perVm", required = false, defaultValue = "false") Boolean perVm,
			@RequestParam(value = "top", required = false, defaultValue = "0") int top,
			@RequestParam(value = "low", required = false, defaultValue = "0") int low) {
		Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
		ObjectNode rootNode = jacksonMapper.createObjectNode();
		try {
			String[] metricsId = {"totalSysCpu", "totalUsrCpu"};
			AggregateIterable<Document> results = analyticsDbReader.getCoreUsedTrendingReport( appId, project, life, metricsId, perVm, top, low, days, dateOffSet );
			String data = JSON.serialize( results );
			JsonNode hostArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", hostArrayNode );
			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "hostreport", null );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		timerAll.stop();
		return rootNode;
	}

	@CsapDoc(notes = "Get health report ",
			linkTests = {"appId=csapeng.gen", "appId=csapeng.gen,top=1"},
			linkGetParams = {"appId=csapeng.gen", "appId=csapeng.gen,top=1"},
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_JSON_VALUE}
	)
	@Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	@RequestMapping(value = "/custom/health", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode healthTrending(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "trending", required = true, defaultValue = "true") Boolean trending,
			@RequestParam(value = "perVm", required = true, defaultValue = "false") Boolean perVm,
			@RequestParam(value = "top", required = false, defaultValue = "0") int top,
			@RequestParam(value = "low", required = false, defaultValue = "0") int low,
			@RequestParam(value = "category", required = false, defaultValue = "/csap/reports/health") String category
	) {

		Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
		ObjectNode rootNode = jacksonMapper.createObjectNode();
		try {
			AggregateIterable<Document> results = analyticsDbReader.getVmHealthReport( appId, project, life,
					perVm, top, low,
					days, dateOffSet, category );
			String data = JSON.serialize( results );
			JsonNode hostArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", hostArrayNode );
			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "hostreport", null );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}

		timerAll.stop();
		return rootNode;
	}

	@CsapDoc(notes = "Get log rotate report ",
			linkTests = {"appId=csapeng.gen,metricsId=MeanSeconds", "appId=csapeng.gen,serviceName=data,metricsId=MeanSeconds"},
			linkGetParams = {"appId=csapeng.gen,metricsId=MeanSeconds", "appId=csapeng.gen,serviceName=data,metricsId=MeanSeconds"},
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_JSON_VALUE}
	)
	@Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	@RequestMapping(value = "/custom/logRotate", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode logRotateTrending(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "serviceName", required = false) String serviceNameFilter,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "trending", required = false, defaultValue = "false") Boolean trending,
			@RequestParam(value = "metricsId", required = false) String[] metricsId,
			@RequestParam(value = "divideBy", required = false) String[] divideBy,
			@RequestParam(value = "category", required = false, defaultValue = "/csap/reports/logRotate") String category) {

		Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
		ObjectNode rootNode = jacksonMapper.createObjectNode();
		try {
			AggregateIterable<Document> results = analyticsDbReader.getTrendingReportByCategory( appId, project, life, serviceNameFilter, category, metricsId, divideBy, null, days, dateOffSet );
			if ( null != results ) {
				String data = JSON.serialize( results );
				JsonNode hostArrayNode = jacksonMapper.readTree( data );
				rootNode.set( "data", hostArrayNode );
			}
			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "logRotateReport", serviceNameFilter );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		
		timerAll.stop() ;
		return rootNode;
	}

	@CsapDoc(notes = "Service report",
			linkTests = {"a=b", "appId=csapeng.gen,serviceName=data,metricsId=socketCount"},
			linkGetParams = {"a=b", "appId=csapeng.gen,serviceName=data,metricsId=socketCount"},
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_JSON_VALUE}
	)

	@Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	@RequestMapping(value = "/service", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode serviceReports(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "host", required = false) String host,
			@RequestParam(value = "serviceName", required = false) String serviceNameFilter,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "trending", required = false, defaultValue = "false") Boolean trending,
			@RequestParam(value = "metricsId", required = false) String[] metricsId,
			@RequestParam(value = "divideBy", required = false) String[] divideBy,
			@RequestParam(value = "allVmTotal", required = false) String allVmTotal,
			@RequestParam(value = "category", required = false,
					defaultValue = "/csap/reports/process/daily") String category) {

		logger.debug( "OS Resource Report for host: {}, number of Days: {} , offSet: {}", host, days, dateOffSet );
		ObjectNode rootNode = jacksonMapper.createObjectNode();
		try {
			String data = "";
			if ( trending ) {

				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
				AggregateIterable<Document> results = analyticsDbReader.getTrendingReportByCategory( appId, project, life, serviceNameFilter, category, metricsId, divideBy, allVmTotal, days, dateOffSet );
				data = JSON.serialize( results );
				timerAll.stop();
			} else {

				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_SUMMARY ).start();
				AggregateIterable<Document> results = analyticsDbReader.findSummaryReportDataUsingCategory(
						appId, project, life, host, serviceNameFilter, category,
						days, dateOffSet );

				data = JSON.serialize( results );

				timerAll.stop();
			}
			JsonNode hostArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", hostArrayNode );

			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "servicereport", serviceNameFilter );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		return rootNode;
	}

	@CsapDoc(notes = "Service detail report",
			linkTests = {"a=b", "appId=csapeng.gen,serviceName=data,metricsId=socketCount", "appId=csapeng.gen,serviceName=data,metricsId=socketCount,trending=true"},
			linkGetParams = {"a=b", "appId=csapeng.gen,serviceName=data,metricsId=socketCount", "appId=csapeng.gen,serviceName=data,metricsId=socketCount,trending=true"},
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_JSON_VALUE}
	)
	@Cacheable(CsapAnalyticsApplication.DETAILS_REPORT_CACHE)
	@RequestMapping(value = "/service/detail", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode serviceDetailReports(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "serviceName", required = false) String serviceNameFilter,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "trending", required = false, defaultValue = "false") Boolean trending,
			@RequestParam(value = "metricsId", required = false) String[] metricsId,
			@RequestParam(value = "divideBy", required = false) String[] divideBy,
			@RequestParam(value = "allVmTotal", required = false) String allVmTotal,
			@RequestParam(value = "category", required = false, defaultValue = "/csap/reports/process/daily") String category
	) {
		ObjectNode rootNode = jacksonMapper.createObjectNode();

		try {
			AggregateIterable<Document> results = null;
			if ( trending ) {
				
				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
				results = analyticsDbReader.getTrendingReportByCategory( appId, project, life, serviceNameFilter, category, metricsId, divideBy, allVmTotal, days, dateOffSet );
				timerAll.stop() ;
			} else {
				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_SUMMARY ).start();
				results = analyticsDbReader.findDetailReportDataUsingCategory( appId, project, life, serviceNameFilter, category, days, dateOffSet );
				timerAll.stop() ;
			}
			String data = JSON.serialize( results );
			JsonNode hostArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", hostArrayNode );
			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "servicereport", serviceNameFilter );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		return rootNode;
	}

	 @Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	@RequestMapping(value = "/jmx", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode jmxReports(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "host", required = false) String host,
			@RequestParam(value = "serviceName", required = false) String serviceNameFilter,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "trending", required = false, defaultValue = "false") Boolean trending,
			@RequestParam(value = "metricsId", required = false) String[] metricsId,
			@RequestParam(value = "divideBy", required = false) String[] divideBy,
			@RequestParam(value = "allVmTotal", required = false) String allVmTotal,
			@RequestParam(value = "category", required = false, defaultValue = "/csap/reports/jmx/daily") String category
	) {
		ObjectNode rootNode = jacksonMapper.createObjectNode();

		try {
			String data = "";
			if ( trending ) {

				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
				AggregateIterable<Document> results = analyticsDbReader.getTrendingReportByCategory( appId, project, life, serviceNameFilter, category, metricsId, divideBy, allVmTotal, days, dateOffSet );
				data = JSON.serialize( results );
				timerAll.stop();
			} else {

				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_SUMMARY ).start();
				AggregateIterable<Document> results = analyticsDbReader.findSummaryReportDataUsingCategory( appId, project, life, host, serviceNameFilter, category, days, dateOffSet );
				data = JSON.serialize( results );
				timerAll.stop() ;
			}
			JsonNode hostArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", hostArrayNode );
			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "jmxreport", serviceNameFilter );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
			rootNode.put( "error", "Error getting jmx data" );
		}
		return rootNode;
	}

	@Cacheable(CsapAnalyticsApplication.DETAILS_REPORT_CACHE)
	@RequestMapping(value = "/jmx/detail", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode jmxDetailReports(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "serviceName", required = false) String serviceNameFilter,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "trending", required = false, defaultValue = "false") Boolean trending,
			@RequestParam(value = "metricsId", required = false) String[] metricsId,
			@RequestParam(value = "divideBy", required = false) String[] divideBy,
			@RequestParam(value = "allVmTotal", required = false) String allVmTotal,
			@RequestParam(value = "category", required = false, defaultValue = "/csap/reports/jmx/daily") String category
	) {
		ObjectNode rootNode = jacksonMapper.createObjectNode();

		try {
			AggregateIterable<Document> results = null;
			if ( trending ) {
				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
				
				results = analyticsDbReader.getTrendingReportByCategory( appId, project, life, serviceNameFilter, category, metricsId, divideBy, allVmTotal, days, dateOffSet );
				timerAll.stop() ;
			
			} else {
				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_SUMMARY ).start();
				
				results = analyticsDbReader.findDetailReportDataUsingCategory( appId, project, life, serviceNameFilter, category, days, dateOffSet );
				timerAll.stop() ;
			
			}
			String data = JSON.serialize( results );
			JsonNode hostArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", hostArrayNode );
			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "jmxreport", serviceNameFilter );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		return rootNode;
	}

	@Cacheable(CsapAnalyticsApplication.SIMPLE_REPORT_CACHE)
	@RequestMapping(value = "/jmxCustom", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode applicationReportsJmxAndHttp(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "host", required = false) String host,
			@RequestParam(value = "serviceName") String serviceNameFilter,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "trending", required = false, defaultValue = "false") Boolean trending,
			@RequestParam(value = "metricsId", required = false) String[] metricsId,
			@RequestParam(value = "divideBy", required = false) String[] divideBy,
			@RequestParam(value = "allVmTotal", required = false) String allVmTotal,
			@RequestParam(value = "category", required = false, defaultValue = "/csap/reports/jmxCustom/daily") String category
	) {
		ObjectNode rootNode = jacksonMapper.createObjectNode();
		try {
			String data = "";
			if ( trending ) {

				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
				AggregateIterable<Document> results = analyticsDbReader.getTrendingReportByCategory( appId, project, life, serviceNameFilter, category, metricsId, divideBy, allVmTotal, days, dateOffSet );
				data = JSON.serialize( results );
				timerAll.stop();
			} else {

				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_SUMMARY ).start();
				AggregateIterable<Document> results = analyticsDbReader.findSummaryReportDataUsingCategory( appId, project, life, host, serviceNameFilter, category, days, dateOffSet );
				data = JSON.serialize( results );
				timerAll.stop();
			}
			JsonNode hostArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", hostArrayNode );
			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "jmxCustomReport", serviceNameFilter );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
			rootNode.put( "error", "Error getting jmx  custom data" );
		}
		return rootNode;
	}

	@Cacheable(CsapAnalyticsApplication.DETAILS_REPORT_CACHE)
	@RequestMapping(value = "/jmxCustom/detail", produces = MediaType.APPLICATION_JSON_VALUE)
	public ObjectNode applicationDetailReportsJmxAndHttp(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "serviceName") String serviceNameFilter,
			@RequestParam(value = "numDays", required = false, defaultValue = "1") Integer days,
			@RequestParam(value = "dateOffSet", required = false, defaultValue = "0") Integer dateOffSet,
			@RequestParam(value = "trending", required = false, defaultValue = "false") Boolean trending,
			@RequestParam(value = "metricsId", required = false) String[] metricsId,
			@RequestParam(value = "divideBy", required = false) String[] divideBy,
			@RequestParam(value = "allVmTotal", required = false) String allVmTotal,
			@RequestParam(value = "category", required = false, defaultValue = "/csap/reports/jmxCustom/daily") String category
	) {
		ObjectNode rootNode = jacksonMapper.createObjectNode();

		try {
			AggregateIterable<Document> results = null;
			if ( trending ) {
				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_TREND ).start();
				results = analyticsDbReader.getTrendingReportByCategory( appId, project, life, serviceNameFilter, category, metricsId, divideBy, allVmTotal, days, dateOffSet );
				timerAll.stop() ;
			} else {
				Split timerAll = SimonManager.getStopwatch( REPORT_DATA_ALL_SUMMARY ).start();
				results = analyticsDbReader.findDetailReportDataUsingCategory( appId, project, life, serviceNameFilter, category, days, dateOffSet );
				timerAll.stop() ;
			}
			String data = JSON.serialize( results );
			JsonNode hostArrayNode = jacksonMapper.readTree( data );
			rootNode.set( "data", hostArrayNode );
			if ( null != appId && null != project && null != life ) {
				long count = analyticsDbReader.numDaysAnalyticsAvailable( appId, project, life, "jmxCustomReport", serviceNameFilter );
				rootNode.put( "numDaysAvailable", count );
			}
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		return rootNode;
	}

	@CsapDoc(notes = "Get attributes for service name ",
			linkTests = {"serviceName=data"},
			linkGetParams = {"serviceName=data"},
			produces = {MediaType.APPLICATION_JSON_VALUE}
	)
	@RequestMapping(value = "/attributes", produces = MediaType.APPLICATION_JSON_VALUE)
	public String reportAttributes(@RequestParam(value = "appId", required = false) String appId,
			@RequestParam(value = "project", required = false) String project,
			@RequestParam(value = "life", required = false) String life,
			@RequestParam(value = "serviceName") String serviceNameFilter,
			@RequestParam(value = "category", required = false, defaultValue = "/csap/reports/jmxCustom/daily") String category
	) {
		String result = "";

		try {
			Set<String> results = analyticsHelper.findReportDocumentAttributes( appId, project, life, category, serviceNameFilter );
			result = jacksonMapper.writeValueAsString( results );
		} catch ( Exception e ) {
			logger.error( "Exception while converting data", e );
		}
		return result;
	}

	@RequestMapping(value = "/postAnalytics", produces = MediaType.TEXT_PLAIN_VALUE)
	public void postAnalytics(
			@RequestParam(value = "offSet", required = false, defaultValue = "1") Integer offSet,
			PrintWriter outputWriter
	) {
		
		outputWriter.println( "Starting Report for all projects - note this can take some time to complete");
		adoptionReportBuilder.buildAdoptionReportAndSaveToDB( offSet );
		outputWriter.println( "Posted analytics");
		//return "Posted analytics";
	}

	@RequestMapping(value = "/postProjectAnalytics", produces = MediaType.APPLICATION_JSON_VALUE)
	public String postAnalyticsForAProject(@RequestParam(value = "numDays", defaultValue = "1") Integer numDays,
			@RequestParam(value = "projectName") String projectName) {
		logger.info( "Project Name {} offset {}", projectName, numDays );
		String result = "";
		for ( int offSet = 1; offSet <= numDays; offSet++ ) {
			result = adoptionReportBuilder.buildAdoptionReportAndSaveToDB( offSet, projectName );
		}
		return result;
	}

}
