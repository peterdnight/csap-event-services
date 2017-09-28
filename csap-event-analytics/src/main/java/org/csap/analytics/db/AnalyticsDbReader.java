package org.csap.analytics.db;

import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static org.csap.analytics.misc.MetricsJsonConstants.APP_ID;
import static org.csap.analytics.misc.MetricsJsonConstants.CATEGORY;
import static org.csap.analytics.misc.MetricsJsonConstants.CREATED_ON;
import static org.csap.analytics.misc.MetricsJsonConstants.DATE;
import static org.csap.analytics.misc.MetricsJsonConstants.HOST;
import static org.csap.analytics.misc.MetricsJsonConstants.HOST_NAME;
import static org.csap.analytics.misc.MetricsJsonConstants.LIFE_CYCLE;
import static org.csap.analytics.misc.MetricsJsonConstants.METADATA;
import static org.csap.analytics.misc.MetricsJsonConstants.PROJECT;
import static org.csap.analytics.misc.MetricsJsonConstants.UIUSER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.Document;
import org.csap.analytics.CsapAnalyticsApplication;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.utils.SimonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;

@Service
public class AnalyticsDbReader {

	private Logger logger = LoggerFactory.getLogger( getClass() );

	@Inject
	private MongoClient mongoClient;

	@Inject
	private AnalyticsHelper analyticsHelper;

	@Inject
	private TrendingReportHelper trendingReportHelper;

	public AggregateIterable<Document> userActivityReport (	
	                                                       	String appId, 
	                                                       	String project, 
	                                                       	String life, 
	                                                       	int numDays ) {

		Document query = analyticsHelper.constructQuery( appId, project, life, numDays );
		//query.append( METADATA + "." + UIUSER, new BasicDBObject( "$exists", true ) );
		
		Pattern uiPattern = Pattern.compile( "^/csap/ui/"  );
		query.append(  CATEGORY, uiPattern )  ;
		
		Document match = new Document( "$match", query );

		Document groupFields = new Document( "_id", "$" + METADATA + "." + UIUSER );
		groupFields.put( "totActivity", new BasicDBObject( "$sum", 1 ) );
		Document group = new Document( "$group", groupFields );

		Document projectFields = new Document();

		projectFields.append( UIUSER, "$_id" );
		projectFields.append( "totActivity", "$totActivity" );
		projectFields.append( "_id", 0 );
		Document projectUiUser = new Document( "$project", projectFields );

		List<Document> operations = new ArrayList<>();
		operations.add( match );
		operations.add( group );
		operations.add( projectUiUser );

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection()
			.aggregate( operations );

		logger.info( "Query: {}", operations );

		return aggregationOutput;

	}

	public AggregateIterable<Document> userActivityTrendingReport (	String appId, String project, String life,
																	int numDays,
																	int dateOffSet ) {

		Split split = SimonManager.getStopwatch( "userReportTrending" ).start();
		List<Document> aggregationPipeline = new ArrayList<>();
		// category is passed as null so that it is not added to query
		Document query = constructDocumentQuery( appId, project, life, null );
		query = addDateToDocumentQuery( query, numDays, dateOffSet );
//		query.append( METADATA + "." + UIUSER, new Document( "$exists", true ) );
		
		Pattern uiPattern = Pattern.compile( "^/csap/ui/"  );
		query.append(  CATEGORY, uiPattern )  ;
		
		Document match = new Document( "$match", query );
		aggregationPipeline.add( match );

		Map<String, Object> groupFieldMap = new HashMap<>();
		groupFieldMap.put( "appId", "$appId" );
		groupFieldMap.put( "project", "$project" );
		groupFieldMap.put( "lifecycle", "$lifecycle" );
		groupFieldMap.put( "date", "$createdOn.date" );
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		groupFields.put( "totActivity", new Document( "$sum", 1 ) );
		Document group = new Document( "$group", groupFields );
		aggregationPipeline.add( group );

		Document sortOrder = new Document();
		sortOrder.append( "_id.date", 1 );
		Document sort = new Document( "$sort", sortOrder );
		aggregationPipeline.add( sort );

		Map<String, Object> groupByDateMap = new HashMap<>();
		groupByDateMap.put( "appId", "$_id.appId" );
		groupByDateMap.put( "project", "$_id.project" );
		groupByDateMap.put( "lifecycle", "$_id.lifecycle" );
		Document groupVmData = new Document( "_id", new Document( groupByDateMap ) );
		groupVmData.append( "date", new Document( "$push", "$_id.date" ) );
		groupVmData.append( "totActivity", new Document( "$push", "$totActivity" ) );
		Document groupData = new Document( "$group", groupVmData );
		aggregationPipeline.add( groupData );

		Document projectOutput = new Document();
		projectOutput.append( "appId", "$_id.appId" )
			.append( "lifecycle", "$_id.lifecycle" )
			.append( "project", "$_id.project" )
			.append( "date", 1 )
			.append( "totActivity", 1 )
			.append( "_id", 0 );
		Document projectOutputData = new Document( "$project", projectOutput );

		aggregationPipeline.add( projectOutputData );
		if ( StringUtils.isBlank( project ) ) {
			Document sortByProjectOrder = new Document();
			sortByProjectOrder.append( "project", 1 );
			Document sortByProject = new Document( "$sort", sortByProjectOrder );
			aggregationPipeline.add( sortByProject );
		}

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection()
			.aggregate( aggregationPipeline );
		split.stop();

		logger.info( "pipeline: {}",
			SimonUtils.presentNanoTime( split.runningFor() ), aggregationPipeline );

		return aggregationOutput;

	}

	// https://docs.mongodb.org/getting-started/java/client/
	public AggregateIterable<Document> getTrendingReportByCategory (
																		String appId, String project, String life,
																		String serviceNameFilter, String category, String[] metricsId,
																		String[] divideBy, String allVmTotal, int numDays,
																		int dateOffSet ) {
		Split split = SimonManager.getStopwatch( "trendingByCategory" ).start();

		logger.debug( "Trending report category {}", category );

		AggregateIterable<Document> trendingResults = analyticsHelper.getMongoEventCollection()
			.aggregate(
				trendingReportHelper.trendingOperationPipelineBuilder(
					appId, project, life, serviceNameFilter,
					category, metricsId, divideBy,
					allVmTotal, numDays, dateOffSet ) );
		split.stop();

		return trendingResults;

	}

	public AggregateIterable<Document> findSummaryReportDataUsingCategory (	String appId, String project, String life,
																			String hostName, String serviceNameFilter,
																			String category, int numDays, int dateOffSet ) {

		logger.debug( "{} Report for project: {}, number of Days: {} , offSet: {}",
			category, project, numDays, dateOffSet );

		List<Document> operations = new ArrayList<>();
		logger.debug( "category {} ", category );
		Document query = constructDocumentQuery( appId, project, life, category );
		if ( StringUtils.isNotBlank( hostName ) ) {
			query.append( HOST, hostName );
		}

		query = addDateToDocumentQuery( query, numDays, dateOffSet );

		Document match = new Document( "$match", query );
		operations.add( match );

		Document unwind = new Document( "$unwind", "$data.summary" );
		operations.add( unwind );

		String attributesService = "CsAgent";// use csagent attributes
		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {
			attributesService = serviceNameFilter;
			Document serviceNameQuery = new Document();
			serviceNameQuery.append( "data.summary.serviceName", serviceNameFilter );
			Document serviceNameMatch = new Document( "$match", serviceNameQuery );
			operations.add( serviceNameMatch );
		}

		Map<String, Object> groupFieldMap = new HashMap<>();
		groupFieldMap.put( "serviceName", "$data.summary.serviceName" );
		groupFieldMap.put( APP_ID, "$" + APP_ID );
		groupFieldMap.put( PROJECT, "$" + PROJECT );
		groupFieldMap.put( LIFE_CYCLE, "$" + LIFE_CYCLE );

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		Document projectFields = new Document();
		projectFields
			.append( APP_ID, "$_id." + APP_ID )
			.append( LIFE_CYCLE, "$_id." + LIFE_CYCLE )
			.append( PROJECT, "$_id." + PROJECT )
			.append( "serviceName", "$_id.serviceName" )
			.append( "_id", 0 );
		Set<String> keys = analyticsHelper.findReportDocumentAttributes( appId, project, life, category, attributesService );
		if ( null == keys )
			keys = new HashSet<>();
		/*
		 * keys.stream().filter(key ->
		 * !"serviceName".equalsIgnoreCase(key)).forEach(key -> {
		 * 
		 * });
		 */
		for ( String key : keys ) {
			if ( "serviceName".equalsIgnoreCase( key ) )
				continue;
			if ( key.endsWith( "Avg" ) ) {
				groupFields.append( key, new Document( "$avg", "$data.summary." + key ) );
			} else {
				groupFields.append( key, new Document( "$sum", "$data.summary." + key ) );
			}
			projectFields.append( key, 1 );
		}
		Document group = new Document( "$group", groupFields );
		Document projectHost = new Document( "$project", projectFields );

		operations.add( group );
		operations.add( projectHost );

		logger.debug( "Aggregation: {}", operations );

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection()
			.aggregate( operations );
		return aggregationOutput;

	}

	public AggregateIterable<Document> findDetailReportDataUsingCategory (	String appId, String project, String life,
																			String serviceNameFilter, String category, int numDays,
																			int dateOffSet ) {

		List<Document> operations = new ArrayList<>();

		Document query = constructDocumentQuery( appId, project, life, category );
		query = addDateToDocumentQuery( query, numDays, dateOffSet );
		logger.debug( "query::{}", query );
		Document match = new Document( "$match", query );
		operations.add( match );

		Document unwind = new Document( "$unwind", "$data.summary" );
		operations.add( unwind );
		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {
			Document serviceNameQuery = new Document();
			serviceNameQuery.append( "data.summary.serviceName", serviceNameFilter );
			Document serviceNameMatch = new Document( "$match", serviceNameQuery );
			operations.add( serviceNameMatch );
		}

		Map<String, Object> groupFieldMap = new HashMap<>();
		groupFieldMap.put( "serviceName", "$data.summary.serviceName" );
		groupFieldMap.put( APP_ID, "$" + APP_ID );
		groupFieldMap.put( PROJECT, "$" + PROJECT );
		groupFieldMap.put( LIFE_CYCLE, "$" + LIFE_CYCLE );
		groupFieldMap.put( HOST, "$" + HOST );

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		Document projectFields = new Document();
		projectFields
			.append( APP_ID, "$_id." + APP_ID )
			.append( LIFE_CYCLE, "$_id." + LIFE_CYCLE )
			.append( PROJECT, "$_id." + PROJECT )
			.append( "serviceName", "$_id.serviceName" )
			.append( HOST, "$_id." + HOST )
			.append( "_id", 0 );
		Set<String> keys = analyticsHelper.findReportDocumentAttributes( appId, project, life, category, serviceNameFilter );
		if ( null == keys )
			keys = new HashSet<>();

		for ( String key : keys ) {
			if ( "serviceName".equalsIgnoreCase( key ) )
				continue;
			if ( key.endsWith( "Avg" ) ) {
				groupFields.append( key, new BasicDBObject( "$avg", "$data.summary." + key ) );
			} else {
				groupFields.append( key, new BasicDBObject( "$sum", "$data.summary." + key ) );
			}
			projectFields.append( key, 1 );
		}
		Document group = new Document( "$group", groupFields );
		Document projectHost = new Document( "$project", projectFields );

		operations.add( group );
		operations.add( projectHost );

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection()
			.aggregate( operations );
		return aggregationOutput;
	}

	public AggregateIterable<Document> getCoreUsedTrendingReport (	String appId, String project, String life,
																	String[] metricsId, boolean perVm, int top,
																	int low, int numDays, int dateOffSet ) {

		Split split = SimonManager.getStopwatch( "vmCoreTrendingReport" ).start();
		String category = "/csap/reports/host/daily";
		Set matchHosts = new HashSet();
		if ( top > 0 ) {
			List topHosts = trendingReportHelper.topHosts( appId, project, life, metricsId,
				new String[] { "numberOfSamples" }, top, -1,
				category, "", false, numDays, dateOffSet );
			// logger.debug("top Hosts {} ",topHosts);
			// matchHosts.addAll(topHosts(appId, project, life,top, -1, numDays,
			// dateOffSet));
			matchHosts.addAll( topHosts );
		}
		if ( low > 0 ) {
			// matchHosts.addAll(topHosts(appId, project, life,low, 1, numDays,
			// dateOffSet));
			List lowHosts = trendingReportHelper.topHosts( appId, project, life, metricsId,
				new String[] { "numberOfSamples" }, low, 1,
				category, "", false, numDays, dateOffSet );
			matchHosts.addAll( lowHosts );
		}
		List<Document> operations = new ArrayList<>();

		Document query = constructDocumentQuery( appId, project, life, category );
		query = addDateToDocumentQuery( query, numDays, dateOffSet );
		if ( matchHosts.size() > 0 ) {
			Document hostMatch = new Document( "$in", matchHosts );
			query.append( "host", hostMatch );
		}
		Document match = new Document( "$match", query );
		operations.add( match );

		Map<String, Object> groupFieldByHostMap = new HashMap<>();
		groupFieldByHostMap.put( "appId", "$appId" );
		groupFieldByHostMap.put( "project", "$project" );
		groupFieldByHostMap.put( "lifecycle", "$lifecycle" );
		groupFieldByHostMap.put( "host", "$host" );
		groupFieldByHostMap.put( "date", "$createdOn.date" );
		Document groupFieldsByHost = new Document( "_id", new Document( groupFieldByHostMap ) );
		for ( String key : metricsId ) {
			groupFieldsByHost.append( key, new Document( "$sum", "$data.summary." + key ) );
		}
		groupFieldsByHost.append( "cpuCountAvg", new BasicDBObject( "$sum", "$data.summary.cpuCountAvg" ) );
		groupFieldsByHost.append( "numberOfSamples", new BasicDBObject( "$sum", "$data.summary.numberOfSamples" ) );
		Document groupByHost = new Document( "$group", groupFieldsByHost );
		operations.add( groupByHost );

		Document projectFields = new Document();
		List<Object> metricsList = new ArrayList<>();
		for ( String key : metricsId ) {
			metricsList.add( "$" + key );
			projectFields.append( key, 1 );
		}
		projectFields.append( "cpuCountAvg", 1 ).append( "numberOfSamples", 1 );
		projectFields.append( "total", new Document( "$add", metricsList ) );
		projectFields.append( "appId", "$_id.appId" )
			.append( "lifecycle", "$_id.lifecycle" )
			.append( "project", "$_id.project" )
			.append( "date", "$_id.date" )
			.append( "host", "$_id.host" )
			.append( "_id", 0 );
		Document projectData = new Document( "$project", projectFields );
		operations.add( projectData );

		Document numSampleProjection = new Document();
		List<Object> numSampleDivideList = new ArrayList<>();
		numSampleDivideList.add( "$total" );
		numSampleDivideList.add( "$numberOfSamples" );
		numSampleProjection.append( "appId", 1 )
			.append( "lifecycle", 1 )
			.append( "project", 1 )
			.append( "cpuCountAvg", 1 )
			.append( "numberOfSamples", 1 )
			.append( "date", 1 )
			.append( "host", 1 )
			.append( "totalSysCpu", 1 )
			.append( "totalUsrCpu", 1 );

		numSampleProjection.append( "total", new Document( "$divide", numSampleDivideList ) );
		Document projectNumSampleByHost = new Document( "$project", numSampleProjection );
		operations.add( projectNumSampleByHost );

		Document projectCoresUsed = new Document();
		List<Object> coresOpList = new ArrayList<>();
		coresOpList.add( "$total" );
		coresOpList.add( "$cpuCountAvg" );
		projectCoresUsed.append( "appId", 1 )
			.append( "lifecycle", 1 )
			.append( "project", 1 )
			.append( "cpuCountAvg", 1 )
			.append( "date", 1 )
			.append( "host", 1 )
			.append( "totalSysCpu", 1 )
			.append( "totalUsrCpu", 1 );
		projectCoresUsed.append( "coresUsed", new Document( "$multiply", coresOpList ) );
		Document projectCoresUsedData = new Document( "$project", projectCoresUsed );
		operations.add( projectCoresUsedData );

		Document projectCoresUsedPercent = new Document();
		List<Object> coresDivideList = new ArrayList<>();
		coresDivideList.add( "$coresUsed" );
		coresDivideList.add( 100 );
		projectCoresUsedPercent.append( "appId", 1 )
			.append( "lifecycle", 1 )
			.append( "project", 1 )
			.append( "cpuCountAvg", 1 )
			.append( "date", 1 )
			.append( "host", 1 )
			.append( "totalSysCpu", 1 )
			.append( "totalUsrCpu", 1 );
		projectCoresUsedPercent.append( "coresUsedPercent", new Document( "$divide", coresDivideList ) );
		Document projectCoresUsedPercentHost = new Document( "$project", projectCoresUsedPercent );
		operations.add( projectCoresUsedPercentHost );

		if ( !perVm ) {
			Map<String, Object> groupByLifeMap = new HashMap<>();
			groupByLifeMap.put( "appId", "$appId" );
			groupByLifeMap.put( "project", "$project" );
			groupByLifeMap.put( "lifecycle", "$lifecycle" );
			groupByLifeMap.put( "date", "$date" );
			Document groupLifeData = new Document( "_id", new Document( groupByLifeMap ) );
			groupLifeData.append( "coresUsedLife", new Document( "$sum", "$coresUsedPercent" ) );
			groupLifeData.append( "totalSysCpu", new Document( "$sum", "$totalSysCpu" ) );
			groupLifeData.append( "totalUsrCpu", new Document( "$sum", "$totalUsrCpu" ) );
			groupLifeData.append( "cpuCountAvg", new Document( "$sum", "$cpuCountAvg" ) );
			Document groupByLife = new Document( "$group", groupLifeData );
			operations.add( groupByLife );
		}

		Document sortOrder = new Document();
		if ( perVm ) {
			sortOrder.append( "date", 1 );
		} else {
			sortOrder.append( "_id.date", 1 );
		}
		Document sort = new Document( "$sort", sortOrder );
		operations.add( sort );
		if ( perVm ) {
			Map<String, Object> groupByDateMap = new HashMap<>();
			groupByDateMap.put( "appId", "$appId" );
			groupByDateMap.put( "project", "$project" );
			groupByDateMap.put( "host", "$host" );
			groupByDateMap.put( "lifecycle", "$lifecycle" );

			Document groupVmData = new Document( "_id", new Document( groupByDateMap ) );
			groupVmData.append( "date", new Document( "$push", "$date" ) );
			for ( String key : metricsId ) {
				groupVmData.append( key, new Document( "$push", "$" + key ) );
			}
			groupVmData.append( "cpuCountAvg", new Document( "$push", "$cpuCountAvg" ) );
			groupVmData.append( "coresUsed", new Document( "$push", "$coresUsedPercent" ) );
			Document groupData = new Document( "$group", groupVmData );
			operations.add( groupData );

		} else {
			Map<String, Object> groupByDateMap = new HashMap<>();
			groupByDateMap.put( "appId", "$_id.appId" );
			groupByDateMap.put( "project", "$_id.project" );
			groupByDateMap.put( "lifecycle", "$_id.lifecycle" );

			Document groupVmData = new Document( "_id", new Document( groupByDateMap ) );
			groupVmData.append( "date", new Document( "$push", "$_id.date" ) );
			for ( String key : metricsId ) {
				groupVmData.append( key, new Document( "$push", "$" + key ) );
			}
			groupVmData.append( "cpuCountAvg", new Document( "$push", "$cpuCountAvg" ) );
			groupVmData.append( "coresUsed", new Document( "$push", "$coresUsedLife" ) );
			Document groupData = new Document( "$group", groupVmData );
			operations.add( groupData );
		}
		Document projectOutput = new Document();
		if ( logger.isDebugEnabled() ) {
			for ( String key : metricsId ) {
				projectOutput.append( key, 1 );
			}
			projectOutput.append( "cpuCountAvg", 1 );
		}
		projectOutput.append( "coresUsed", 1 );
		projectOutput.append( "appId", "$_id.appId" )
			.append( "lifecycle", "$_id.lifecycle" )
			.append( "project", "$_id.project" )
			.append( "date", 1 )
			.append( "_id", 0 );
		if ( perVm ) {
			projectOutput.append( "host", "$_id.host" );
		}
		Document projectOutputData = new Document( "$project", projectOutput );
		operations.add( projectOutputData );

		if ( StringUtils.isBlank( project ) ) {
			Document sortByProjectOrder = new Document();
			sortByProjectOrder.append( "project", 1 );
			Document sortByProject = new Document( "$sort", sortByProjectOrder );
			operations.add( sortByProject );
		}

		logger.debug( "Query {}", operations );
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection()
			.aggregate( operations );

		split.stop();
		return aggregationOutput;
	}

	public AggregateIterable<Document> getVmHealthReport (	String appId, String project, String life,
															boolean perVm, int top, int low,
															int numDays, int dateOffSet, String category ) {
		Split split = SimonManager.getStopwatch( "vmHealthReport" ).start();
		List<Document> aggregationPipeline = new ArrayList<>();
		Set matchHosts = new HashSet();
		if ( top > 0 ) {
			List topHosts = trendingReportHelper.topHosts( appId, project, life, new String[] { "UnHealthyEventCount" },
				null, top, -1,
				category, "", false, numDays, dateOffSet );
			logger.debug( "top hosts {} ", topHosts );
			matchHosts.addAll( topHosts );
		}
		if ( low > 0 ) {
			List lowHosts = trendingReportHelper.topHosts( appId, project, life, new String[] { "UnHealthyEventCount" },
				null, low, 1,
				category, "", false, numDays, dateOffSet );
			matchHosts.addAll( lowHosts );
		}
		Document query = constructDocumentQuery( appId, project, life, category );
		query = addDateToDocumentQuery( query, numDays, dateOffSet );
		if ( matchHosts.size() > 0 ) {
			Document hostMatch = new Document( "$in", matchHosts );
			query.append( "host", hostMatch );
		}
		Document match = new Document( "$match", query );
		aggregationPipeline.add( match );

		Map<String, Object> groupFieldMap = new HashMap<>();
		groupFieldMap.put( "appId", "$appId" );
		groupFieldMap.put( "project", "$project" );
		groupFieldMap.put( "lifecycle", "$lifecycle" );
		groupFieldMap.put( "date", "$createdOn.date" );
		if ( perVm ) {
			groupFieldMap.put( "host", "$host" );
		}
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		groupFields.append( "UnHealthyCount", new Document( "$sum", "$data.UnHealthyEventCount" ) );
		Document group = new Document( "$group", groupFields );
		aggregationPipeline.add( group );

		Document sortOrder = new Document();
		sortOrder.append( "_id.date", 1 );
		Document sort = new Document( "$sort", sortOrder );
		aggregationPipeline.add( sort );

		Map<String, Object> groupByDateMap = new HashMap<>();
		groupByDateMap.put( "appId", "$_id.appId" );
		groupByDateMap.put( "project", "$_id.project" );
		groupByDateMap.put( "lifecycle", "$_id.lifecycle" );
		if ( perVm ) {
			groupByDateMap.put( "host", "$_id.host" );
		}
		Document groupVmData = new Document( "_id", new Document( groupByDateMap ) );
		groupVmData.append( "date", new Document( "$push", "$_id.date" ) );
		groupVmData.append( "UnHealthyCount", new Document( "$push", "$UnHealthyCount" ) );

		Document groupData = new Document( "$group", groupVmData );
		aggregationPipeline.add( groupData );

		Document projectOutput = new Document();
		projectOutput.append( "appId", "$_id.appId" )
			.append( "lifecycle", "$_id.lifecycle" )
			.append( "project", "$_id.project" )
			.append( "date", 1 )
			.append( "UnHealthyCount", 1 )
			.append( "_id", 0 );
		if ( perVm ) {
			projectOutput.append( "host", "$_id.host" );
		}
		Document projectOutputData = new Document( "$project", projectOutput );
		aggregationPipeline.add( projectOutputData );

		if ( StringUtils.isBlank( project ) ) {
			Document sortByProjectOrder = new Document();
			sortByProjectOrder.append( "project", 1 );
			Document sortByProject = new Document( "$sort", sortByProjectOrder );
			aggregationPipeline.add( sortByProject );
		}
		logger.debug( "aggregation pipe line {}", aggregationPipeline );
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection()
			.aggregate( aggregationPipeline );

		split.stop();
		return aggregationOutput;

	}

	public AggregateIterable<Document> getVmTrendingReport (	String appId, String project, String life,
																String[] metricsId, String[] divideBy, String allVmTotal,
																boolean perVm, int top, int low,
																int numDays, int dateOffSet ) {

		logger.debug( "Metrics: {}, numDays: {} , Date: {}", Arrays.asList( metricsId ), numDays, dateOffSet );
		Split split = SimonManager.getStopwatch( "vmTrending" ).start();
		String category = "/csap/reports/host/daily";
		List<Document> trendingCommandPipleline = new ArrayList<>();

		Set matchHosts = new HashSet();
		if ( top > 0 ) {
			List topHosts = trendingReportHelper.topHosts( appId, project, life, metricsId,
				new String[] { "numberOfSamples" }, top, -1,
				category, "", false, numDays, dateOffSet );
			matchHosts.addAll( topHosts );
		}
		if ( low > 0 ) {
			List lowHosts = trendingReportHelper.topHosts( appId, project, life, metricsId,
				new String[] { "numberOfSamples" }, low, 1,
				category, "", false, numDays, dateOffSet );
			matchHosts.addAll( lowHosts );
		}
		Document query = constructDocumentQuery( appId, project, life, category );
		query = addDateToDocumentQuery( query, numDays, dateOffSet );
		if ( matchHosts.size() > 0 ) {
			Document hostMatch = new Document( "$in", matchHosts );
			query.append( "host", hostMatch );
		}
		Document match = new Document( "$match", query );
		trendingCommandPipleline.add( match );
		if ( "true".equalsIgnoreCase( allVmTotal ) || perVm ) {
			Document groupByHost = trendingReportHelper.groupByHostNameDocument( metricsId, divideBy );
			trendingCommandPipleline.add( groupByHost );
			trendingReportHelper.addDivideByProjectionsToPipeline( metricsId, divideBy, trendingCommandPipleline );
			if ( !perVm ) {
				Document groupByLife = trendingReportHelper.groupByLife( metricsId );
				trendingCommandPipleline.add( groupByLife );
			}
		} else {
			Map<String, Object> groupFieldMap = new HashMap<>();
			groupFieldMap.put( "appId", "$appId" );
			groupFieldMap.put( "project", "$project" );
			groupFieldMap.put( "lifecycle", "$lifecycle" );
			groupFieldMap.put( "date", "$createdOn.date" );
			Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
			for ( String key : metricsId ) {
				groupFields.append( key, new Document( "$sum", "$data.summary." + key ) );
			}
			if ( ArrayUtils.contains( divideBy, "numberOfSamples" ) ) {
				groupFields.append( "numberOfSamples", new BasicDBObject( "$sum", "$data.summary.numberOfSamples" ) );
			}
			Document group = new Document( "$group", groupFields );
			trendingCommandPipleline.add( group );
		}

		Document projectFields = new Document();
		List<Object> metricsList = new ArrayList<>();
		for ( String key : metricsId ) {
			metricsList.add( "$" + key );
			projectFields.append( key, 1 );
		}
		if ( metricsId.length > 1 ) {
			projectFields.append( "total", new BasicDBObject( "$add", metricsList ) );
		}
		projectFields.append( "appId", "$_id.appId" )
			.append( "lifecycle", "$_id.lifecycle" )
			.append( "project", "$_id.project" )
			.append( "date", "$_id.date" )
			.append( "_id", 0 );
		if ( perVm ) {
			projectFields.append( "host", "$_id.host" );
		}
		if ( ArrayUtils.contains( divideBy, "numberOfSamples" ) ) {
			projectFields.append( "numberOfSamples", 1 );
		}
		Document projectData = new Document( "$project", projectFields );
		trendingCommandPipleline.add( projectData );

		if ( null != divideBy && !"true".equalsIgnoreCase( allVmTotal ) && !perVm ) {
			String dividend = metricsId[0];
			if ( metricsId.length > 1 ) {
				dividend = "total";
			}
			logger.debug( "dividend {} ", dividend );
			for ( String divisorStr : divideBy ) {
				Document numSampleProjection = new Document();
				List<Object> numSampleDivideList = new ArrayList<>();
				numSampleDivideList.add( "$" + dividend );
				if ( divisorStr.equalsIgnoreCase( "numberOfSamples" ) ) {
					numSampleDivideList.add( "$numberOfSamples" );
				} else {
					if ( NumberUtils.isNumber( divisorStr ) ) {
						numSampleDivideList.add( Double.parseDouble( divisorStr ) );
					}
				}
				numSampleProjection.append( "appId", 1 )
					.append( "lifecycle", 1 )
					.append( "project", 1 )
					.append( "numberOfSamples", 1 )
					.append( "date", 1 );
				numSampleProjection.append( dividend, new BasicDBObject( "$divide", numSampleDivideList ) );
				Document projectDivision = new Document( "$project", numSampleProjection );
				if ( numSampleDivideList.size() == 2 ) {
					trendingCommandPipleline.add( projectDivision );
				}
			}
		}

		Document sortOrder = new Document();
		sortOrder.append( "date", 1 );
		Document sort = new Document( "$sort", sortOrder );
		trendingCommandPipleline.add( sort );

		Map<String, Object> groupByDateMap = new HashMap<>();
		groupByDateMap.put( "appId", "$appId" );
		groupByDateMap.put( "project", "$project" );
		groupByDateMap.put( "lifecycle", "$lifecycle" );
		if ( perVm ) {
			groupByDateMap.put( "host", "$host" );
		}

		Document groupVmData = new Document( "_id", new Document( groupByDateMap ) );
		groupVmData.append( "date", new Document( "$push", "$date" ) );
		for ( String key : metricsId ) {
			groupVmData.append( key, new Document( "$push", "$" + key ) );
		}
		if ( metricsId.length > 1 ) {
			groupVmData.append( "total", new Document( "$push", "$total" ) );
		}
		Document groupData = new Document( "$group", groupVmData );
		trendingCommandPipleline.add( groupData );

		Document projectOutput = new Document();

		if ( metricsId.length > 1 ) {
			projectOutput.append( "total", 1 );
			if ( logger.isDebugEnabled() ) {
				for ( String key : metricsId ) {
					projectOutput.append( key, 1 );
				}
			}
		} else {
			for ( String key : metricsId ) {
				projectOutput.append( key, 1 );
			}
		}
		projectOutput.append( "appId", "$_id.appId" )
			.append( "lifecycle", "$_id.lifecycle" )
			.append( "project", "$_id.project" )
			.append( "date", 1 )
			.append( "_id", 0 );
		if ( perVm ) {
			projectOutput.append( "host", "$_id.host" );
		}
		Document projectOutputData = new Document( "$project", projectOutput );
		trendingCommandPipleline.add( projectOutputData );
		if ( StringUtils.isBlank( project ) ) {
			Document sortByProjectOrder = new Document();
			sortByProjectOrder.append( "project", 1 );
			Document sortByProject = new Document( "$sort", sortByProjectOrder );
			trendingCommandPipleline.add( sortByProject );
		}

		logger.debug( "trendingCommandPipleline: {} ", trendingCommandPipleline );

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection()
			.aggregate( trendingCommandPipleline );

		split.stop();
		return aggregationOutput;
	}

	public AggregateIterable<Document> getVmReport (	String appId, String project, String life, int numDays,
														int dateOffSet ) {
		String category = "/csap/reports/host/daily";
		Document query = constructDocumentQuery( appId, project, life, category );
		query = addDateToDocumentQuery( query, numDays, dateOffSet );
		Document match = new Document( "$match", query );

		Map<String, Object> groupFieldMap = new HashMap<>();
		groupFieldMap.put( APP_ID, "$" + APP_ID );
		groupFieldMap.put( PROJECT, "$" + PROJECT );
		groupFieldMap.put( LIFE_CYCLE, "$" + LIFE_CYCLE );
		groupFieldMap.put( HOST, "$" + HOST );

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );

		Document projectFields = new Document( HOST_NAME, "$_id." + HOST );
		projectFields.append( APP_ID, "$_id." + APP_ID ).append( LIFE_CYCLE, "$_id." + LIFE_CYCLE )
			.append( PROJECT, "$_id." + PROJECT ).append( "_id", 0 );
		Set<String> keys = analyticsHelper.findReportDocumentAttributes( appId, project, life, category, null );
		if ( null == keys )
			keys = new HashSet<>();
		for ( String key : keys ) {
			if ( key.endsWith( "Avg" ) ) {
				groupFields.append( key, new Document( "$avg", "$data.summary." + key ) );
			} else {
				groupFields.append( key, new Document( "$sum", "$data.summary." + key ) );
			}
			projectFields.append( key, 1 );
		}
		Document group = new Document( "$group", groupFields );
		Document projectHost = new Document( "$project", projectFields );

		List<Document> operations = new ArrayList<>();
		operations.add( match );
		operations.add( group );
		operations.add( projectHost );
		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection()
			.aggregate( operations );
		return aggregationOutput;

	}

	public Document constructDocumentQuery ( String appId, String project, String life, String category ) {
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
		return query;
	}

	private Document addDateToDocumentQuery ( Document query, int numDays, int dateOffSet ) {
		return trendingReportHelper.addDateToDocumentQuery( query, numDays, dateOffSet );
	}
	
	final static public int MONGO_ASCENDING_SORT=1;

	@Cacheable(value = CsapAnalyticsApplication.NUM_DAYS_CACHE)
	public long numDaysAnalyticsAvailable (	String appId, String project, String life, String reportType,
											String serviceName ) {

		Split timer = SimonManager.getStopwatch( "reportsNumDays" ).start();

		long numDaysAvailable = 0;
		Document query = getUserOrVmQuery( appId, project, life, reportType, serviceName );
		Document sortOrder = new Document( "createdOn.date", MONGO_ASCENDING_SORT );

		Document earliestRecord = analyticsHelper.getMongoEventCollection()
			.find( query )
			.sort( sortOrder )
			.limit( 1 )
			.projection( fields( include( "createdOn" ), excludeId() ) )
			.first();
		
		if ( null != earliestRecord ) {
			Document createdOnDbObject = (Document) earliestRecord.get( CREATED_ON );
			Date earliestDate = (Date) createdOnDbObject.get( "mongoDate" );
			logger.debug( "Earliest Date ::--> {}", earliestDate );
			long diff = Calendar.getInstance().getTimeInMillis() - earliestDate.getTime();
			numDaysAvailable = TimeUnit.DAYS.convert( diff, TimeUnit.MILLISECONDS );
		}

		timer.stop();

		logger.info( "Days Of Data: {}. Time Taken: {} \n Report: {}",
			numDaysAvailable, 
			SimonUtils.presentNanoTime( timer.runningFor() ),
			query );

		return numDaysAvailable;
	}


	private Document getUserOrVmQuery (
										String appId, String project, String life,
										String reportType, String serviceName ) {

		Document query = new Document();
		query.append( APP_ID, appId )
			.append( PROJECT, project )
			.append( LIFE_CYCLE, life );
		
		if ( "userreport".equalsIgnoreCase( reportType ) ) {
//			query.append( METADATA + "." + UIUSER, new BasicDBObject( "$exists", true ) );
			Pattern uiPattern = Pattern.compile( "^/csap/ui/"  );
			query.append(  CATEGORY, uiPattern )  ;
		}
		if ( "hostreport".equalsIgnoreCase( reportType ) ) {
			query.append( CATEGORY, "/csap/reports/host/daily" );
		}
		if ( "servicereport".equalsIgnoreCase( reportType ) ) {
			query.append( CATEGORY, "/csap/reports/process/daily" );
		}
		if ( "jmxreport".equalsIgnoreCase( reportType ) ) {
			query.append( CATEGORY, "/csap/reports/jmx/daily" );
		}
		if ( "jmxCustomReport".equalsIgnoreCase( reportType ) ) {
			query.append( CATEGORY, "/csap/reports/jmxCustom/daily" );
		}
		if ( "logRotateReport".equalsIgnoreCase( reportType ) ) {
			query.append( CATEGORY, "/csap/reports/logRotate" );
		}
		if ( StringUtils.isNotBlank( serviceName ) ) {
			Document serviceNameObj = new Document( "serviceName", serviceName );
			query.append( "data.summary", new Document( "$elemMatch", serviceNameObj ) );
		}

		return query;
	}

	private Document getGroupByAppIdProjectLifeAndDate () {
		Map<String, Object> groupFieldMap = new HashMap<>();
		groupFieldMap.put( APP_ID, "$" + APP_ID );
		groupFieldMap.put( PROJECT, "$" + PROJECT );
		groupFieldMap.put( LIFE_CYCLE, "$" + LIFE_CYCLE );
		groupFieldMap.put( DATE, "$" + CREATED_ON + "." + DATE );
		Document groupFields = new Document( "_id", new BasicDBObject( groupFieldMap ) );
		Document group = new Document( "$group", groupFields );
		return group;
	}

	private Document getGroupByAppIdProjectLife () {
		Map<String, Object> groupFieldsMap = new HashMap<>();
		groupFieldsMap.put( APP_ID, "$_id." + APP_ID );
		groupFieldsMap.put( PROJECT, "$_id." + PROJECT );
		groupFieldsMap.put( LIFE_CYCLE, "$_id." + LIFE_CYCLE );
		Document projectGroupFields = new Document( "_id", new BasicDBObject( groupFieldsMap ) );
		projectGroupFields.put( "totNumDays", new Document( "$sum", 1 ) );
		Document group = new Document( "$group", projectGroupFields );
		return group;
	}

}
