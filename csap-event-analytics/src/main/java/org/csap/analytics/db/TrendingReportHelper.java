package org.csap.analytics.db;

import static org.csap.analytics.misc.MetricsJsonConstants.APP_ID;
import static org.csap.analytics.misc.MetricsJsonConstants.CATEGORY;
import static org.csap.analytics.misc.MetricsJsonConstants.CREATED_ON;
import static org.csap.analytics.misc.MetricsJsonConstants.DATE;
import static org.csap.analytics.misc.MetricsJsonConstants.LIFE_CYCLE;
import static org.csap.analytics.misc.MetricsJsonConstants.PROJECT;
import static org.csap.analytics.misc.MongoConstants.EVENT_COLLECTION_NAME;
import static org.csap.analytics.misc.MongoConstants.EVENT_DB_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.Document;
import org.csap.analytics.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;

public class TrendingReportHelper {

	public static final String MULTIPLE_SERVICE_DELIMETER = ",";
	private Logger logger = LoggerFactory.getLogger( getClass() );

	@Inject
	private MongoClient mongoClient;

	@Inject
	private AnalyticsHelper analyticsHelper;

	public List topHosts(String appId, String project, String life,
			String[] metricsId, String[] divideBy,
			int numHosts, int sortOrd,
			String category, String serviceNameFilter,
			boolean unwindSummary,
			int numDays, int dateOffSet) {
		if ( logger.isDebugEnabled() ) {
			for (String key : metricsId) {
				logger.debug( "Metrics ID {} ", key );
			}
			logger.debug( "Service name filter {} ", serviceNameFilter );
			logger.debug( "Unwind summary {} ", unwindSummary );
		}
		String metricsPath = getReportPath( category );
		logger.debug( "metrics path {} ", metricsPath );
		List<Document> aggregationPipeline = new ArrayList<>();
		Document query = constructDocumentQuery( appId, project, life, category );
		query = addDateToDocumentQuery( query, numDays, dateOffSet );
		Document match = new Document( "$match", query );
		aggregationPipeline.add( match );

		if ( unwindSummary ) {
			Document unwind = new Document( "$unwind", "$" + metricsPath );
			aggregationPipeline.add( unwind );
			if ( StringUtils.isNotBlank( serviceNameFilter ) ) {
				Document filterService = new Document( metricsPath + ".serviceName", serviceNameFilter );
				Document serviceMatch = new Document( "$match", filterService );
				aggregationPipeline.add( serviceMatch );
			}
		}

		Map<String, Object> groupFieldMap = new HashMap<>();
		groupFieldMap.put( "appId", "$appId" );
		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {
			groupFieldMap.put( "serviceName", "$" + metricsPath + ".serviceName" );
		}
		if ( StringUtils.isNotBlank( project ) ) {
			groupFieldMap.put( "project", "$project" );
		}
		if ( StringUtils.isNotBlank( life ) ) {
			groupFieldMap.put( "lifecycle", "$lifecycle" );
		}
		groupFieldMap.put( "host", "$host" );

		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		for (String metricsKey : metricsId) {
			groupFields.append( metricsKey, new Document( "$sum", "$" + metricsPath + "." + metricsKey ) );
		}
		groupFields.append( "numberOfSamples", new Document( "$sum", "$" + metricsPath + ".numberOfSamples" ) );
		Document group = new Document( "$group", groupFields );
		aggregationPipeline.add( group );

		Document projectFields = new Document();
		List<Object> metricsList = new ArrayList<>();
		for (String metricsKey : metricsId) {
			metricsList.add( "$" + metricsKey );
		}
		projectFields.append( "total", new Document( "$add", metricsList ) );
		projectFields
				.append( "appId", "$_id.appId" )
				.append( "host", "$_id.host" )
				.append( "numberOfSamples", 1 )
				.append( "_id", 0 );
		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {
			projectFields.append( "serviceName", "$_id.serviceName" );
		}
		if ( StringUtils.isNotBlank( project ) ) {
			projectFields.append( "project", "$_id.project" );
		}
		if ( StringUtils.isNotBlank( life ) ) {
			projectFields.append( "lifecycle", "$_id.lifecycle" );
		}
		Document projectData = new Document( "$project", projectFields );
		aggregationPipeline.add( projectData );

		addDivideByProjectionsToPipeline( new String[] { "total" }, divideBy, aggregationPipeline );

		Document sortOrder = new Document();
		// -1 is highest first
		// 1 lowest first
		sortOrder.append( "total", sortOrd );
		Document sort = new Document( "$sort", sortOrder );
		aggregationPipeline.add( sort );

		Map<String, Object> groupByDateMap = new HashMap<>();
		groupByDateMap.put( "appId", "$appId" );
		if ( StringUtils.isNotBlank( project ) ) {
			groupByDateMap.put( "project", "$project" );
		}
		if ( StringUtils.isNotBlank( life ) ) {
			groupByDateMap.put( "lifecycle", "$lifecycle" );
		}
		Document groupVmData = new Document( "_id", new Document( groupByDateMap ) );
		groupVmData.append( "host", new Document( "$push", "$host" ) );
		groupVmData.append( "total", new Document( "$push", "$total" ) );
		Document groupData = new Document( "$group", groupVmData );
		aggregationPipeline.add( groupData );

		// DBCollection eventCollection = getEventCollection();
		// AggregationOutput output =
		// eventCollection.aggregate(aggregationPipeline,ReadPreference.secondaryPreferred());

		AggregateIterable<Document> aggregationOutput = analyticsHelper.getMongoEventCollection()
				.aggregate( aggregationPipeline );
		// logger.debug("query {} ",output.getCommand());
		List topHosts = new ArrayList();
		MongoCursor<Document> cursor = aggregationOutput.iterator();
		while (cursor.hasNext()) {
			Document hostDocument = cursor.next();
			logger.debug( "hostDocument {} ", hostDocument );
			List dbHostList = (List) hostDocument.get( "host" );
			if ( null != dbHostList ) {
				int toIndex = numHosts;
				if ( dbHostList.size() < numHosts || numHosts == 0 ) {
					toIndex = dbHostList.size();
				}
				topHosts.addAll( dbHostList.subList( 0, toIndex ) );
			}
		}
		/*
		 * output.results().forEach(dbObject -> {
		 * logger.debug("dbObject {} ",dbObject); BasicDBList dbHostList =
		 * (BasicDBList) dbObject.get("host"); if(null != dbHostList){ int
		 * toIndex = numHosts; if(dbHostList.size() < numHosts || numHosts ==
		 * 0){ toIndex = dbHostList.size(); }
		 * //logger.debug("To Index {} ",toIndex);
		 * topHosts.addAll(dbHostList.subList(0, toIndex)); } });
		 */
		return topHosts;

	}

	private Document buildGroupByService(String[] metricsId, String[] divideBy, boolean isIncludeService) {
		Map<String, Object> groupFieldMap = new HashMap<>();
		
		if ( isIncludeService ) {
			groupFieldMap.put( "serviceName", "$data.summary.serviceName" );
		}
		groupFieldMap.put( "appId", "$appId" );
		groupFieldMap.put( "project", "$project" );
		groupFieldMap.put( "lifecycle", "$lifecycle" );
		groupFieldMap.put( "date", "$createdOn.date" );
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		for (String key : metricsId) {
			groupFields.append( key, new Document( "$sum", "$data.summary." + key ) );
		}
		if ( ArrayUtils.contains( divideBy, "numberOfSamples" ) ) {
			groupFields.append( "numberOfSamples", new Document( "$sum", "$data.summary.numberOfSamples" ) );
		}
		Document group = new Document( "$group", groupFields );
		return group;
	}

	public Document groupByHostNameDocument(String[] metricsId, String[] divideBy) {
		Map<String, Object> groupFieldMap = new HashMap<>();
		groupFieldMap.put( "appId", "$appId" );
		groupFieldMap.put( "project", "$project" );
		groupFieldMap.put( "lifecycle", "$lifecycle" );
		groupFieldMap.put( "date", "$createdOn.date" );
		groupFieldMap.put( "host", "$host" );
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		for (String key : metricsId) {
			groupFields.append( key, new Document( "$sum", "$data.summary." + key ) );
		}
		groupFields.append( "numberOfSamples", new Document( "$sum", "$data.summary.numberOfSamples" ) );

		Document group = new Document( "$group", groupFields );
		return group;
	}

	private Document trendingMetricsProjection(String[] metricsId) {
		Document projectCondition = new Document();
		projectCondition.append( "appId", 1 )
				.append( "lifecycle", 1 )
				.append( "project", 1 )
				.append( "serviceName", 1 )
				// .append("numberOfSamples", 1)
				.append( "date", 1 );
		for (String key : metricsId) {
			projectCondition.append( key, 1 );
		}
		if ( metricsId.length > 1 ) {
			projectCondition.append( "total", 1 );
		}
		ArrayList trueCondition = new ArrayList();
		trueCondition.add( "$numberOfSamples" );
		trueCondition.add( 0 );
		ArrayList trueCondArray = new ArrayList();
		trueCondArray.add( new Document( "$eq", trueCondition ) );
		trueCondArray.add( 1 );
		trueCondArray.add( "$numberOfSamples" );
		Document condition = new Document( "$cond", trueCondArray );
		projectCondition.append( "numberOfSamples", condition );
		Document projCondition = new Document( "$project", projectCondition );
		// operations.add(projCondition);
		return projCondition;
	}

	private Document groupByServiceNameAndHostName(String[] metricsId, String[] divideBy  ) {
		Map<String, Object> groupFieldMap = new HashMap<>();
		
		groupFieldMap.put( "serviceName", "$data.summary.serviceName" );
		
		groupFieldMap.put( "appId", "$appId" );
		groupFieldMap.put( "project", "$project" );
		groupFieldMap.put( "lifecycle", "$lifecycle" );
		groupFieldMap.put( "host", "$host" );
		groupFieldMap.put( "date", "$createdOn.date" );
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		for (String key : metricsId) {
			groupFields.append( key, new Document( "$sum", "$data.summary." + key ) );
		}
		groupFields.append( "numberOfSamples", new Document( "$sum", "$data.summary.numberOfSamples" ) );
		Document group = new Document( "$group", groupFields );
		return group;
	}

	public void addDivideByProjectionsToPipeline(String[] metricsId, String[] divideBy, List<Document> aggregationPipeline) {
		if ( null != divideBy ) {
			for (String key : metricsId) {
				for (String divisorStr : divideBy) {
					Document numSampleProjection = new Document();
					List<Object> numSampleDivideList = new ArrayList<>();
					numSampleDivideList.add( "$" + key );
					if ( divisorStr.equalsIgnoreCase( "numberOfSamples" ) ) {
						numSampleDivideList.add( "$numberOfSamples" );
					} else {
						if ( NumberUtils.isNumber( divisorStr ) ) {
							numSampleDivideList.add( Double.parseDouble( divisorStr ) );
						}
					}
					for (String metricsIdForProjection : metricsId) {
						if ( !key.equalsIgnoreCase( metricsIdForProjection ) ) {
							numSampleProjection.append( metricsIdForProjection, 1 );
						}
					}
					numSampleProjection.append( "appId", 1 )
							.append( "project", 1 )
							.append( "lifecycle", 1 )
							.append( "host", 1 )
							.append( "numberOfSamples", 1 )
							.append( "date", 1 );
					numSampleProjection.append( key, new Document( "$divide", numSampleDivideList ) );
					Document projectDivision = new Document( "$project", numSampleProjection );
					if ( numSampleDivideList.size() == 2 ) {
						aggregationPipeline.add( projectDivision );
					}
				}
			}
		}
	}

	public Document groupByLife(String[] metricsId) {
		Map<String, Object> groupFieldMap = new HashMap<>();
		groupFieldMap.put( "appId", "$_id.appId" );
		groupFieldMap.put( "project", "$_id.project" );
		groupFieldMap.put( "lifecycle", "$_id.lifecycle" );
		groupFieldMap.put( "date", "$_id.date" );
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		for (String key : metricsId) {
			groupFields.append( key, new Document( "$sum", "$" + key ) );
		}
		groupFields.append( "numberOfSamples", new Document( "$sum", "$numberOfSamples" ) );
		Document group = new Document( "$group", groupFields );
		return group;
	}

	private Document buildPrimaryProjectionGrouping(String[] metricsId) {
		Map<String, Object> groupFieldMap = new HashMap<>();
		
		groupFieldMap.put( "serviceName", "$_id.serviceName" );
		groupFieldMap.put( "appId", "$_id.appId" );
		groupFieldMap.put( "project", "$_id.project" );
		groupFieldMap.put( "lifecycle", "$_id.lifecycle" );
		groupFieldMap.put( "date", "$_id.date" );
		Document groupFields = new Document( "_id", new Document( groupFieldMap ) );
		for (String key : metricsId) {
			groupFields.append( key, new Document( "$sum", "$" + key ) );
		}
		groupFields.append( "numberOfSamples", new Document( "$sum", "$numberOfSamples" ) );
		Document group = new Document( "$group", groupFields );
		return group;
	}

	public Document addDateToDocumentQuery(Document query, int numDays, int dateOffSet) {

		
		if ( dateOffSet == 0 ) {
			if ( 1 == numDays ) {
				String dateString = DateUtil.buildMongoCreatedDateFromOffset( 0 );
				query.append( CREATED_ON + "." + DATE, dateString );
			} else {
				String fromDate = DateUtil.buildMongoCreatedDateFromOffset( numDays - 1 );
				String toDate = DateUtil.buildMongoCreatedDateFromOffset( 0 );
				query.append( CREATED_ON + "." + DATE, new Document( "$gte", fromDate ).append( "$lte", toDate ) );
			}
		} else {
			// int startOffSet = dateOffSet;
			// int endOffSet = dateOffSet - numDays ;
			// Fix: Most common use case is from agent going backwards.
			int startOffSet = dateOffSet + numDays;
			int endOffSet = dateOffSet;
			String fromDate = DateUtil.buildMongoCreatedDateFromOffset( startOffSet );
			String toDate = DateUtil.buildMongoCreatedDateFromOffset( endOffSet );
			query.append( CREATED_ON + "." + DATE, new Document( "$gte", fromDate ).append( "$lt", toDate ) );
		}

		logger.debug( "Query: {} ", query.toString() );
		return query;
	}

	public Document constructDocumentQuery(String appId, String project, String life, String category) {
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

	private String getReportPath(String category) {
		if ( "/csap/reports/health".equalsIgnoreCase( category ) ) {
			return "data";
		} else {
			return "data.summary";
		}
	}

	private DBCollection getEventCollection() {
		return mongoClient.getDB( EVENT_DB_NAME ).getCollection( EVENT_COLLECTION_NAME );
	}


	public List<Document> trendingOperationPipelineBuilder(String appId, String project, String life,
			String serviceNameFilter,
			String category, String[] metricsId, String[] divideBy, String allVmTotal, int numDays, int dateOffSet) {
		List<Document> mongoOperationPipeline = new ArrayList<>();

		mongoOperationPipeline.add(
				trendingPrimaryQueryBuilder( appId, project, life, serviceNameFilter, category,
						numDays, dateOffSet ) );

		mongoOperationPipeline.add( new Document( "$unwind", "$data.summary" ) );

		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {
			List<Document> serviceList = new ArrayList<Document>();
			for (String name : serviceNameFilter.split( MULTIPLE_SERVICE_DELIMETER )) {
				serviceList.add( new Document( "data.summary.serviceName", name ) );
			}
			Document unwindMatchOp = new Document( "$or", serviceList );
			mongoOperationPipeline.add( new Document( "$match", unwindMatchOp ) );
		}

		logger.debug( "allVmTotal{} ", allVmTotal );

		
		
		if ( "true".equalsIgnoreCase( allVmTotal ) ) {
			logger.debug( "average by host" );

			mongoOperationPipeline.add(
					groupByServiceNameAndHostName( metricsId, divideBy ) );

			Document projectcondtion = trendingMetricsProjection( metricsId );
			mongoOperationPipeline.add( projectcondtion );

			addDivideByProjectionsToPipeline( metricsId, divideBy, mongoOperationPipeline );
			// group by service name
			Document op4_sum_the_host_values = buildPrimaryProjectionGrouping( metricsId );
			mongoOperationPipeline.add( op4_sum_the_host_values );

		} else {
			// support for multiple services separated by "," used for aggregating multiple service (or old and new) in a single trend
			boolean isIncludeService=true ;
			if ( serviceNameFilter != null && serviceNameFilter.contains( MULTIPLE_SERVICE_DELIMETER ))
				isIncludeService=false ;
			
			mongoOperationPipeline.add( 
					buildGroupByService( metricsId, divideBy, isIncludeService ) 
			);
		}

		mongoOperationPipeline.add(
				trendingProjectionBuilder( metricsId, divideBy ) );

		mongoOperationPipeline.add(
				trendingProjectionConditionBuilder( metricsId ) );

		if ( null != divideBy && !"true".equalsIgnoreCase( allVmTotal ) ) {
			trendingDivideByBuilder( metricsId, divideBy, mongoOperationPipeline );
		}

		if ( StringUtils.isBlank( serviceNameFilter ) ) {

			mongoOperationPipeline.add( trendingGroupByLifeBuilder( metricsId ) );
			mongoOperationPipeline.add( trendingProjectionByLifeBuilder( metricsId ) );

		}

		Document sortOrder = new Document();
		sortOrder.append( "date", 1 );
		mongoOperationPipeline.add( new Document( "$sort", sortOrder ) );

		mongoOperationPipeline.add(
				trendingOutputGroupingBuilder( serviceNameFilter, metricsId ) );

		mongoOperationPipeline.add(
				trendingOutputFieldSelectionBuilder( serviceNameFilter, metricsId ) );

		if ( StringUtils.isBlank( project ) ) {
			Document sortByProjectOrder = new Document();
			sortByProjectOrder.append( "project", 1 );
			Document sortByProject = new Document( "$sort", sortByProjectOrder );
			mongoOperationPipeline.add( sortByProject );
		}
		logger.debug( "pipeline {} ", mongoOperationPipeline );
		return mongoOperationPipeline;
	}

	private Document trendingOutputFieldSelectionBuilder(String serviceNameFilter, String[] metricsId) {
		Document primaryTrendingProjection = new Document();
		for (String key : metricsId) {
			primaryTrendingProjection.append( key, 1 );
		}
		if ( metricsId.length > 1 ) {
			primaryTrendingProjection.append( "total", 1 );
			if ( logger.isDebugEnabled() ) {
				for (String key : metricsId) {
					primaryTrendingProjection.append( key, 1 );
				}
			}
		} else {
			for (String key : metricsId) {
				primaryTrendingProjection.append( key, 1 );
			}
		}
		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {
			primaryTrendingProjection.append( "serviceName", "$_id.serviceName" );
		}

		primaryTrendingProjection.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "date", 1 )
				.append( "_id", 0 );

		Document primaryTrendingProjectionOp = new Document( "$project", primaryTrendingProjection );
		return primaryTrendingProjectionOp;
	}

	private Document trendingOutputGroupingBuilder(String serviceNameFilter, String[] metricsId) {
		Map<String, Object> groupByDateMap = new HashMap<>();
		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			// if ( !serviceNameFilter.contains( MULTIPLE_SERVICE_DELIMETER ) )
			// {
			groupByDateMap.put( "serviceName", "$serviceName" );
			// }
		}
		groupByDateMap.put( "appId", "$appId" );
		groupByDateMap.put( "project", "$project" );
		groupByDateMap.put( "lifecycle", "$lifecycle" );

		Document groupServiceData = new Document( "_id", new BasicDBObject( groupByDateMap ) );
		groupServiceData.append( "date", new Document( "$push", "$date" ) );
		for (String key : metricsId) {
			groupServiceData.append( key, new Document( "$push", "$" + key ) );
		}
		if ( metricsId.length > 1 ) {
			groupServiceData.append( "total", new Document( "$push", "$total" ) );
		}
		Document groupData = new Document( "$group", groupServiceData );
		return groupData;
	}

	private Document trendingProjectionByLifeBuilder(String[] metricsId) {
		Document projectLife = new Document();
		for (String key : metricsId) {
			projectLife.append( key, 1 );
		}
		if ( metricsId.length > 1 ) {
			projectLife.append( "total", 1 );
		}
		projectLife.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "date", "$_id.date" )
				.append( "_id", 0 );
		Document projectLifeOp = new Document( "$project", projectLife );
		return projectLifeOp;
	}

	private Document trendingGroupByLifeBuilder(String[] metricsId) {
		Map<String, Object> groupByLifeMap = new HashMap<>();

		groupByLifeMap.put( "appId", "$appId" );
		groupByLifeMap.put( "project", "$project" );
		groupByLifeMap.put( "lifecycle", "$lifecycle" );
		groupByLifeMap.put( "date", "$date" );
		Document groupByLifeFields = new Document( "_id", new BasicDBObject( groupByLifeMap ) );
		for (String key : metricsId) {
			groupByLifeFields.append( key, new Document( "$sum", "$" + key ) );
		}
		if ( metricsId.length > 1 ) {
			groupByLifeFields.append( "total", new Document( "$sum", "$total" ) );
		}
		Document groupByLife = new Document( "$group", groupByLifeFields );
		return groupByLife;
	}

	private void trendingDivideByBuilder(String[] metricsId, String[] divideBy,
			List<Document> trendingQueryOperations) {
		for (String divisorStr : divideBy) {
			if ( metricsId.length == 1 ) {
				Document numSampleProjection = new Document();
				List<Object> numSampleDivideList = new ArrayList<>();
				numSampleDivideList.add( "$" + metricsId[0] );
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
						.append( "serviceName", 1 )
						.append( "numberOfSamples", 1 )
						.append( "date", 1 )

				;
				numSampleProjection.append( metricsId[0], new Document( "$divide", numSampleDivideList ) );
				Document projectDivision = new Document( "$project", numSampleProjection );
				if ( numSampleDivideList.size() == 2 ) {
					trendingQueryOperations.add( projectDivision );
				}
			} else if ( metricsId.length > 1 ) {
				Document numSampleProjection = new Document();
				List<Object> numSampleDivideList = new ArrayList<>();
				numSampleDivideList.add( "$total" );
				if ( divisorStr.equalsIgnoreCase( "numberOfSamples" ) ) {
					numSampleDivideList.add( "$numberOfSamples" );
				} else {
					if ( NumberUtils.isNumber( divisorStr ) ) {
						numSampleDivideList.add( Double.parseDouble( divisorStr ) );
					}
				}
				for (String key : metricsId) {
					numSampleProjection.append( key, 1 );
				}
				numSampleProjection.append( "appId", 1 )
						.append( "lifecycle", 1 )
						.append( "project", 1 )
						.append( "serviceName", 1 )
						.append( "numberOfSamples", 1 )
						.append( "date", 1 );
				numSampleProjection.append( "total", new BasicDBObject( "$divide", numSampleDivideList ) );
				Document projectDivison = new Document( "$project", numSampleProjection );
				if ( numSampleDivideList.size() == 2 ) {
					trendingQueryOperations.add( projectDivison );
				}
			}
		}
	}

	private Document trendingProjectionConditionBuilder(String[] metricsId) {
		Document trendingProjectionCondition = new Document();
		trendingProjectionCondition.append( "appId", 1 )
				.append( "lifecycle", 1 )
				.append( "project", 1 )
				.append( "serviceName", 1 )
				// .append("numberOfSamples", 1)
				.append( "date", 1 );
		for (String key : metricsId) {
			trendingProjectionCondition.append( key, 1 );
		}
		if ( metricsId.length > 1 ) {
			trendingProjectionCondition.append( "total", 1 );
		}
		ArrayList trueCondition = new ArrayList();
		trueCondition.add( "$numberOfSamples" );
		trueCondition.add( 0 );
		ArrayList trueCondArray = new ArrayList();
		trueCondArray.add( new Document( "$eq", trueCondition ) );
		trueCondArray.add( 1 );
		trueCondArray.add( "$numberOfSamples" );
		Document condition = new Document( "$cond", trueCondArray );
		trendingProjectionCondition.append( "numberOfSamples", condition );
		Document trendingProjectionConitionOperation = new Document( "$project", trendingProjectionCondition );
		return trendingProjectionConitionOperation;
	}

	private Document trendingProjectionBuilder(String[] metricsId, String[] divideBy) {
		Document trendingProjection = new Document(); // used to filter output
		List<Object> metricsList = new ArrayList<>();
		for (String key : metricsId) {
			metricsList.add( "$" + key );
			trendingProjection.append( key, 1 );
		}
		if ( metricsId.length > 1 ) {
			trendingProjection.append( "total", new Document( "$add", metricsList ) );
		}
		trendingProjection.append( "appId", "$_id.appId" )
				.append( "lifecycle", "$_id.lifecycle" )
				.append( "project", "$_id.project" )
				.append( "serviceName", "$_id.serviceName" )
				.append( "date", "$_id.date" )
				.append( "_id", 0 );
		if ( ArrayUtils.contains( divideBy, "numberOfSamples" ) ) {
			trendingProjection.append( "numberOfSamples", 1 );
		}
		Document projectionOperation = new Document( "$project", trendingProjection );
		return projectionOperation;
	}

	private Document trendingPrimaryQueryBuilder(String appId, String project, String life, String serviceNameFilter,
			String category, int numDays, int dateOffSet) {
		Document trendQuery = constructDocumentQuery( appId, project, life, category );
		trendQuery = addDateToDocumentQuery( trendQuery, numDays, dateOffSet );
		if ( StringUtils.isNotBlank( serviceNameFilter ) ) {

			List<Document> serviceList = new ArrayList<Document>();
			for (String name : serviceNameFilter.split( MULTIPLE_SERVICE_DELIMETER )) {
				serviceList.add( new Document( "serviceName", name ) );
			}
			Document anyServiceMatch = new Document( "$or", serviceList );
			trendQuery.append( "data.summary", new Document( "$elemMatch", anyServiceMatch ) );
		}
		Document op1_global_match = new Document( "$match", trendQuery );
		return op1_global_match;
	}

}
