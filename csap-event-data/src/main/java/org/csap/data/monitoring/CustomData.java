package org.csap.data.monitoring;

import javax.inject.Inject;

import org.csap.data.MongoConstants;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.support.MetricType;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.MongoClient;

/**
 * 
 */
@Service
@ManagedResource(objectName = "org.csap:application=CsapData,name=Performance", description = "Exports performance data to external systems")
public class CustomData {

	ObjectMapper jacksonMapper = new ObjectMapper();
	
	@Inject
	private MongoClient mongoClient;
	
	@ManagedMetric(category = "UTILIZATION", displayName = "DB Storage Size", 
			description = "Mongo DB "+MongoConstants.EVENT_DB_NAME + " Storage size", metricType = MetricType.GAUGE)
	synchronized public long getEventDBStorageSize() {
		CommandResult dbStats = mongoClient.getDB(MongoConstants.EVENT_DB_NAME).getStats();
		long storageSize = dbStats.getLong("storageSize");
		return convertSize(storageSize); 
	}
	
	@ManagedMetric(category = "UTILIZATION", displayName = "DB Data Size", 
			description = "Mongo DB "+MongoConstants.EVENT_DB_NAME + " Data size", metricType = MetricType.GAUGE)
	synchronized public long getEventDBDataSize() {
		CommandResult dbStats = mongoClient.getDB(MongoConstants.EVENT_DB_NAME).getStats();
		long dataSize = dbStats.getLong("dataSize");
		return convertSize(dataSize); 
	}
	
	@ManagedMetric(category = "UTILIZATION", displayName = "DB Storage Size", 
			description = "Mongo DB metricsDb" + " Storage size", metricType = MetricType.GAUGE)
	synchronized public long getMetricsDBStorageSize() {
		CommandResult dbStats = mongoClient.getDB("metricsDb").getStats();
		long storageSize = dbStats.getLong("storageSize");
		return convertSize(storageSize); 
	}
	
	@ManagedMetric(category = "UTILIZATION", displayName = "DB Data Size", 
			description = "Mongo DB metricsDb" + " Data size", metricType = MetricType.GAUGE)
	synchronized public long getMetricsDBDataSize() {
		CommandResult dbStats = mongoClient.getDB("metricsDb").getStats();
		long dataSize = dbStats.getLong("dataSize");
		return convertSize(dataSize); 
	}
	
	@ManagedMetric(category = "UTILIZATION", displayName = "Number of records in collection", 
			description = "Number of records in Metrics DB", metricType = MetricType.GAUGE)
	synchronized public long getNumRecordsInMetrics() {
		CommandResult dbStats = mongoClient.getDB("metricsDb").getCollection("metrics").getStats();
		long count = dbStats.getLong("count");
		return count; 
	}
	
	private long convertSize(long size){
		double sizeInMB = (size / (1024d * 1024d));		
		return Math.round(sizeInMB);
	}
	@ManagedMetric(category = "UTILIZATION", displayName = "Number of records in collection", 
			description = "Number of records in "+MongoConstants.EVENT_COLLECTION_NAME, metricType = MetricType.GAUGE)
	synchronized public long getNumRecordsInEvents() {
		CommandResult dbStats = mongoClient.getDB(MongoConstants.EVENT_DB_NAME).getCollection(MongoConstants.EVENT_COLLECTION_NAME).getStats();
		long count = dbStats.getLong("count");
		return count; 
	}
	
	@ManagedMetric(category = "UTILIZATION", displayName = "DB Index Size", 
			description = "Mongo DB "+MongoConstants.EVENT_DB_NAME + " Index size", metricType = MetricType.GAUGE)
	synchronized public long getDBIndexSize() {
		CommandResult dbStats = mongoClient.getDB(MongoConstants.EVENT_DB_NAME).getStats();
		long indexSize = dbStats.getLong("indexSize");
		return convertSize(indexSize); 
	}



}
