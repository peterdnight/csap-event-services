package org.csap;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.csap.alerts.AlertProcessor;
import org.csap.data.EventMetaData;
import org.csap.data.db.EventDataReader;
import org.csap.helpers.CsapRestTemplateFactory;
import org.csap.integations.CsapPerformance;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.MongoClient;

@Configuration
@ConfigurationProperties(prefix = "my-service-configuration.health") 
public class HealthMonitor {

	private Logger logger = LoggerFactory.getLogger( getClass() );
	

	ObjectMapper jacksonMapper = new ObjectMapper();
	
	private List<String> appIds = new ArrayList<>() ;
	private List<String> lifes = new ArrayList<>() ;

	@PostConstruct
	public void showConfiguration() {
		logger.info( "checking health every minute using appids: {} and lifes: {}", getAppIds(), getLifes()  ); ;
	}
	
	@Bean
	public CsapPerformance.CustomHealth myHealth () {

		// Push any work into background thread to avoid blocking collection

		CsapPerformance.CustomHealth health = new CsapPerformance.CustomHealth() {

			@Autowired
			AlertProcessor alertProcessor;
			
			@Override
			public boolean isHealthy ( ObjectNode healthReport )
					throws Exception {
				logger.debug( "Invoking custom health" );

				getFailureReasons().forEach( reason -> {

					alertProcessor.addFailure( this, healthReport, reason );
					
				} );

				if ( getFailureReasons().size() > 0 )
					return false;
				return true;
			}

			@Override
			public String getComponentName () {
				return CsapDataApplication.class.getName();
			}
		};

		return health;

	}

	private volatile List<String> failureReasons = new ArrayList<>();

	private List<String> getFailureReasons () {
		return failureReasons;
	}
	

	@Autowired
	private ApplicationContext applicationContext;
	
	private final long SECONDS=1000 ;

	@Scheduled(fixedDelay= 30*SECONDS)
	public void verify_mongo_cluster_members () {
		
		List<String> latestIssues = new ArrayList<>();

		logger.debug( "Updating replica status" );
		CommandResult replicaStatusResult = applicationContext.getBean( MongoClient.class ).getDB( "admin" ).command( "replSetGetStatus" );
		BasicDBList dbList = (BasicDBList) replicaStatusResult.get( "members" );
		if ( null != dbList ) {
			for ( Object obj : dbList ) {
				BasicDBObject dbObject = (BasicDBObject) obj;
				int healthRc = dbObject.getInt( "health" );
				if ( healthRc == 0 ) {
					latestIssues.add( "Mongo Instance: " + dbObject.getString( "name" ) + " healthRc:" + healthRc );
				}
			}
		} else {
			latestIssues.add( "Failed to get mongo replica status" );
		}

		failureReasons = latestIssues ;
	}
	

	Random random = new Random();
	@Inject 
	EventDataReader eventDataReader ;
	
	@Scheduled(fixedDelay = 60*SECONDS)
	public void verify_search_filters () {
		
		List<String> latestIssues = new ArrayList<>();

		LocalDate today = LocalDate.now(); //format( DateTimeFormatter.ofPattern( "HH:mm:ss,   MMMM d  uuuu " ) ) ;
		LocalDate weekAgo = LocalDate.now().minusDays( 7 ); //format( DateTimeFormatter.ofPattern( "HH:mm:ss,   MMMM d  uuuu " ) ) ;

		try {
			String appId = getAppIds().get(random.nextInt( getAppIds().size() )) ;
			String life = getLifes().get( random.nextInt( getLifes().size() ) )  ;
			String fromDate=weekAgo.format( DateTimeFormatter.ofPattern( "M/d/yyyy" ) );
			String toDate=today.format( DateTimeFormatter.ofPattern( "M/d/yyyy" ) );
			Split healthTime = SimonManager.getStopwatch( EventDataReader.SEARCH_FILTER_KEY + "health" ).start();
			
			logger.debug( "Testing: {}, {}, {}, {}", appId, life, fromDate, toDate);
			EventMetaData searchFilters =  eventDataReader.getEventMetaData( 
				 appId , 
				life, 
				fromDate, toDate, 60 ) ;
			healthTime.stop() ;
			
			logger.debug( "query: {}, {}, {}, {}, Time: {} searchFilters: \n {}",
				healthTime.presentRunningFor(), appId, life, fromDate, toDate);
			if ( searchFilters.getAppIds().isEmpty() || searchFilters.getUiUsers().isEmpty() ) {
				latestIssues.add( EventDataReader.SEARCH_FILTER_KEY + " - Failed to find users or appids"  );
			}
		} catch (Exception e) {
			latestIssues.add( EventDataReader.SEARCH_FILTER_KEY + " - Failed to get resutls" + e.getMessage()  );
			logger.error( "Failed to load: {}", CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}


		failureReasons = latestIssues ;
	}

	public List<String> getAppIds () {
		return appIds;
	}

	public void setAppIds ( List<String> appIds ) {
		this.appIds = appIds;
	}

	public List<String> getLifes () {
		return lifes;
	}

	public void setLifes ( List<String> lifes ) {
		this.lifes = lifes;
	}

}
