package org.csap.analytics.http.ui.health;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.csap.alerts.AlertProcessor;
import org.csap.helpers.CsapRestTemplateFactory;
import org.csap.integations.CsapInformation;
import org.csap.integations.CsapPerformance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Service
@ConfigurationProperties(prefix = "my-service-configuration.health") 
public class AnalyticsHealthMonitor {
	private Logger logger = LoggerFactory.getLogger( getClass() );


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
				return AnalyticsHealthMonitor.class.getName();
			}
		};

		return health;

	}
	

	private RestTemplate restTemplate;
	private ObjectMapper jacksonMapper = new ObjectMapper();

	@Autowired
	CsapInformation csapInformation;

	public AnalyticsHealthMonitor( RestTemplate restTemplate ) {
		super();
		this.restTemplate = restTemplate;
	}

	private boolean isHealthy = true;

	public Boolean getAnalyticsStatus () {
		return isHealthy;
	}

	// @Async

	private final long SECONDS = 1000;

	@Scheduled(initialDelay = 2 * SECONDS, fixedDelay = 30 * SECONDS)
	public void monitorMetrics () {

//		Configurator.setLevel( AnalyticsHealthMonitor.class.getName(), Level.DEBUG );
		List<String> latestIssues = new ArrayList<>();

		logger.debug( "calling metrics and latest event api" );
		String[] appIdLifeResourceCategory = getAppIdLifeReportPattern();

		queryLatestMetrics( latestIssues, appIdLifeResourceCategory );	
		
		queryLatestUserEvents( latestIssues, appIdLifeResourceCategory );
		
		queryUserAnalytics( latestIssues, appIdLifeResourceCategory );
		
		
		
		failureReasons = latestIssues;
	}

	private void queryUserAnalytics ( List<String> latestIssues, String[] appIdLifeResourceCategory ) {
		// TODO Auto-generated method stub
		
	}


	SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
	private void queryLatestUserEvents ( List<String> latestIssues, String[] appIdLifeResourceCategory ) {
		
		Date yesterDay = new Date( System.currentTimeMillis() - TimeUnit.DAYS.toMillis( 1 )) ;
		String from=",from="+  formatter.format(yesterDay);
		
		// event ui has custom params used for querying from ui
		URI eventUiUri = UriComponentsBuilder.fromHttpUrl( csapInformation.getLoadBalancerUrl() )
				.path( "/data/api/event" )
				.queryParam( "length", 5 )
				.queryParam( "start", 0 )
				.queryParam( "searchText", "simpleSearchText=/csap/ui/*,appId=" + appIdLifeResourceCategory[0] +
					",lifecycle=" +appIdLifeResourceCategory[1] + from)
			.build().toUri();
		try {

			// latest metric event and then data
			String latestEventResponse = restTemplate.getForObject( eventUiUri, String.class );
			logger.debug( "latest user events using {} \n {} ", eventUiUri  );
			if ( StringUtils.isNotBlank( latestEventResponse ) && !latestEventResponse.equalsIgnoreCase( "null" ) ) {
				JsonNode eventJson = jacksonMapper.readTree( latestEventResponse );
				JsonNode dataNode = eventJson.at( "/data" ) ;
				
				if ( dataNode.isArray() && dataNode.size() > 1 ) {

					logger.debug( "all good" );
				} else {
					String reason = "Did not find data: " + eventUiUri ;
					latestIssues.add( reason );
					logger.error( reason);
				}
				
			} else {
				logger.debug( "Latest event is null for url {} ", eventUiUri );
			}
			
		} catch (Exception e) {
			latestIssues.add( "Exception while running: " + eventUiUri + " Exception: " + e.getClass().getSimpleName() );
			logger.error( "{} Failed to load: {}", eventUiUri, CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}
	}

	private void queryLatestMetrics ( List<String> latestIssues, String[] appIdLifeResourceCategory ) {
		// Get the latest event information
		URI latestEventUri = UriComponentsBuilder.fromHttpUrl( csapInformation.getLoadBalancerUrl() )
				.path( "/data/eventApi/latest" )
				.queryParam( "appId", appIdLifeResourceCategory[0] )
				.queryParam( "life", appIdLifeResourceCategory[1] )
				.queryParam( "category", appIdLifeResourceCategory[3] ) 
				.build().toUri();
		try {

			// latest metric event and then data
			String latestEventResponse = restTemplate.getForObject( latestEventUri, String.class );
			logger.debug( "latestEvent {} ", latestEventResponse );
			if ( StringUtils.isNotBlank( latestEventResponse ) && !latestEventResponse.equalsIgnoreCase( "null" ) ) {
	
				queryMetricsData( latestIssues, appIdLifeResourceCategory[2], latestEventResponse );
				
			} else {
				logger.debug( "Latest event is null for url {} ", latestEventUri );
			}
			
		} catch (Exception e) {
			latestIssues.add( "Exception while running: " + latestEventUri + " Exception: " + e.getClass().getSimpleName() );
			logger.error( "Exception from: {}, {}", latestEventUri, CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}
	}

	private void queryMetricsData ( List<String> latestIssues, String category, String latestEventResponse )
			throws Exception {
		
		
		JsonNode eventJson = jacksonMapper.readTree( latestEventResponse );
		String host = eventJson.at( "/host" ).asText() ;
		String life = eventJson.at( "/lifecycle" ).asText();
		String appId = eventJson.at( "/appId" ).asText();
		logger.debug( "host {} life {} appId {} ", host, life, appId );
		
		
		if ( StringUtils.isNotBlank( host ) && StringUtils.isNotBlank( appId ) && StringUtils.isNotBlank( life ) ) {
			URI urlWithParams = UriComponentsBuilder.fromHttpUrl( csapInformation.getLoadBalancerUrl() )
					.path( "/analytics/api/metrics/" + host + "/" + category )
					.queryParam( "appId", appId)
					.queryParam( "life", life )
					.queryParam( "numberOfDays", 1 )
					.queryParam( "padLatest", false )
				.build().toUri();
			
			logger.debug( "latest metrics url {} ", urlWithParams );
			String metricsResponse = restTemplate.getForObject( urlWithParams, String.class );

			//logger.debug( "latest metrics: \n{} ", metrics );

			//if ( null != metrics && !metrics.contains( "ERROR getting data" ) ) {
			if ( ( null != metricsResponse) && 
					( metricsResponse.length() > 100)  ) {
				logger.debug( "Healthy true" );
				
			} else {
				latestIssues.add( "Failed to get metrics" );
				logger.warn( "latestMetrics {} ", metricsResponse );
			}
		}
	}

	private volatile List<String> failureReasons = new ArrayList<>();

	public List<String> getFailureReasons () {
		return failureReasons;
	}

	Random random = new Random();
	private String[] getAppIdLifeReportPattern () {
		
		String target = getAppIds().get( random.nextInt( getAppIds().size() ) )  ;

		target += ":" +   getLifes().get( random.nextInt( getLifes().size() ) ) ;
		
		target += ":" +  testCategories[ random.nextInt( testCategories.length )];
		
		return target.split( ":" ) ;
	}

	
	private String[] testCategories = {
			"resource_30:/csap/metrics/host/30/data",
			"resource_300:/csap/metrics/host/300/data",
			"resource_3600:/csap/metrics/host/3600/data",
			"service_30:/csap/metrics/process/30/data",
			"service_300:/csap/metrics/process/300/data",
			"service_3600:/csap/metrics/process/3600/data",
			"jmx_30:/csap/metrics/jmx/standard/30/data",
			"jmx_300:/csap/metrics/jmx/standard/300/data",
			"jmx_3600:/csap/metrics/jmx/standard/3600/data"
	} ;


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
