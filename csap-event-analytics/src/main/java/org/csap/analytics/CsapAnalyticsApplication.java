package org.csap.analytics;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Pointcut;
import org.csap.CsapBootApplication;
import org.csap.analytics.db.AnalyticsDbReader;
import org.csap.analytics.db.AnalyticsHelper;
import org.csap.analytics.db.GlobalAnalyticsDbReader;
import org.csap.analytics.db.MetricsDataHandler;
import org.csap.analytics.db.MetricsDataReader;
import org.csap.analytics.db.ServiceAnalytics;
import org.csap.analytics.db.TrendingReportHelper;
import org.csap.analytics.http.ui.health.AnalyticsHealthMonitor;
import org.csap.analytics.monitoring.MongoClusterListener;
import org.csap.analytics.monitoring.MongoCommandListener;
import org.csap.integations.CsapPerformance;
import org.csap.integations.CsapRolesEnum;
import org.csap.integations.CsapSecurityConfiguration;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.exceptions.EncryptionOperationNotPossibleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.client.RestTemplate;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

@CsapBootApplication
@EnableAsync
@ConfigurationProperties ( prefix = "my-service-configuration" )
public class CsapAnalyticsApplication {

	final Logger logger = LoggerFactory.getLogger( CsapAnalyticsApplication.class );

	public static void main ( String[] args ) {

		SpringApplication.run( CsapAnalyticsApplication.class, args );

		// CsapCommonConfiguration wires in ldap, perf monitors, ...
	}

	public final static String BASE_URL = "/";
	public final static String API_URL = BASE_URL + "api";
	public final static String REPORT_URL = API_URL + "/report";
	public final static String JSP_VIEW = "/view/";
	public final static String ATTRIBUTES_CACHE = "attributesCache";
	public final static String SIMPLE_REPORT_CACHE = "simpleReportCache";
	public final static String DETAILS_REPORT_CACHE = "detailsReportCache";
	public final static String METRICS_REPORT_CACHE = "metricsReportCache";
	public final static String HISTORICAL_REPORT_CACHE = "historicalPerformanceCache";
	public final static String NUM_DAYS_CACHE = "numDaysCache";

	private MongoConfig mongoConfig;

	public enum SimonIds {

		// add as many as needed. Optionally read values
		// from limits.yml
		exceptions( "health.exceptions" ),
		nullPointer( "health.nullPointer" );

		public String id;

		private SimonIds( String simonId ) {
			this.id = simonId;
		}
	}

	@Bean
	@ConditionalOnProperty ( "csap.security.enabled" )
	public CsapSecurityConfiguration.CustomHttpSecurity customRules () {

		// @formatter:off
		return (httpSecurity -> {
			httpSecurity
			// CSRF adds complexity - refer to
			// https://docs.spring.io/spring-security/site/docs/current/reference/htmlsingle/#csrf
			// csap.security.csrf also needs to be enabled or this will be ignored
			.csrf()
				.requireCsrfProtectionMatcher( CsapSecurityConfiguration.buildRequestMatcher( "/login*" ) )
				.and()
				
			.authorizeRequests()
			//
			//
				.antMatchers( "/api/**" )
					.permitAll()
				//
				//
				.antMatchers( "/webjars/**", "/noAuth/**", "/js/**", "/css/**", "/images/**" )
					.permitAll()
				//
				.anyRequest()
					.access( CsapSecurityConfiguration.hasAnyRole( CsapRolesEnum.view ) );
			// .anyRequest()
			// .access( "hasAnyRole(@CsapSecurityConfiguration.getViewGroups(),
			// @CsapSecurityConfiguration.getAdminGroups())" );
		});

		// @formatter:on

	}
	// @formatter:on

	// Summary of all public events in class
	@Pointcut ( "within(org.csap.analytics.db..*)" )
	private void dbPC () {
	}

	;

	@Around ( "dbPC()" )
	public Object linuxAdvice ( ProceedingJoinPoint pjp )
			throws Throwable {

		Object obj = CsapPerformance.executeSimon( pjp, "aop.db." );

		return obj;
	}

	// configure @Scheduled thread pool
	@Bean
	public TaskScheduler taskScheduler () {
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setThreadNamePrefix( CsapAnalyticsApplication.class.getSimpleName() + "Scheduler" );
		scheduler.setPoolSize( 2 );
		return scheduler;
	}

	// configure @Async thread pool
	final public static String HEALTH_EXECUTOR = "CsapHealthExecutor";

	@Bean ( HEALTH_EXECUTOR )
	public TaskExecutor taskExecutor () {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setThreadNamePrefix( CsapAnalyticsApplication.class.getSimpleName() + "@Async" );
		taskExecutor.setMaxPoolSize( 5 );
		taskExecutor.setQueueCapacity( 300 );
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	@Bean
	public TrendingReportHelper getTrendingReportHelper () {
		return new TrendingReportHelper();
	}

	@Bean
	public GlobalAnalyticsDbReader getGlobalAnalyticsDbReader () {
		return new GlobalAnalyticsDbReader();
	}

	@Bean
	public AnalyticsHelper getAnalyticsHelper () {
		return new AnalyticsHelper();
	}

	@Bean
	public ServiceAnalytics getServiceAnalytics () {
		return new ServiceAnalytics();
	}

	@Bean
	public MetricsDataReader getMetricsDataReader () {
		return new MetricsDataReader();
	}

	@Bean
	public MetricsDataHandler getMetricsDataHandler () {
		return new MetricsDataHandler();
	}

	@Bean ( name = "genericRestTemplate" )
	public RestTemplate getGenericRestTemplate () {

		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
		// factory.setHttpClient(httpClient);
		// factory.getHttpClient().getConnectionManager().getSchemeRegistry().register(scheme);

		factory.setConnectTimeout( 120000 );
		factory.setReadTimeout( 120000 );

		RestTemplate restTemplate = new RestTemplate( factory );
		restTemplate.getMessageConverters().clear();
		restTemplate.getMessageConverters().add( new FormHttpMessageConverter() );
		restTemplate.getMessageConverters().add( new StringHttpMessageConverter() );

		return restTemplate;
	}

	final static int MAX_MONGO_IDLE_MS = 60 * 1000;

	@Bean
	public MongoClient getMongoClient ( StandardPBEStringEncryptor encryptor )
			throws UnknownHostException {

		logger.warn( "\n === Mongo Connection using: \n === {} \n Max Idle Seconds: {}",
			mongoConfig, MAX_MONGO_IDLE_MS / 1000 );

		MongoClient client = null;
		try {
			String[] mongoHosts = getMongoConfig().getHosts().split( " " );
			List<ServerAddress> serverList = new ArrayList<>();
			for ( String host : mongoHosts ) {
				ServerAddress serverAddress = new ServerAddress( host, getMongoConfig().getPort() );
				serverList.add( serverAddress );
			}
			String password = getMongoConfig().getPassword();
			try {
				password = encryptor.decrypt( getMongoConfig().getPassword() );
			} catch (EncryptionOperationNotPossibleException e) {
				logger.warn( "Password is not encrypted. Use CSAP encrypt to generate" );
			}

			MongoCredential credential = MongoCredential.createMongoCRCredential( getMongoConfig().getUser(),
				getMongoConfig().getUserDb(),
				password.toCharArray() );

			List<MongoCredential> credentialList = new ArrayList<>();
			credentialList.add( credential );
			MongoClientOptions options = MongoClientOptions
				.builder()
				.addCommandListener( new MongoCommandListener() )
				.addClusterListener( new MongoClusterListener( ReadPreference.nearest() ) )
				.maxConnectionIdleTime( MAX_MONGO_IDLE_MS )
				.build();

			client = new MongoClient( serverList, credentialList, options );
		} catch (Exception e) {
			logger.error( "Exception while connecting to mongodb.", e );
		}
		if ( null != client ) {
			int idleTime = client.getMongoClientOptions().getMaxConnectionIdleTime();
			int lifeTime = client.getMongoClientOptions().getMaxConnectionLifeTime();
			logger.info( "Idle Time :: " + idleTime );
			logger.info( "Life Time :: " + lifeTime );
		}
		return client;
	}

	public MongoConfig getMongoConfig () {
		return mongoConfig;
	}

	public void setMongoConfig ( MongoConfig mongoConfig ) {
		this.mongoConfig = mongoConfig;
	}

}
