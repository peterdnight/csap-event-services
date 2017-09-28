package org.csap;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Pointcut;
import org.csap.alerts.AlertProcessor;
import org.csap.data.admin.AdminDBHelper;
import org.csap.data.db.EventDataHelper;
import org.csap.data.monitoring.MongoClusterListener;
import org.csap.data.monitoring.MongoCommandListener;
import org.csap.integations.CsapPerformance;
import org.csap.integations.CsapRolesEnum;
import org.csap.integations.CsapSecurityConfiguration;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.exceptions.EncryptionOperationNotPossibleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.resource.VersionResourceResolver;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

@CsapBootApplication	
@EnableAsync
@ConfigurationProperties(prefix = "my-service-configuration")
public class CsapDataApplication extends WebMvcConfigurerAdapter {

	final Logger logger = LoggerFactory.getLogger( getClass() );

	public static void main ( String[] args ) {
		SpringApplication.run( CsapDataApplication.class, args );
	}

	public static boolean isRunningOnDesktop () {
		if ( System.getenv( "STAGING" ) == null ) {
			return true;
		}
		return false;
	}
	int ONE_YEAR_SECONDS = 60 * 60 * 24 * 365;
	final String VERSION = "1.1";
	// https://spring.io/blog/2014/07/24/spring-framework-4-1-handling-static-web-resources
	// http://www.mscharhag.com/spring/resource-versioning-with-spring-mvc
	@Override
	public void addResourceHandlers ( ResourceHandlerRegistry registry ) {

		if ( isRunningOnDesktop() ) {
			logger.warn( "\n\n\n Desktop detected: Caching DISABLED \n\n\n" );
			ONE_YEAR_SECONDS = 0;
		}

		String version="start"+System.currentTimeMillis();
		VersionResourceResolver versionResolver = new VersionResourceResolver()
			// .addFixedVersionStrategy( version, "/**/*.js" ) //Enable this
			// if we use a JavaScript module loader
				.addFixedVersionStrategy( version,
						"/**/modules/**/*.js" ) // requriesjs uses relative paths
			.addContentVersionStrategy( "/**" );

		// A Handler With Versioning - note images in css files need to be
		// resolved.
		registry
			.addResourceHandler( "/**/*.js", "/**/*.css", "/**/*.png", "/**/*.gif", "/**/*.jpg" )
			.addResourceLocations( "classpath:/static/", "classpath:/public/" )
			.setCachePeriod( ONE_YEAR_SECONDS )
			.resourceChain( true )
			.addResolver( versionResolver );

	}
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

	public final static String BASE_URL = "/";
	public final static String API_URL = BASE_URL + "eventApi";
	public final static String JSP_VIEW = "/view/";
	public final static String ADMIN_API = BASE_URL + "api/admin";
	public final static String EVENT_API = BASE_URL + "api/event";

	private MongoConfig mongoConfig;

	@Inject
	Environment env;



	// configure @Scheduled thread pool
	@Bean
	public TaskScheduler taskScheduler () {
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setThreadNamePrefix( CsapDataApplication.class.getSimpleName() + "Scheduler" );
		scheduler.setPoolSize( 2 );
		return scheduler;
	}

	final public static String HEALTH_EXECUTOR = "CsapHealthExecutor" ;
	@Bean(HEALTH_EXECUTOR)
	public TaskExecutor taskExecutor () {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setThreadNamePrefix(  CsapDataApplication.class.getSimpleName() + "@Async" );
		taskExecutor.setMaxPoolSize( 5 );
		taskExecutor.setQueueCapacity( 300 );
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	// @Override
	// public Executor getAsyncExecutor () {
	// return Executors.newScheduledThreadPool( 5 );
	// }
	//
	// @Override
	// public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler ()
	// {
	// AsyncUncaughtExceptionHandler handler = new
	// SimpleAsyncUncaughtExceptionHandler();
	// return handler;
	// }

	@Bean
	@ConditionalOnProperty("csap.security.enabled")
	public CsapSecurityConfiguration.CustomHttpSecurity httpAuthorization () {

		// ref
		// https://docs.spring.io/spring-security/site/docs/3.0.x/reference/el-access.html
		// String rootAcl="hasRole('ROLE_AUTHENTICATED')";

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
			
			// protect using csap api filter
			.antMatchers( "/api/**", "/eventApi/**" )
				.permitAll()
			//
			//
			.antMatchers( "/webjars/**", "/noAuth/**", "/js/**", "/css/**", "/images/**" )
				.permitAll()
			// protect admin operations
			//
			.antMatchers( "/eventApi/delete/**", "/eventApi/insert/**", "/eventApi/deleteBySearch/**",
				"/eventApi/update/**" )
				.access( CsapSecurityConfiguration.hasAnyRole( CsapRolesEnum.admin ) )
			//
			//
			.anyRequest()
				.access( CsapSecurityConfiguration.hasAnyRole( CsapRolesEnum.view ) );

		});

		// @formatter:on

	}

	@Bean
	public EventDataHelper getEventDataHelper () {
		return new EventDataHelper();
	}

	/*
	 * @Bean public Jmx_EventDBMonitoring getJmx_EventDBMonitoring(){ return new
	 * Jmx_EventDBMonitoring(); }
	 */

	@Bean
	public AdminDBHelper getAdminDBHelper () {
		return new AdminDBHelper();
	}

	final static int MAX_MONGO_IDLE_MS = 60 * 1000;

	/*
	 * @Bean public Jmx_HealthCheck getJmx_HealthCheck(){ return new
	 * Jmx_HealthCheck(); }
	 */
	/*
	 * @Bean public Jmx_SimonRegistration getJmx_SimonRegistration() { return
	 * new Jmx_SimonRegistration(); }
	 */
	@Bean
	public MongoClient getMongoClient ( StandardPBEStringEncryptor encryptor )
			throws UnknownHostException {

		logger.warn( "\n === Mongo Connection using: \n === {} \\n Max Idle Seconds: {} \n Connection Pool: *Mongo Default", 
			mongoConfig, MAX_MONGO_IDLE_MS / 1000 );

		if ( logger.isDebugEnabled() ) {
			logger.debug( "Replica set-->", getMongoConfig() );
		}
		MongoClient client = null;
		try {
			String[] replicaMemberArray = getMongoConfig().getHosts().split( " " );
			List<ServerAddress> serverList = new ArrayList<>();
			for ( String host : replicaMemberArray ) {
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
				// .connectionsPerHost( 10 ) defaults to 100
				.build();

			client = new MongoClient( serverList, credentialList, options );
			// client = new MongoClient(Arrays.asList(new
			// ServerAddress(host,port)));
		} catch (Exception e) {
			logger.error( "Exception while connecting to mongodb.", e );
		}
		return client;
	}

	@Pointcut("within(@org.springframework.stereotype.Controller *)")
	private void mvcPC () {
	}

	;

	@Around("mvcPC()")
	public Object mvcAdvice ( ProceedingJoinPoint pjp )
			throws Throwable {

		Object obj = CsapPerformance.executeSimon( pjp, "aop.controller." );

		return obj;
	}

	// All restController events
	@Pointcut("within(@org.springframework.web.bind.annotation.RestController *)")
	private void mvcRestPC () {
	}

	;

	@Around("mvcRestPC()")
	public Object mvcRestAdvice ( ProceedingJoinPoint pjp )
			throws Throwable {

		Object obj = CsapPerformance.executeSimon( pjp, "aop.rest." );

		return obj;
	}

	public MongoConfig getMongoConfig () {
		return mongoConfig;
	}

	public void setMongoConfig ( MongoConfig mongoConfig ) {
		this.mongoConfig = mongoConfig;
	}

}
