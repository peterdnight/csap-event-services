package test.analytics;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongoImportExecutable;
import de.flapdoodle.embed.mongo.MongoImportProcess;
import de.flapdoodle.embed.mongo.MongoImportStarter;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.DownloadConfigBuilder;
import de.flapdoodle.embed.mongo.config.ExtractedArtifactStoreBuilder;
import de.flapdoodle.embed.mongo.config.IMongoImportConfig;
import de.flapdoodle.embed.mongo.config.MongoImportConfigBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.runtime.Network;

@Service
public class MongoEmbedded {

	private  int MONGO_TEST_PORT = 12345; // client hardcoded in app.yml
	Random randomGenerator = new Random();
	
	private static final String MONGO_TEST_STORAGE_LOCATION = "./target/storage";
	
	private static Logger logger = LoggerFactory.getLogger( MongoEmbedded.class );
	private static MongodStarter starter = null;// MongodStarter.getDefaultInstance();
	private static MongodExecutable mongodExe;
	private static MongodProcess mongod;
	private static MongoClient mongoClient;
	private static MongoImportProcess mongoImport;
	private ObjectMapper jacksonMapper = new ObjectMapper();

	public ObjectMapper getJacksonMapper() {
		return jacksonMapper;
	}
	
	

	@PostConstruct
	public void setUpMongo() throws Exception {
		try {
			//MONGO_TEST_PORT += randomGenerator.nextInt( 100 ) ;
			logger.info( "\n\n Downloading and Starting Mongo server with port: {} and data in {} \n\n" , MONGO_TEST_PORT , MONGO_TEST_STORAGE_LOCATION);
			
			// Delete storage in case dir is still around
			FileUtils.deleteQuietly( new File( MONGO_TEST_STORAGE_LOCATION ) );
			
			Command command = Command.MongoD;
			
			Logger mongoLogger = LoggerFactory.getLogger( IRuntimeConfig.class );

			IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
					.defaultsWithLogger( Command.MongoD, mongoLogger )
					.artifactStore( new ExtractedArtifactStoreBuilder()
							.defaults( command )
							.extractDir( new FixedPath( "./target" ) )
							.download( new DownloadConfigBuilder()
									.defaultsForCommand( command )
									.artifactStorePath( new FixedPath( "./target" ) )
					// .downloadPath("http://csaptools.yourcompany.com/mongo/")
					) )
					.build();

			starter = MongodStarter.getInstance( runtimeConfig );
			Storage storage = new Storage( MONGO_TEST_STORAGE_LOCATION, null, 0 );
			mongodExe = starter.prepare( new MongodConfigBuilder()
					.version( Version.V3_0_6 )
					.net( new Net( MONGO_TEST_PORT, Network.localhostIsIPv6() ) )
					.replication( storage )
					.build() );
			String path = mongodExe.getFile().executable().toString();
			logger.info( "Mongo extracted {} ", path );
			mongod = mongodExe.start();
		} catch (Exception e) {
			logger.error( "Failed to download and start mongo", e );
		}

		try {
			logger.info( "\n\n Adding user and loading test data \n\n" );
			mongoClient = new MongoClient( "localhost", MONGO_TEST_PORT );
			createUser();
			retrieveTestData( "events.json" );
			mongoImport = startMongoImport( MONGO_TEST_PORT, "event", "eventRecords", "./target/events.json", false, true, true );
		} catch (Exception e) {
			logger.error( "Failed to Import test data", e );
		}

	}
	
	
	// note @PreDestroy - logger is already dead. Use Spring lc instead.
	@EventListener
	public void onApplicationEvent(ContextClosedEvent event) {
		try {
			logger.info( "\n\n Stopping Mongo and removing storage: {}  \n\n", MONGO_TEST_STORAGE_LOCATION );
			mongod.stop();
			mongodExe.stop();
			FileUtils.deleteQuietly( new File( MONGO_TEST_STORAGE_LOCATION ) );
		} catch (Exception e) {
			logger.error( "Errors during shut down", e );
		}
	}

	private void retrieveTestData(String fileName) throws Exception {
		File jsonFile = new File( "./target/" + fileName );
		if ( !jsonFile.exists() ) {
			logger.info( "File {} does not exists in target. Downloading from tools", fileName );
			RestTemplate restTemplate = getGenericRestTemplate();
			String restUrl = "http://csaptools.yourcompany.com/mongo/testdata/" + fileName;
			String eventsJson = restTemplate.getForObject( restUrl, String.class );
			FileUtils.writeStringToFile( jsonFile, eventsJson );
		} else {
			logger.info( "File {} already downloaded, so re using.", fileName );
		}
	}

	private RestTemplate getGenericRestTemplate() {

		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
		factory.setConnectTimeout( 120000 );
		factory.setReadTimeout( 120000 );

		RestTemplate restTemplate = new RestTemplate( factory );
		restTemplate.getMessageConverters().clear();
		restTemplate.getMessageConverters().add( new FormHttpMessageConverter() );
		restTemplate.getMessageConverters().add( new StringHttpMessageConverter() );

		return restTemplate;
	}

	private MongoImportProcess startMongoImport(int port, String dbName,
			String collection, String jsonFile,
			Boolean jsonArray, Boolean upsert, Boolean drop) throws Exception {
		IMongoImportConfig mongoImportConfig = new MongoImportConfigBuilder()
				.version( Version.Main.V3_0 )
				.net( new Net( port, Network.localhostIsIPv6() ) )
				.db( dbName )
				.collection( collection )
				.upsert( upsert )
				.dropCollection( drop )
				.jsonArray( jsonArray )
				.importFile( jsonFile )
				.build();
		MongoImportExecutable mongoImportExecutable = MongoImportStarter.getDefaultInstance()
				.prepare( mongoImportConfig );
		MongoImportProcess mongoImport = mongoImportExecutable.start();
		return mongoImport;
	}

	/**
	 * CSAP is using old authentication scheme
	 */
	private void createUser() {
		MongoDatabase db = mongoClient.getDatabase( "admin" );
		Document authVersionDoc = new Document( "_id", "authSchema" );
		authVersionDoc.append( "currentVersion", 3 );
		mongoClient.getDatabase( "admin" ).getCollection( "system.version" ).insertOne( authVersionDoc );

		Map<String, Object> commandArguments = new BasicDBObject();
		commandArguments.put( "createUser", "dataBaseReadWriteUser" );
		commandArguments.put( "pwd", "password" );
		String[] roles = { "dbAdminAnyDatabase", "readWriteAnyDatabase", "clusterAdmin", "userAdminAnyDatabase" };
		commandArguments.put( "roles", roles );
		BasicDBObject command = new BasicDBObject( commandArguments );
		Document result = db.runCommand( command );

	}

	public int getDateOffSet(String startDate) throws Exception {
		Calendar today = Calendar.getInstance();
		int diffInDays = (int) ((today.getTimeInMillis() - (getStartDate( startDate )).getTime())
				/ (1000 * 60 * 60 * 24));
		logger.debug( "Date offset {} ", diffInDays );
		return diffInDays;
	}

	private Date getStartDate(String dateString) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat( "MM/dd/yyyy" );
		// sdf.setTimeZone(TimeZone.getTimeZone("CST"));
		Date date = sdf.parse( dateString );
		return date;
	}

	public Map<String, String> createStandardUrlParamMap(String startDate, String numDays) throws Exception {
		Map<String, String> urlParams = new HashMap<>();
		urlParams.put( "appId", "csapeng.gen" );
		urlParams.put( "life", "dev" );
		urlParams.put( "project", "CSAP Engineering" );
		urlParams.put( "numDays", numDays );
		urlParams.put( "dateOffSet", "" + getDateOffSet( startDate ) );
		return urlParams;
	}

	public JsonNode getJsonNodeFromArray(JsonNode dataNode, String attributeName, String attributeValue) {
		if ( dataNode.isArray() ) {
			ArrayNode arrayDataNode = (ArrayNode) dataNode;
			for (int i = 0; i < arrayDataNode.size(); i++) {
				JsonNode dataElementNode = arrayDataNode.get( i );
				if ( attributeValue.equals( dataElementNode.at( "/" + attributeName ).asText() ) ) {
					return dataElementNode;
				}
			}
		}
		return jacksonMapper.createObjectNode();

	}

}
