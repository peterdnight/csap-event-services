package scenario_3.data_rest_api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.csap.CsapDataApplication;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongoImportProcess;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.DownloadConfigBuilder;
import de.flapdoodle.embed.mongo.config.ExtractedArtifactStoreBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import de.flapdoodle.embed.process.runtime.Network;

@WebAppConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = CsapDataApplication.class)
@ActiveProfiles("junit")
public class EventApiTest {

	private static Logger logger = LoggerFactory.getLogger(EventApiTest.class);
	private ObjectMapper jacksonMapper = new ObjectMapper();
	private String userId = "csapeng.gen";
	private String pass = "csaprest123!";
	
	private static  MongodStarter starter = null ;//MongodStarter.getDefaultInstance();

    private static MongodExecutable mongodExe;
    private static MongodProcess mongod;
    private static MongoClient mongoClient;
    private static MongoImportProcess mongoImport;


	@Autowired
	private WebApplicationContext wac;
	private MockMvc mockMvc;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		logger.info("Starting Mongo server");
		Command command = Command.MongoD;

		IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
				.defaultsWithLogger(Command.MongoD, logger)
				.artifactStore(new ExtractedArtifactStoreBuilder()
						.defaults(command)
						.extractDir(new FixedPath("./target"))
//						.download(new DownloadConfigBuilder()
//								.defaultsForCommand(command)
//								.artifactStorePath(new FixedPath("./target"))
//								.downloadPath("http://csaptools.youcompany.com/mongo/"))
				)
				.build();
		logger.debug("after run time config");

		starter = MongodStarter.getInstance(runtimeConfig);
		logger.debug("after getting starter");
		Storage storage = new Storage("./target/storage", null, 0);
		mongodExe = starter.prepare(new MongodConfigBuilder()
				.version(Version.V3_0_6)
				.net(new Net(12345, Network.localhostIsIPv6()))
				.replication(storage)
				.build()
				);
		logger.debug("after mongod exe");
		String path = mongodExe.getFile().executable().toString();
		logger.info("Mongo extracted {} ", path);
		mongod = mongodExe.start();
		mongoClient = new MongoClient("localhost", 12345);
		createUser();
		
		logger.info("Done with setup before class ###############################################");
		
		//create schema using java driver
		//use installed mongo to dump data and import  using java run time exec 
	}
	
	/**
	 * CSAP is using old authentication scheme
	 */
	private static void createUser(){
    	MongoDatabase db =  mongoClient.getDatabase("admin");
    	Document authVersionDoc = new Document("_id","authSchema");
    	authVersionDoc.append("currentVersion", 3);
    	mongoClient.getDatabase("admin").getCollection("system.version").insertOne(authVersionDoc);
    	
        Map<String, Object> commandArguments = new BasicDBObject();
        commandArguments.put("createUser", "dataBaseReadWriteUser");
        commandArguments.put("pwd", "password");
        String[] roles = { "dbAdminAnyDatabase","readWriteAnyDatabase","clusterAdmin","userAdminAnyDatabase" };
        commandArguments.put("roles", roles);
        BasicDBObject command = new BasicDBObject(commandArguments);
        Document result = db.runCommand(command);
                   
    }

	@AfterClass
	public static void tearDownAfterClass() throws Exception {		
		mongod.stop();
        mongodExe.stop();
        FileUtils.deleteQuietly(new File("./target/storage"));
	}

	Set<String> insertedObjIds = new HashSet<>();

	@Before
	public void setUp() throws Exception {	
		logger.info("In setup method");
		this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
		logger.info("Done setup method");
	}

	@After
	public void tearDown() throws Exception {
		insertedObjIds.forEach(objId -> deleteEventById(objId));
		
	}
	
	@Test
	public void verify_delete_by_query() throws Exception {
		String searchQuery = "lifecycle=dev,appId=csapeng.gen,metaData.uiUser=paranant,eventReceivedOn=false,isDataRequired=false";
		String fileName = "ui_event.json";
		insertEvent(fileName);
		insertEvent(fileName);
		insertEvent(fileName);
		fileName = "global_report.json";
		insertEvent(fileName);
		
		try {
			ResultActions resultActions = mockMvc.perform(post(CsapDataApplication.API_URL + "/deleteBySearch")
					.param("searchString", searchQuery).param("userid", "csapeng.gen").param("pass", "csaprest123!")
					.contentType(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON));
			String responseText = resultActions.andExpect(status().isOk())
					.andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn().getResponse()
					.getContentAsString();
			JsonNode jsonResults = jacksonMapper.readTree(responseText);
			
			assertEquals("Could not delete the record", "Deleted 3 record ", jsonResults.at("/result").asText());

		} catch (Exception e) {
			logger.error("Exception ", e);
		}

		
	}

	@Test
	public void verify_Metrics() throws Exception {
		String fileName = "metrics.json";
		insertEvent(fileName);
	}
	
	
	@Test
	public void verify_global_host_report() throws Exception {
		String fileName = "global_report.json";
		logger.info("Starting test");
		insertEvent(fileName);
		String fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		JsonNode insertedEvent = jacksonMapper.readTree(fileContent);
		JsonNode retrievedEvent = getLatestEvent(insertedEvent.at("/category").asText(), userId,
				insertedEvent.at("/lifecycle").asText());

		assertThat(insertedEvent.at("/category").asText())
				.as("Category does not match")
				.isEqualTo(retrievedEvent.at("/category").asText());
		assertThat(retrievedEvent.has("expiresAt"))
				.as("Expires does not exists in created event")
				.isTrue();
		assertThat(retrievedEvent.at("/expiresAt").asLong())
				.as("Expires time is not greater than current time")
				.isGreaterThan(System.currentTimeMillis());

		assertThat(retrievedEvent.at("/createdOn/mongoDate").isMissingNode())
				.as("Created event does not have mongo date")
				.isFalse();
		assertThat(retrievedEvent.at("/createdOn/lastUpdatedOn").isMissingNode())
				.as("Created event does not have last updated on")
				.isFalse();
		assertThat(retrievedEvent.at("/counter").asInt())
				.as("Counter does not match")
				.isEqualTo(1);

		fileName = "global_report_diff_host.json";
		insertEvent(fileName);
		fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		JsonNode updatedEventFromFile = jacksonMapper.readTree(fileContent);
		JsonNode updatedEventFromDb = getLatestEvent(updatedEventFromFile.at("/category").asText(), userId,
				updatedEventFromFile.at("/lifecycle").asText());
		JsonNode countEvents = countEvents(insertedEvent.at("/category").asText(), userId,
				insertedEvent.at("/lifecycle").asText());

		assertThat(countEvents.at("/count").asInt())
				.as("Number of events does not match")
				.isEqualTo(1);

		assertThat(updatedEventFromFile.at("/host").asText())
				.as("Host does not match")
				.isEqualTo(updatedEventFromDb.at("/host").asText());
		assertThat(updatedEventFromDb.at("/counter").asInt())
				.as("Counter does not match")
				.isEqualTo(2);
		assertThat(updatedEventFromFile.at("/data"))
				.as("Data does not match")
				.isEqualTo(updatedEventFromDb.at("/data"));

	}

	@Test
	public void verify_host_report() throws Exception {
		String fileName = "host_report.json";
		insertEvent(fileName);
		String fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		JsonNode insertedEvent = jacksonMapper.readTree(fileContent);
		JsonNode retrievedEvent = getLatestEvent(insertedEvent.at("/category").asText(), userId,
				insertedEvent.at("/lifecycle").asText());

		assertThat(insertedEvent.at("/category").asText())
				.as("Category does not match")
				.isEqualTo(retrievedEvent.at("/category").asText());
		assertThat(retrievedEvent.has("expiresAt"))
				.as("Expires does not exists in created event")
				.isTrue();
		assertThat(retrievedEvent.at("/expiresAt").asLong())
				.as("Expires time is not greater than current time")
				.isGreaterThan(System.currentTimeMillis());

		assertThat(retrievedEvent.at("/createdOn/mongoDate").isMissingNode())
				.as("Created event does not have mongo date")
				.isFalse();
		assertThat(retrievedEvent.at("/createdOn/lastUpdatedOn").isMissingNode())
				.as("Created event does not have last updated on")
				.isFalse();
		assertThat(retrievedEvent.at("/counter").asInt())
				.as("Counter does not match")
				.isEqualTo(1);

		fileName = "updated_host_report.json";
		insertEvent(fileName);
		fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		JsonNode updatedEventFromFile = jacksonMapper.readTree(fileContent);
		JsonNode updatedEventFromDb = getLatestEvent(updatedEventFromFile.at("/category").asText(), userId,
				updatedEventFromFile.at("/lifecycle").asText());

		assertThat(updatedEventFromDb.at("/counter").asInt())
				.as("Counter does not match")
				.isEqualTo(2);
		assertThat(updatedEventFromFile.at("/data"))
				.as("Data does not match")
				.isEqualTo(updatedEventFromDb.at("/data"));

	}

	@Test
	public void verify_memory_low_event() throws Exception {
		String fileName = "memory_low.json";
		insertEvent(fileName);
		String fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		JsonNode insertedEvent = jacksonMapper.readTree(fileContent);
		JsonNode retrievedEvent = getLatestEvent(insertedEvent.at("/category").asText(), userId,
				insertedEvent.at("/lifecycle").asText());

		assertThat(insertedEvent.at("/category").asText())
				.as("Category does not match")
				.isEqualTo(retrievedEvent.at("/category").asText());
		assertThat(retrievedEvent.has("expiresAt"))
				.as("Expires does not exists in created event")
				.isTrue();
		assertThat(retrievedEvent.at("/expiresAt").asLong())
				.as("Expires time is not greater than current time")
				.isGreaterThan(System.currentTimeMillis());
		/*
		 * assertThat(retrievedEvent.at("/expiresAt").asText())
		 * .as("Expires at for memory low event does not exists")
		 * .contains("ananth");
		 */

		assertThat(retrievedEvent.at("/createdOn/mongoDate").isMissingNode())
				.as("Created event does not have mongo date")
				.isFalse();
		assertThat(retrievedEvent.at("/createdOn/lastUpdatedOn").isMissingNode())
				.as("Created event does not have last updated on")
				.isFalse();
		assertThat(retrievedEvent.at("/counter").asInt())
				.as("Counter does not match")
				.isEqualTo(1);

		fileName = "updated_memory_low.json";
		insertEvent(fileName);
		fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		JsonNode updatedEventFromFile = jacksonMapper.readTree(fileContent);
		JsonNode updatedEventFromDb = getLatestEvent(updatedEventFromFile.at("/category").asText(), userId,
				updatedEventFromFile.at("/lifecycle").asText());

		assertThat(updatedEventFromDb.at("/counter").asInt())
				.as("Counter does not match")
				.isEqualTo(2);
		assertThat(updatedEventFromFile.at("/data"))
				.as("Data does not match")
				.isEqualTo(updatedEventFromDb.at("/data"));
	}

	@Test
	public void verify_ui_event() throws Exception {
		String fileName = "ui_event.json";
		insertEvent(fileName);
		String fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		JsonNode insertedEvent = jacksonMapper.readTree(fileContent);
		JsonNode retrievedEvent = getLatestEvent(insertedEvent.at("/category").asText(), userId,
				insertedEvent.at("/lifecycle").asText());

		assertThat(insertedEvent.at("/category").asText())
				.as("Category does not match")
				.isEqualTo(retrievedEvent.at("/category").asText());
		assertThat(retrievedEvent.has("expiresAt"))
				.as("Expires does not exists in created event")
				.isTrue();
		assertThat(retrievedEvent.at("/expiresAt").asLong())
				.as("Expires time is not greater than current time")
				.isGreaterThan(System.currentTimeMillis());
		assertThat(insertedEvent.at("/metaData"))
				.as("Data does not match")
				.isEqualTo(retrievedEvent.at("/metaData"));
		assertThat(retrievedEvent.at("/createdOn/mongoDate").isMissingNode())
				.as("Created event does not have mongo date")
				.isFalse();
		assertThat(retrievedEvent.at("/createdOn/lastUpdatedOn").isMissingNode())
				.as("Created event does not have last updated on")
				.isFalse();
		
		Thread.sleep(60000);

	}

	@Test
	public void verify_settings() throws Exception {
		String fileName = "event_settings.json";
		insertEvent(fileName);
		String fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		JsonNode insertedEvent = jacksonMapper.readTree(fileContent);
		JsonNode retrievedEvent = getLatestEvent(insertedEvent.at("/category").asText(), userId,
				insertedEvent.at("/lifecycle").asText());
		assertThat(insertedEvent.at("/category").asText())
				.as("Category does not match")
				.isEqualTo(retrievedEvent.at("/category").asText());
		assertThat(retrievedEvent.has("expiresAt"))
				.as("Expires exists in created event")
				.isFalse();

		assertThat(retrievedEvent.at("/createdOn/mongoDate").isMissingNode())
				.as("Created event does not have mongo date")
				.isFalse();
		assertThat(retrievedEvent.at("/createdOn/lastUpdatedOn").isMissingNode())
				.as("Created event does not have last updated on")
				.isFalse();

	}
	
	@Test
	public void verify_insert_event() throws Exception {
		String fileName = "analytics_settings.json";
		String fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		ResultActions resultActions = mockMvc.perform(post(CsapDataApplication.API_URL + "/insert").param("eventJson", fileContent)
				.param("userid", "csapeng.gen").param("pass", "csaprest123!")
				.param("appId", "csapeng.gen").param("life", "dev").param("project", "Test Project").param("summary", "Test insert").param("category", "/csap/settings/uisettings")
				.contentType(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON));
		String responseText = resultActions.andExpect(status().isOk())
				// .andExpect(content().contentType(MediaType.APPLICATION_JSON))
				.andReturn()
				.getResponse()
				.getContentAsString();
		
		String searchQuery = "lifecycle=dev,appId=csapeng.gen,simpleSearchText=/csap/settings/uisettings,eventReceivedOn=false,isDataRequired=false";		
		
		try {
			resultActions = mockMvc.perform(post(CsapDataApplication.API_URL + "/deleteBySearch")
					.param("searchString", searchQuery).param("userid", "csapeng.gen").param("pass", "csaprest123!")
					.contentType(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON));
			responseText = resultActions.andExpect(status().isOk())
					.andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn().getResponse()
					.getContentAsString();
			JsonNode jsonResults = jacksonMapper.readTree(responseText);			
			assertEquals("Could not delete the record", "Deleted 1 record ", jsonResults.at("/result").asText());

		} catch (Exception e) {
			logger.error("Exception ", e);
		}
		
		
	}

	private JsonNode getLatestEvent(String category, String appId, String life) throws Exception {
		ResultActions resultActions = mockMvc.perform(post(CsapDataApplication.API_URL + "/latest").param("category", category)
				.param("appId", appId).param("life", life).accept(MediaType.APPLICATION_JSON));

		String responseText = resultActions.andExpect(status().isOk())
				.andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn().getResponse()
				.getContentAsString();

		JsonNode latestEvent = jacksonMapper.readTree(responseText);
		return latestEvent;

	}

	private JsonNode countEvents(String category, String appId, String life) throws Exception {
		ResultActions resultActions = mockMvc.perform(post(CsapDataApplication.API_URL + "/count").param("category", category)
				.param("appId", appId).param("life", life).param("days", "-1").accept(MediaType.APPLICATION_JSON));

		String responseText = resultActions.andExpect(status().isOk())
				.andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn().getResponse()
				.getContentAsString();

		JsonNode latestEvent = jacksonMapper.readTree(responseText);
		return latestEvent;

	}

	private String insertEvent(String fileName) throws Exception {
		String fileContent = new String(Files.readAllBytes(Paths.get(EventApiTest.class.getResource(fileName).toURI())));
		ResultActions resultActions = mockMvc.perform(post(CsapDataApplication.EVENT_API).param("eventJson", fileContent)
				.param("userid", "csapeng.gen").param("pass", "csaprest123!")
				.contentType(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON));
		String responseText = resultActions.andExpect(status().isOk())
				// .andExpect(content().contentType(MediaType.APPLICATION_JSON))
				.andReturn()
				.getResponse()
				.getContentAsString();
		logger.debug("Object id{}", responseText);
		if (StringUtils.isBlank(responseText) || "Success".equalsIgnoreCase(responseText)
				|| "Failure".equalsIgnoreCase(responseText)) {
			logger.error("Did not find object id. Deletion will not happen");
		} else {
			insertedObjIds.add(responseText);
		}
		return responseText;

	}

	private void deleteEventById(String objectId) {
		try {
			ResultActions resultActions = mockMvc.perform(post(CsapDataApplication.API_URL + "/delete")
					.param("objectId", objectId).param("userid", "csapeng.gen").param("pass", "csaprest123!")
					.contentType(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON));
			String responseText = resultActions.andExpect(status().isOk())
					.andExpect(content().contentType(MediaType.APPLICATION_JSON)).andReturn().getResponse()
					.getContentAsString();
			JsonNode jsonResults = jacksonMapper.readTree(responseText);
			logger.info("Delete result for object id {} is {} ", objectId, jsonResults.at("/result").asText());
			//assertEquals("Could not delete the record", "Deleted 1 record ", jsonResults.at("/result").asText());

		} catch (Exception e) {
			logger.error("Exception ", e);
		}

	}
}
