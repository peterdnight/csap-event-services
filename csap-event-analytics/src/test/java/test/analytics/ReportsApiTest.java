package test.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.csap.analytics.CsapAnalyticsApplication;
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
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

@WebAppConfiguration
@RunWith ( SpringJUnit4ClassRunner.class )
@SpringBootTest ( classes = { CsapAnalyticsApplication.class, TestSpringConfiguration.class } )
@ActiveProfiles("junit")
public class ReportsApiTest  {
	private static Logger logger = LoggerFactory.getLogger(TopLowHostsTest.class);

	@Autowired
	private WebApplicationContext wac;
	private MockMvc mockMvc;

	@Autowired
	private MongoEmbedded mongoTest; 
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TrendApiTest.printTestHeader("ReportsApiTest");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void verify_vm_report_for_single_day() throws Exception {
		String vmReport = retrieveAnalyticsReport(mongoTest.createStandardUrlParamMap("09/04/2015", "1"), "/api/report/vm");
		JsonNode vmReportNode = mongoTest.getJacksonMapper().readTree(vmReport);
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(vmReportNode.at("/data"), "hostName", "csap-dev02");

		assertThat(dataElement.at("/socketTotal").asLong())
				.as("Socket count does not match")
				.isEqualTo(192947);

		assertThat(dataElement.at("/totalLoad").asDouble())
				.as("Total Load does not match")
				.isEqualTo(297.93999999999994);

		assertThat(dataElement.at("/totalUsrCpu").asLong())
				.as("Total usr cpu does not match")
				.isEqualTo(1172);

	}
 
	@Test
	public void verify_vm_report_multi_day() throws Exception {
		String vmReport = retrieveAnalyticsReport(mongoTest.createStandardUrlParamMap("09/05/2015", "2"), "/api/report/vm");
		JsonNode vmReportNode = mongoTest.getJacksonMapper().readTree(vmReport);
		
		

		logger.info("Report Received: \n{} " ,  mongoTest.getJacksonMapper().writerWithDefaultPrettyPrinter().writeValueAsString( vmReportNode) );
		
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(vmReportNode.at("/data"), "hostName", "csap-dev02");
		
		assertThat(dataElement.at("/socketTotal").asLong())
				.as("Socket count does not match")
				.isEqualTo(387440);
		
		
		assertThat(dataElement.at("/totalLoad").asDouble())
				.as("Total Load does not match")
				.isEqualTo(662.55);
		
		
		assertThat(dataElement.at("/totalUsrCpu").asLong())
				.as("Total usr cpu does not match")
				.isEqualTo(3161);

	}

	@Test
	public void verify_service_report_single_day() throws Exception {
		String serviceReport = retrieveAnalyticsReport(mongoTest.createStandardUrlParamMap("09/04/2015", "1"),
				"/api/report/service");
		JsonNode serviceReportNode = mongoTest.getJacksonMapper().readTree(serviceReport);
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(serviceReportNode.at("/data"), "serviceName", "CsAgent");
		
		
		assertThat(dataElement.at("/rssMemory").asLong())
				.as("Rss memory does not match")
				.isEqualTo(5695681);
		
		
		assertThat(dataElement.at("/fileCount").asLong())
				.as("File count does not match")
				.isEqualTo(3001637);

	}

	@Test
	public void verify_service_report_multi_day() throws Exception {
		String serviceReport = retrieveAnalyticsReport(mongoTest.createStandardUrlParamMap("09/05/2015", "2"),
				"/api/report/service");
		JsonNode serviceReportNode = mongoTest.getJacksonMapper().readTree(serviceReport);
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(serviceReportNode.at("/data"), "serviceName", "CsAgent");
		
		
		assertThat(dataElement.at("/rssMemory").asLong())
				.as("Rss memory does not match")
				.isEqualTo(11495974);
		
		
		assertThat(dataElement.at("/fileCount").asLong())
				.as("File count does not match")
				.isEqualTo(6005866);

	}

	@Test
	public void verify_service_report_with_host_and_service_filter_single_day() throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap("09/04/2015", "1");
		urlParams.put("host", "csap-dev01");
		urlParams.put("serviceName", "mongoDb");
		String serviceReport = retrieveAnalyticsReport(urlParams, "/api/report/service");
		JsonNode serviceReportNode = mongoTest.getJacksonMapper().readTree(serviceReport);

		JsonNode dataElement = mongoTest.getJsonNodeFromArray(serviceReportNode.at("/data"), "serviceName", "mongoDb");
		
		logger.debug("service report node {} ", dataElement);
		
		
		assertThat(dataElement.at("/rssMemory").asLong())
				.as("Rss memory does not match")
				.isEqualTo(11392477);
		
		
		assertThat(dataElement.at("/fileCount").asLong())
				.as("File count does not match")
				.isEqualTo(239552);

	}

	@Test
	public void verify_service_details_report_multi_day() throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap("09/05/2015", "2");
		urlParams.put("serviceName", "mongoDb");
		String serviceReport = retrieveAnalyticsReport(urlParams, "/api/report/service/detail");
		JsonNode serviceReportNode = mongoTest.getJacksonMapper().readTree(serviceReport);
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(serviceReportNode.at("/data"), "serviceName", "mongoDb");
		logger.debug("service report node {} ", dataElement);
		
		
		assertThat(dataElement.at("/diskReadKb").asLong())
				.as("Disk read kb does not match")
				.isEqualTo(5575);
		
		
		assertThat(dataElement.at("/numberOfSamples").asLong())
				.as("Number of samples does not match")
				.isEqualTo(5760);

	}

	@Test
	public void verify_jmx_report_multi_day() throws Exception {
		String jmxReport = retrieveAnalyticsReport(mongoTest.createStandardUrlParamMap("09/05/2015", "2"), "/api/report/jmx");
		JsonNode jmxReportNode = mongoTest.getJacksonMapper().readTree(jmxReport);
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(jmxReportNode.at("/data"), "serviceName", "CsAgent");
		logger.debug("data element {} ", dataElement);
		
		
		assertThat(dataElement.at("/httpProcessingTime").asLong())
				.as("httpProcessingTime does not match")
				.isEqualTo(3114283);
		
		
		assertThat(dataElement.at("/sessionsActive").asLong())
				.as("sessionsActive does not match")
				.isEqualTo(39066);

	}

	@Test
	public void verify_jmx_details_report_multi_day() throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap("09/05/2015", "2");
		urlParams.put("serviceName", "data");
		String jmxDetailsReport = retrieveAnalyticsReport(urlParams, "/api/report/jmx/detail");
		JsonNode jmxDetailsReportNode = mongoTest.getJacksonMapper().readTree(jmxDetailsReport);
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(jmxDetailsReportNode.at("/data"), "serviceName", "data");
		logger.debug("data element {} ", dataElement);
		
		
		assertThat(dataElement.at("/httpKbytesReceived").asLong())
				.as("httpKbytesReceived does not match")
				.isEqualTo(1567541);
		
		
		assertThat(dataElement.at("/httpRequestCount").asLong())
				.as("httpRequestCount does not match")
				.isEqualTo(361173);

	}

	@Test
	public void validate_http_request_multi_service_multi_day() throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap("09/05/2015", "2");
		urlParams.put("serviceName", "data");
		// urlParams.put("serviceName", "data"); // need to convert to multvaluemap
		String jmxDetailsReport = retrieveAnalyticsReport(urlParams, "/api/report/jmx/detail");
		JsonNode jmxDetailsReportNode = mongoTest.getJacksonMapper().readTree(jmxDetailsReport);
		

		logger.info("Report Received: \n{} " ,  mongoTest.getJacksonMapper().writerWithDefaultPrettyPrinter().writeValueAsString( jmxDetailsReportNode) );
		
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(jmxDetailsReportNode.at("/data"), "serviceName", "data");


		
		
		assertThat(dataElement.at("/httpRequestCount").asLong())
				.as("httpRequestCount does not match")
				.isEqualTo(361173);

	}

	@Test
	public void verify_custom_jmx_report_multi_day() throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap("09/05/2015", "2");
		urlParams.put("serviceName", "data");
		String customJmxReport = retrieveAnalyticsReport(urlParams, "/api/report/jmxCustom");
		JsonNode customJmxReportNode = mongoTest.getJacksonMapper().readTree(customJmxReport);
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(customJmxReportNode.at("/data"), "serviceName", "data");
		logger.debug("data element {} ", dataElement);
		
		
		assertThat(dataElement.at("/EventDbDataMb").asLong())
				.as("EventDbDataMb does not match")
				.isEqualTo(46817100);
		
		
		assertThat(dataElement.at("/EventPostRate").asLong())
				.as("EventPostRate does not match")
				.isEqualTo(666820);
	}

	@Test
	public void verify_custom_jmx_details_report_multi_day() throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap("09/05/2015", "2");
		urlParams.put("serviceName", "data");
		String jmxDetailsReport = retrieveAnalyticsReport(urlParams, "/api/report/jmxCustom/detail");
		JsonNode jmxDetailsReportNode = mongoTest.getJacksonMapper().readTree(jmxDetailsReport);
		JsonNode dataElement = mongoTest.getJsonNodeFromArray(jmxDetailsReportNode.at("/data"), "serviceName", "data");
		logger.debug("data element {} ", dataElement);
		
		
		assertThat(dataElement.at("/SpringMvcRequests").asLong())
				.as("SpringMvcRequests does not match")
				.isEqualTo(361171);
		
		
		assertThat(dataElement.at("/EventRecordsInK").asLong())
				.as("EventRecordsInK does not match")
				.isEqualTo(18066914444L);
	}

	@Test
	public void verify_service_attributes() throws Exception {

		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap("09/05/2015", "2");
		urlParams.put("serviceName", "data");
		String attributesResponse = retrieveAnalyticsReport(urlParams, "/api/report/attributes");
		JsonNode attributesNode = mongoTest.getJacksonMapper().readTree(attributesResponse);
		ArrayNode attributes = (ArrayNode) attributesNode;
		String expectedResponse = "[\"EventDbDataMb\",\"EventPostRate\",\"EventsPerSecond\",\"EventRecordsInK\",\"EventSecurityErrors\",\"EventPostMeanMs\",\"SpringMvcRequests\",\"MetricsRecordsInK\",\"EventGetMaxMs\",\"EventSecurity\",\"EventFilteredCount\",\"EventGetMeanMs\",\"EventDbStorageMb\",\"jmxHeartbeatMs\",\"MetricsEventPostMeanMs\",\"numberOfSamples\",\"EventDbIndexMb\",\"serviceName\",\"EventPostMaxMs\",\"EventGetCount\",\"MetricsDbStorageMb\",\"MetricsDbDataMb\",\"MetricsEventPostMaxMs\",\"MetricsEventPostRate\"]";
		JsonNode expectedAttributesNode = mongoTest.getJacksonMapper().readTree(expectedResponse);
		ArrayNode expectedAttributes = (ArrayNode) expectedAttributesNode;
		List<String> expectedAttributesList = new ArrayList<>();
		for (JsonNode attr : expectedAttributes) {
			expectedAttributesList.add(attr.asText());
		}
		logger.debug("expectedAttributesList {} ", expectedAttributesList);
		attributes.forEach(attribNode -> {
			
			assertThat(expectedAttributesList.contains(attribNode.asText()))
					.as("Missing attribute" + attribNode.asText())
					.isTrue();
			
		});
	}

	private String retrieveAnalyticsReport(Map<String, String> urlParams, String url) throws Exception {
		MockHttpServletRequestBuilder requestBuilder = post(url);
		for (String key : urlParams.keySet()) {
			requestBuilder.param(key, urlParams.get(key));
		}
		requestBuilder.contentType(MediaType.APPLICATION_FORM_URLENCODED);
		requestBuilder.accept(MediaType.APPLICATION_JSON);
		ResultActions resultActions = mockMvc.perform(requestBuilder);
		String responseText = resultActions.andExpect(status().isOk())
				.andExpect(content().contentType(MediaType.APPLICATION_JSON))
				.andReturn()
				.getResponse()
				.getContentAsString();
		return responseText;
	}

}
