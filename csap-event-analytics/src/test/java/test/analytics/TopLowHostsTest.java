package test.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 * This works on data stored in csap tools. If that data is changed asserts have
 * to changed.
 * 
 *
 */
@WebAppConfiguration
@RunWith ( SpringJUnit4ClassRunner.class )
@SpringBootTest ( classes = { CsapAnalyticsApplication.class, TestSpringConfiguration.class } )
@ActiveProfiles ( "junit" )
public class TopLowHostsTest {

	private static Logger logger = LoggerFactory.getLogger( TopLowHostsTest.class );

	@Autowired
	private WebApplicationContext wac;
	private MockMvc mockMvc;

	@Autowired
	private MongoEmbedded mongoTest;

	@BeforeClass
	public static void setUpBeforeClass ()
			throws Exception {
		TrendApiTest.printTestHeader( "TopLow" );
	}

	@AfterClass
	public static void tearDownAfterClass ()
			throws Exception {
	}

	@Before
	public void setUp ()
			throws Exception {
		this.mockMvc = MockMvcBuilders.webAppContextSetup( this.wac ).build();
	}

	@After
	public void tearDown ()
			throws Exception {
	}

	@Test
	public void verify_top_by_unHealthy_count ()
			throws Exception {
		String responseText = retrieveTopHosts( "1", "health.UnhealthyEventCount", "1", "09/03/2015" );

		assertThat( responseText )
			.as( "Host name does not match" )
			.isEqualTo( "[\"csap-dev02\"]" );
	}

	@Test
	public void verify_top_by_custom_jmx_data ()
			throws Exception {
		String responseText = retrieveTopHosts( "1", "jmxCustom.data.EventPostRate", "1", "09/03/2015" );

		logger.info( "Report Received: \n{} ",
			mongoTest.getJacksonMapper().writerWithDefaultPrettyPrinter().writeValueAsString( responseText ) );

		assertThat( responseText )
			.as( "Host name does not match" )
			.isEqualTo( "[\"csap-dev01\"]" );
	}

	@Test
	public void verify_top_by_jmx_by_service ()
			throws Exception {
		String responseText = retrieveTopHosts( "10", "jmx.HeapUsed_data", "1", "09/03/2015" );

		assertThat( responseText )
			.as( "Host name does not match" )
			.isEqualTo( "[\"csap-dev02\"]" );
	}

	@Test
	public void verify_top_by_jmx_by_heap ()
			throws Exception {
		String responseText = retrieveTopHosts( "10", "jmx.HeapUsed", "2", "09/04/2015" );

		assertThat( responseText )
			.as( "Host name does not match" )
			.isEqualTo( "[\"csap-dev02\",\"csap-dev01\"]" );
	}

	@Test
	public void verify_top_by_process_by_cpu ()
			throws Exception {
		String responseText = retrieveTopHosts( "10", "process.topCpu", "2", "09/12/2015" );

		logger.info( "Report Received: \n{} ",
			mongoTest.getJacksonMapper().writerWithDefaultPrettyPrinter().writeValueAsString( responseText ) );

		assertThat( responseText )
			.as( "Host name does not match" )
			.isEqualTo( "[\"csap-dev01\",\"csapdb-dev02\"]" );
	}

	@Test
	public void verify_top_hosts_by_cores_active ()
			throws Exception {
		String responseText = retrieveTopHosts( "1", "vm.coresActive", "2", "09/03/2015" );

		assertThat( responseText )
			.as( "Host name does not match" )
			.isEqualTo( "[\"csap-dev01\",\"csapdb-dev02\"]" );
	}

	@Test
	public void verify_top_by_process_by_service_name ()
			throws Exception {
		String responseText = retrieveTopHosts( "1", "process.topCpu_mongoDb", "1", "09/03/2015" );

		assertThat( responseText )
			.as( "Host name does not match" )
			.isEqualTo( "[\"csap-dev01\"]" );
	}

	@Test
	public void verify_low_hosts_by_cores_active ()
			throws Exception {
		ResultActions resultActions = mockMvc.perform( post( CsapAnalyticsApplication.REPORT_URL + "/low" )
			.param( "appId", "csapeng.gen" )
			.param( "life", "dev" )
			.param( "project", "CSAP Engineering" )
			.param( "hosts", "2" )
			.param( "numDays", "1" )
			.param( "dateOffSet", "" + mongoTest.getDateOffSet( "09/03/2015" ) )
			.param( "metricsId", "vm.coresActive" )
			.contentType( MediaType.APPLICATION_FORM_URLENCODED )
			.accept( MediaType.APPLICATION_JSON ) );
		String responseText = resultActions.andExpect( status().isOk() )
			.andExpect( content().contentType( MediaType.APPLICATION_JSON ) )
			.andReturn()
			.getResponse()
			.getContentAsString();

		assertThat( responseText )
			.as( "Host name does not match" )
			.isEqualTo( "[\"csapdb-dev01\",\"csap-dev02\"]" );
	}

	private String retrieveTopHosts ( String numDays, String metricsId, String numHosts, String startDate )
			throws Exception {
		ResultActions resultActions = mockMvc.perform( post( CsapAnalyticsApplication.REPORT_URL + "/top" )
			.param( "appId", "csapeng.gen" )
			.param( "life", "dev" )
			.param( "project", "CSAP Engineering" )
			.param( "hosts", numHosts )
			.param( "numDays", numDays )
			.param( "dateOffSet", "" + mongoTest.getDateOffSet( startDate ) )
			.param( "metricsId", metricsId )
			.contentType( MediaType.APPLICATION_FORM_URLENCODED )
			.accept( MediaType.APPLICATION_JSON ) );
		String responseText = resultActions.andExpect( status().isOk() )
			.andExpect( content().contentType( MediaType.APPLICATION_JSON ) )
			.andReturn()
			.getResponse()
			.getContentAsString();
		return responseText;
	}
}
