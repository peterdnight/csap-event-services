package test.scenario_2_locator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import test.scenario_1_container.Boot_Container;

@RunWith ( SpringJUnit4ClassRunner.class )
@WebAppConfiguration
@SpringBootTest ( classes = { CsapAnalyticsApplication.class } )
@ActiveProfiles ( "junit" )
public class Spring_Http_Endpoints {
	final static private Logger logger = LoggerFactory.getLogger( Spring_Http_Endpoints.class );

	@Autowired
	private WebApplicationContext wac;

	private MockMvc mockMvc;

	@BeforeClass
	public static void setUpBeforeClass ()
			throws Exception {

		Boot_Container.printTestHeader( logger.getName() );

	}

	@AfterClass
	public static void tearDownAfterClass ()
			throws Exception {
		// db.shutdown();
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

	ObjectMapper jacksonMapper = new ObjectMapper();

	@Test
	public void http_get_landing_page ()
			throws Exception {
		logger.info( Boot_Container.TC_HEAD + "simple mvc test" );
		// mock does much validation.....
		ResultActions resultActions = mockMvc.perform(
			get( "/" )
				.param( "sampleParam1", "sampleValue1" )
				.param( "sampleParam2", "sampleValue2" )
				.accept( MediaType.TEXT_PLAIN ) );

		//
		String result = resultActions
			.andExpect( status().isOk() )
			.andExpect( content().contentType( "text/html;charset=UTF-8" ) )
			.andReturn().getResponse().getContentAsString();
		logger.info( "result:\n" + result );

		assertThat( result )
			.contains( "Analytics Service" );

	}

}
