package test.scenario_3_performance;

import static org.assertj.core.api.Assertions.assertThat;

import javax.inject.Inject;

import org.csap.analytics.CsapAnalyticsApplication;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import test.scenario_1_container.Boot_Container;

@RunWith ( SpringJUnit4ClassRunner.class )
@SpringBootTest ( classes = { CsapAnalyticsApplication.class } )
//@WebIntegrationTest
@ActiveProfiles ( "junit" )
public class Performance_Console {
	final static private Logger logger = LoggerFactory.getLogger( Performance_Console.class );

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
		logger.info( "Hitting landing page so that simon has results" );
		//ResponseEntity<String> response = restTemplate.getForEntity( "http://localhost:8080/", String.class );
	}

	@After
	public void tearDown ()
			throws Exception {
	}

	ObjectMapper jacksonMapper = new ObjectMapper();


	@Inject
	RestTemplateBuilder restTemplateBuilder;

	@Test
	public void validate_csap_health_using_rest_template() throws Exception {
		logger.info( Boot_Container.TC_HEAD + "simple mvc test" );
		// mock does much validation.....

		TestRestTemplate restTemplate = new TestRestTemplate( restTemplateBuilder );
		
		ResponseEntity<String> response = restTemplate.getForEntity( "http://localhost:8080/csap/health", String.class ) ;
		
		logger.info( "result:\n" + response );

		assertThat( response.getBody() )
				.contains( "<table id=\"metricTable\" class=\"simple\">") ;
	}
}
