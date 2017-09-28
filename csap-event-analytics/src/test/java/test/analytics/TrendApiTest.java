package test.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.text.WordUtils;
import org.csap.analytics.CsapAnalyticsApplication;
import org.csap.analytics.db.TrendingReportHelper;
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
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@WebAppConfiguration
@RunWith ( SpringJUnit4ClassRunner.class )
@SpringBootTest ( classes = { CsapAnalyticsApplication.class, TestSpringConfiguration.class } )
@ActiveProfiles ( "junit" )
public class TrendApiTest {
	private static Logger logger = LoggerFactory.getLogger( TopLowHostsTest.class );

	public static String TC_HEAD = "\n\n ========================= UNIT TEST =========================== \n\n";

	private static boolean isJvmInfoPrinted = false;

	public static void printTestHeader ( String description ) {

		if ( !isJvmInfoPrinted ) {
			isJvmInfoPrinted = true;
			System.out.println( "Working Directory = "
					+ System.getProperty( "user.dir" ) );
			StringBuffer sbuf = new StringBuffer();
			// Dump log4j first - if it does not work, nothing will
			String resource = "log4j2-junit.yml";
			// URL configFile = ClassLoader.getSystemResource( resource );
			FileSystemResource fr = new FileSystemResource( resource );
			try {
				URI configFile = fr.getURI();
				sbuf.append( "\n\n ** " + resource + " found in: " + configFile.getPath() );
			} catch (Exception e) {
				logger.error( "Failed to find: {} ", fr, e );
			}

			// Now dump nicely formatted classpath.
			if ( false ) {
				sbuf.append( "\n\n ====== JVM Classpath is: \n"
						+ WordUtils.wrap( System.getProperty( "java.class.path" ).replaceAll( ";", " " ), 140 ) );
			} else {
				sbuf.append( "\n\n ====== JVM Classpath is: notShown \n" );
			}
			System.out.println( sbuf );
		}

		logger.info( "\n\n *********************     " + description + "   ***********************\n\n" );
	}

	@Autowired
	private WebApplicationContext wac;
	private MockMvc mockMvc;

	@Autowired
	private MongoEmbedded mongoTest;

	@BeforeClass
	public static void setUpBeforeClass ()
			throws Exception {
		printTestHeader( "log4j initialized" );
		// mongoTest.setUpMongo();
	}

	@AfterClass
	public static void tearDownAfterClass ()
			throws Exception {
		// AbstractMongoDbTest.tearDownMongo();
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

	private ObjectMapper jacksonMapper = new ObjectMapper();

	public ObjectMapper getJacksonMapper () {
		return jacksonMapper;
	}

	@Test
	public void verify_log_rotate_report ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/04/2015", "1" );
		urlParams.put( "trending", "true" );
		urlParams.put( "metricsId", "MeanSeconds" );
		String logRotateReport = retrieveAnalyticsReport( urlParams, "/api/report/custom/logRotate" );
		JsonNode logRotateReportNode = getJacksonMapper().readTree( logRotateReport );

		logger.info( "Report Received: \n{} ",
			jacksonMapper.writerWithDefaultPrettyPrinter().writeValueAsString( logRotateReportNode ) );

		String expectedReport = "[{\"date\":[\"2015-09-03\"],\"MeanSeconds\":[4],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( logRotateReportNode.at( "/data" ) )
			.as( "Log rotate report does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_log_rotate_report_Filtered_By_Service_Name ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		urlParams.put( "metricsId", "MeanSeconds" );
		urlParams.put( "serviceName", "CsAgent" );
		String logRotateReport = retrieveAnalyticsReport( urlParams, "/api/report/custom/logRotate" );
		JsonNode logRotateReportNode = getJacksonMapper().readTree( logRotateReport );
		logger.debug( "$$$$$$$ {} ", logRotateReport );
		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"MeanSeconds\":[0,0,0,0,0],\"serviceName\":\"CsAgent\",\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( logRotateReportNode.at( "/data" ) )
			.as( "Log rotate report does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_userid_report ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/11/2015", "8" );
		urlParams.put( "trending", "true" );
		String useridReport = retrieveAnalyticsReport( urlParams, "/api/report/userid" );
		JsonNode userIdReportNode = getJacksonMapper().readTree( useridReport );

		logger.info( "Report Received: \n{} ",
			jacksonMapper.writerWithDefaultPrettyPrinter().writeValueAsString( userIdReportNode ) );

		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-08\",\"2015-09-09\",\"2015-09-10\"],\"totActivity\":[2542,2886,32,128,137],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( userIdReportNode.at( "/data" ) )
			.as( "userid report does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_vm_core_report ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		String vmCoreReport = retrieveAnalyticsReport( urlParams, "/api/report/custom/core" );
		JsonNode vmCoreReportNode = getJacksonMapper().readTree( vmCoreReport );
		logger.debug( "$$$$$$$ {} ", vmCoreReport );
		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalSysCpu\":[2685,3218,1720,1712,1641],\"totalUsrCpu\":[5655,6982,1972,1939,1767],\"cpuCountAvg\":[28,28,28,28,28],\"coresUsed\":[0.19537500000000002,0.24762782420303311,0.06261111111111112,0.06209722222222222,0.058375],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( vmCoreReportNode.at( "/data" ) )
			.as( "vm core report does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_vm_core_per_vm_report ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		urlParams.put( "perVm", "true" );
		String vmCoreReportPerVm = retrieveAnalyticsReport( urlParams, "/api/report/custom/core" );
		JsonNode vmCoreReportPerVmNode = getJacksonMapper().readTree( vmCoreReportPerVm );
		logger.debug( "$$$$$$$ {} ", vmCoreReportPerVm );
		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalSysCpu\":[1631,1632,1704,1686,1624],\"totalUsrCpu\":[982,966,1172,1145,989],\"cpuCountAvg\":[4,4,4,4,4],\"coresUsed\":[0.036291666666666667,0.036083333333333335,0.03994444444444444,0.03931944444444444,0.036291666666666667],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csapdb-dev02\"},{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalSysCpu\":[639,886,1,3,0],\"totalUsrCpu\":[3470,3997,163,143,134],\"cpuCountAvg\":[8,8,8,8,8],\"coresUsed\":[0.1141388888888889,0.136016713091922,0.004555555555555556,0.004055555555555555,0.0037222222222222223],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csap-dev01\"},{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalSysCpu\":[10,9,12,19,11],\"totalUsrCpu\":[31,30,29,30,18],\"cpuCountAvg\":[8,8,8,8,8],\"coresUsed\":[0.001138888888888889,0.0010833333333333333,0.001138888888888889,0.001361111111111111,8.055555555555556E-4],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csapdb-dev01\"},{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalSysCpu\":[405,691,3,4,6],\"totalUsrCpu\":[1172,1989,608,621,626],\"cpuCountAvg\":[8,8,8,8,8],\"coresUsed\":[0.043805555555555556,0.07444444444444445,0.016972222222222222,0.017361111111111112,0.017555555555555557],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csap-dev02\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( vmCoreReportPerVmNode.at( "/data" ) )
			.as( "vm core report PER VM does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_vm_core_per_vm_report_top_2_hosts ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		urlParams.put( "perVm", "true" );
		urlParams.put( "top", "2" );
		String vmCoreReportPerVm = retrieveAnalyticsReport( urlParams, "/api/report/custom/core" );
		JsonNode vmCoreReportPerVmNode = getJacksonMapper().readTree( vmCoreReportPerVm );
		logger.debug( "$$$$$$$ {} ", vmCoreReportPerVm );
		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalSysCpu\":[1631,1632,1704,1686,1624],\"totalUsrCpu\":[982,966,1172,1145,989],\"cpuCountAvg\":[4,4,4,4,4],\"coresUsed\":[0.036291666666666667,0.036083333333333335,0.03994444444444444,0.03931944444444444,0.036291666666666667],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csapdb-dev02\"},{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalSysCpu\":[639,886,1,3,0],\"totalUsrCpu\":[3470,3997,163,143,134],\"cpuCountAvg\":[8,8,8,8,8],\"coresUsed\":[0.1141388888888889,0.136016713091922,0.004555555555555556,0.004055555555555555,0.0037222222222222223],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csap-dev01\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( vmCoreReportPerVmNode.at( "/data" ) )
			.as( "vm core report top 2 hosts does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_health_report ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		String healthReport = retrieveAnalyticsReport( urlParams, "/api/report/custom/health" );
		JsonNode healthReportNode = getJacksonMapper().readTree( healthReport );
		logger.debug( "$$$$$$$ {} ", healthReport );
		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"UnHealthyCount\":[311,643,578,579,575],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( healthReportNode.at( "/data" ) )
			.as( "health report does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_health_report_per_vm ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "perVm", "true" );
		String healthReportPerVm = retrieveAnalyticsReport( urlParams, "/api/report/custom/health" );
		JsonNode healthReportPerVmNode = getJacksonMapper().readTree( healthReportPerVm );
		logger.debug( "$$$$$$$ {} ", healthReportPerVm );
		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"UnHealthyCount\":[127,288,288,288,288],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csapdb-dev02\"},{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"UnHealthyCount\":[57,67,0,3,0],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csap-dev01\"},{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"UnHealthyCount\":[0,0,2,0,0],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csapdb-dev01\"},{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"UnHealthyCount\":[127,288,288,288,287],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\",\"host\":\"csap-dev02\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( healthReportPerVmNode.at( "/data" ) )
			.as( "health report per vm does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_vm_report ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		urlParams.put( "metricsId", "totalMemFree" );
		String vmReport = retrieveAnalyticsReport( urlParams, "/api/report/vm" );
		JsonNode vmReportNode = getJacksonMapper().readTree( vmReport );
		logger.debug( "$$$$$$$ {} ", vmReport );
		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalMemFree\":[130403698,125362779,127791024,127585021,127484249],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( vmReportNode.at( "/data" ) )
			.as( "vm report per vm does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_vm_report_divide_by_num_samples ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		urlParams.put( "metricsId", "totalMemFree" );
		urlParams.put( "divideBy", "numberOfSamples" );
		String vmReport = retrieveAnalyticsReport( urlParams, "/api/report/vm" );
		JsonNode vmReportNode = getJacksonMapper().readTree( vmReport );
		logger.debug( "$$$$$$$ {} ", vmReport );
		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalMemFree\":[11319.765451388888,10889.748002084782,11092.970833333333,11075.088628472222,11066.341059027778],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( vmReportNode.at( "/data" ) )
			.as( "vm report divided by num samples does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_vm_report_divide_by_number ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" ); // dates
																									// always
																									// roll
																									// back
		urlParams.put( "trending", "true" );
		urlParams.put( "metricsId", "totalMemFree" );
		urlParams.put( "divideBy", "10" );
		String vmReport = retrieveAnalyticsReport( urlParams, "/api/report/vm" );

		JsonNode vmReportNode = getJacksonMapper().readTree( vmReport );
		logger.info( "Report Received: \n{} ",
			jacksonMapper.writerWithDefaultPrettyPrinter().writeValueAsString( vmReportNode ) );

		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"totalMemFree\":[1.30403698E7,1.25362779E7,1.27791024E7,1.27585021E7,1.27484249E7],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( vmReportNode.at( "/data" ) )
			.as( "vm report divided by number does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_service_report ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		urlParams.put( "metricsId", "topCpu" );

		String serviceReport = retrieveAnalyticsReport( urlParams, "/api/report/service" );
		JsonNode serviceReportNode = getJacksonMapper().readTree( serviceReport );

		logger.info( "Report Received: \n{} ",
			jacksonMapper.writerWithDefaultPrettyPrinter().writeValueAsString( serviceReportNode ) );

		String expectedReport = "[{\"date\":[\"2015-09-03\",\"2015-09-04\",\"2015-09-05\",\"2015-09-06\",\"2015-09-07\"],\"topCpu\":[41215,52615,19546,19455,17093],\"appId\":\"csapeng.gen\",\"lifecycle\":\"dev\",\"project\":\"CSAP Engineering\"}]";
		JsonNode expectedReportNode = getJacksonMapper().readTree( expectedReport );

		assertThat( serviceReportNode.at( "/data" ) )
			.as( "service report does not match" )
			.isEqualTo( expectedReportNode );
	}

	@Test
	public void verify_admin_service_report ()
			throws Exception {
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		urlParams.put( "metricsId", "topCpu" );
		urlParams.put( "serviceName", "admin" );

		String serviceReport = retrieveAnalyticsReport( urlParams, "/api/report/service" );
		JsonNode adminTrendingReport = getJacksonMapper().readTree( serviceReport );

		logger.info( "Report Received: \n{} ",
			jacksonMapper.writerWithDefaultPrettyPrinter().writeValueAsString( adminTrendingReport ) );

		List<String> dateList = jacksonMapper.readValue(
			adminTrendingReport.at( "/data/0/date" ).traverse(), new TypeReference<List<String>>() {
			} );

		assertThat( dateList )
			.as( "5 days of dates" )
			.hasSize( 5 )
			.contains( "2015-09-03", "2015-09-04", "2015-09-05", "2015-09-06", "2015-09-07" );

		List<Integer> topList = jacksonMapper.readValue(
			adminTrendingReport.at( "/data/0/topCpu" ).traverse(), new TypeReference<List<Integer>>() {
			} );

		assertThat( topList )
			.as( "5 days of topCPU" )
			.hasSize( 5 )
			.contains( 117, 208, 60, 34, 26 );
	}

	@Test
	public void verify_merged_service_report_trends ()
			throws Exception {

		String servicesToMerge = "Cssp3ReferenceTibco" + TrendingReportHelper.MULTIPLE_SERVICE_DELIMETER + "admin";
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		urlParams.put( "metricsId", "topCpu" );
		urlParams.put( "serviceName", servicesToMerge );

		String serviceReport = retrieveAnalyticsReport( urlParams, "/api/report/service" );
		JsonNode adminTrendingReport = getJacksonMapper().readTree( serviceReport );

		logger.info( "Report Received: \n{} ",
			jacksonMapper.writerWithDefaultPrettyPrinter().writeValueAsString( adminTrendingReport ) );

		List<String> dateList = jacksonMapper.readValue(
			adminTrendingReport.at( "/data/0/date" ).traverse(), new TypeReference<List<String>>() {
			} );

		assertThat( dateList )
			.as( "5 days of dates" )
			.hasSize( 5 )
			.contains( "2015-09-03", "2015-09-04", "2015-09-05", "2015-09-06", "2015-09-07" );

		List<Integer> topList = jacksonMapper.readValue(
			adminTrendingReport.at( "/data/0/topCpu" ).traverse(), new TypeReference<List<Integer>>() {
			} );

		// 117, 208, 60, 34, 26 admin
		// 8, 322, 15, 10, 14 Cssp3ReferenceTibco
		assertThat( topList )
			.as( "5 days of topCPU" )
			.hasSize( 5 )
			.contains( 125, 530, 75, 44, 40 );
	}

	@Test
	public void verify_merged_threads_report_trends_total ()
			throws Exception {

		String servicesToMerge = "Cssp3ReferenceTibco" + TrendingReportHelper.MULTIPLE_SERVICE_DELIMETER + "admin";
		Map<String, String> urlParams = mongoTest.createStandardUrlParamMap( "09/08/2015", "5" );
		urlParams.put( "trending", "true" );
		urlParams.put( "metricsId", "threadCount" );
		urlParams.put( "divideBy", "numberOfSamples" );
		urlParams.put( "allVmTotal", "true" );
		urlParams.put( "serviceName", servicesToMerge );

		String serviceReport = retrieveAnalyticsReport( urlParams, "/api/report/service" );
		JsonNode adminTrendingReport = getJacksonMapper().readTree( serviceReport );

		logger.info( "Report Received: \n{} ",
			jacksonMapper.writerWithDefaultPrettyPrinter().writeValueAsString( adminTrendingReport ) );

		assertThat( adminTrendingReport.get( "data" ).size() )
			.as( "when vmTotal is requested, separate feeds will be generated" )
			.isEqualTo( 2 );

		List<String> dateList = jacksonMapper.readValue(
			adminTrendingReport.at( "/data/0/date" ).traverse(), new TypeReference<List<String>>() {
			} );

		assertThat( dateList )
			.as( "5 days of dates" )
			.hasSize( 5 )
			.contains( "2015-09-03", "2015-09-04", "2015-09-05", "2015-09-06", "2015-09-07" );

		List<Double> topList = jacksonMapper.readValue(
			adminTrendingReport.at( "/data/0/threadCount" ).traverse(), new TypeReference<List<Double>>() {
			} );

		// 117, 208, 60, 34, 26 admin
		// 8, 322, 15, 10, 14 Cssp3ReferenceTibco
		assertThat( topList )
			.as( "5 days of threadCount" )
			.hasSize( 5 )
			.contains( 115.9875, 113.1620440821256, 110.66805555555555, 110.659375, 110.65902777777777 );
	}

	private String retrieveAnalyticsReport ( Map<String, String> urlParams, String url )
			throws Exception {

		MockHttpServletRequestBuilder requestBuilder = post( url );
		for ( String key : urlParams.keySet() ) {
			requestBuilder.param( key, urlParams.get( key ) );
		}
		requestBuilder.contentType( MediaType.APPLICATION_FORM_URLENCODED );
		requestBuilder.accept( MediaType.APPLICATION_JSON );
		ResultActions resultActions = mockMvc.perform( requestBuilder );
		String responseText = resultActions.andExpect( status().isOk() )
			.andExpect( content().contentType( MediaType.APPLICATION_JSON ) )
			.andReturn()
			.getResponse()
			.getContentAsString();

		return responseText;
	}

}
