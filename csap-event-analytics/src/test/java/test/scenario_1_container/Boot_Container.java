package test.scenario_1_container;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import java.net.URL;

import javax.inject.Inject;

import org.apache.commons.lang3.text.WordUtils;
import org.csap.analytics.CsapAnalyticsApplication;
import org.csap.analytics.LandingPage;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith ( SpringJUnit4ClassRunner.class )
@SpringBootTest ( classes = { CsapAnalyticsApplication.class } )
@ActiveProfiles ( "junit" )
public class Boot_Container {
	final static private Logger logger = LoggerFactory.getLogger( Boot_Container.class );

	public static String TC_HEAD = "\n\n ========================= UNIT TEST =========================== \n\n";

	private static boolean isJvmInfoPrinted = false;

	public static void printTestHeader ( String description ) {

		if ( !isJvmInfoPrinted ) {
			isJvmInfoPrinted = true;
			System.out.println( "Working Directory = " +
					System.getProperty( "user.dir" ) );
			StringBuffer sbuf = new StringBuffer();
			// Dump log4j first - if it does not work, nothing will
			String resource = "log4j2.yml";
			URL configFile = ClassLoader.getSystemResource( resource );
			try {
				sbuf.append( "\n\n ** " + resource + " found in: " + configFile.toURI().getPath() );
			} catch (URISyntaxException e) {
				logger.error( "" );
			}

			// Now dump nicely formatted classpath.
			sbuf.append( "\n\n ====== JVM Classpath is: \n"
					+ WordUtils.wrap( System.getProperty( "java.class.path" ).replaceAll( ";", " " ), 140 ) );
			System.out.println( sbuf );
		}

		logger.info( "\n\n *********************     " + description + "   ***********************\n\n" );
	}

	@Autowired
	private ApplicationContext applicationContext;

	@Test
	public void load_context () {
		logger.info( Boot_Container.TC_HEAD );

		assertThat( applicationContext.getBeanDefinitionCount() )
			.as( "Spring Bean count" )
			.isGreaterThan( 100 );

		assertThat( applicationContext.getBean( LandingPage.class ) )
			.as( "SpringRequests controller loaded" )
			.isNotNull();

	}

	@Inject
	private StandardPBEStringEncryptor encryptor;

	@Test
	public void testEncryption () {

		String testSample = "Testing encyrpt";
		String encSample = encryptor.encrypt( testSample );

		String message = "Encoding of  " + testSample + " is " + encSample;
		logger.info( TC_HEAD + message );

		assertThat( testSample ).isNotEqualTo( encSample );

		assertThat( testSample ).isEqualTo( encryptor.decrypt( encSample ) );
		// assertTrue( encryptor.decrypt( encSample).equals( testSample) ) ;

	}
}
