package org.csap.analytics;

import org.csap.helpers.CsapRestTemplateFactory;
import org.csap.integations.CsapInformation;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.exceptions.EncryptionOperationNotPossibleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

/**
 *
 * @author paranant
 */
@Configuration
@ConfigurationProperties ( prefix = "my-service-configuration.event-config" )
public class EventServiceConfiguration {

	private Logger logger = LoggerFactory.getLogger( getClass() );

	private String url;
	private String pass;
	private String user;

	@Autowired
	public EventServiceConfiguration( CsapInformation csapInformation, StandardPBEStringEncryptor encryptor ) {

		this.csapInformation = csapInformation;
		this.encryptor = encryptor;

	}

	private CsapInformation csapInformation;
	private StandardPBEStringEncryptor encryptor;

	public void postEventData ( String jsonDoc ) {

		String eventServiceUrl = csapInformation.getLoadBalancerUrl() + url;
		try {

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType( org.springframework.http.MediaType.APPLICATION_JSON );

			MultiValueMap<String, String> requestObj = new LinkedMultiValueMap<String, String>();
			requestObj.add( "userid", user );
			requestObj.add( "pass", pass );
			requestObj.add( "eventJson", jsonDoc );

			logger.debug( "Posting to url: {} \n\t Data: {}", eventServiceUrl, requestObj );
			String result = getRestPostTemplate().postForObject( eventServiceUrl, requestObj, String.class );
			logger.debug( "result{}", result );
		} catch (Exception e) {
			logger.error( "{} Failed to post event data user:{} , reason {}", eventServiceUrl, user, CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}

	}

	public RestTemplate getRestPostTemplate () {

		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
		// factory.setHttpClient(httpClient);
		// factory.getHttpClient().getConnectionManager().getSchemeRegistry().register(scheme);

		factory.setConnectTimeout( 20000 );
		factory.setReadTimeout( 20000 );

		RestTemplate restTemplate = new RestTemplate( factory );
		restTemplate.getMessageConverters().clear();
		restTemplate.getMessageConverters().add( new FormHttpMessageConverter() );
		restTemplate.getMessageConverters().add( new StringHttpMessageConverter() );

		return restTemplate;
	}

	public void setPass ( String pass ) {
		String password = pass;
		try {
			password = encryptor.decrypt( pass );
		} catch (EncryptionOperationNotPossibleException e) {
			logger.warn( "Password is not encrypted. Use CSAP encrypt to generate" );
		}
		logger.info( "Password is: {}", password );
		this.pass = password;
	}

	public String getPass () {
		return pass;
	}

	public String getUser () {
		return user;
	}

	public void setUser ( String user ) {
		this.user = user;
	}

	/**
	 * @return the url
	 */
	public String getUrl () {
		return url;
	}

	/**
	 * @param url
	 *            the url to set
	 */
	public void setUrl ( String url ) {
		this.url = url;
	}

}
