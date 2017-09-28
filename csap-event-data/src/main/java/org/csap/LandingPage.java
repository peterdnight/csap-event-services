package org.csap;

import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;

import javax.inject.Inject;
import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.csap.data.db.HealthEventWriter;
import org.csap.integations.CsapSecurityConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@CsapMonitor
@RequestMapping("/")
public class LandingPage {

	protected final Log logger = LogFactory.getLog( getClass() );

	@Autowired(required = false)
	private CsapSecurityConfiguration securityConfig;

	@RequestMapping(method = RequestMethod.GET)
	public String get(ModelMap springViewModel, HttpSession session) {

		boolean isAdmin = true;

		if ( securityConfig != null ) {
			Collection<?> authorities = SecurityContextHolder.getContext()
					.getAuthentication()
					.getAuthorities();
			if ( !securityConfig.getAndStoreUserRoles( session )
					.contains( CsapSecurityConfiguration.ADMIN_ROLE ) ) {
				isAdmin = false;
			}
		}
		springViewModel.addAttribute( "admin", isAdmin );
		addCommonAttributes( springViewModel );
		return "events/events";
	}

	@RequestMapping("/stats")
	public String stats() {
		return "dbstats/stats";
	}

	@RequestMapping("/test")
	public String test(Model springViewModel) {
		return "/LandingPage";
	}

	@RequestMapping("/events")
	public String events(ModelMap springViewModel) {
		addCommonAttributes( springViewModel );
		return "events/events";
	}
	
	@Autowired
	private Environment springEnvironment;

	private void addCommonAttributes ( ModelMap springViewModel ) {
		springViewModel.addAttribute( "csapPageLabel", "Csap Events Browser" );
		springViewModel.addAttribute( "agentHostUrlPattern", springEnvironment.getProperty( "csap-agent.host-url-pattern" )  );
	}

	@RequestMapping("/currentTime")
	public void currentTime(PrintWriter writer) {

		logger.info( "SpringMvc writer" );

		writer.println( "currentTime: "
				+ LocalDateTime.now().format( DateTimeFormatter.ofPattern( "HH:mm:ss,   MMMM d  uuuu " ) ) );

		// templates are in: resources/templates/*.html
		// leading "/" is critical when running in a jar
		return;
	}

	@RequestMapping("/testNullPointer")
	public String testNullPointer() {

		if ( System.currentTimeMillis() > 1 ) {
			throw new NullPointerException( "For testing only" );
		}

		return "hello";
	}
	
	@Inject
	HealthEventWriter healthEventWriter ;
	
	@RequestMapping("/testAsync")
	@ResponseBody
	public String testAsync (
								@RequestParam(value = "delaySeconds", required = false, defaultValue = "5") int delaySeconds )
			throws Exception {
		String message = "Hello from " + this.getClass().getSimpleName()
					+ " at " + LocalDateTime.now().format( DateTimeFormatter.ofPattern( "hh:mm:ss" ) );
		healthEventWriter.printMessage( message, delaySeconds );
		return "Look in logs for async to complete in: " + delaySeconds + " seconds";
	}

	

	@RequestMapping("/missingTemplate")
	public String missingTempate(Model springViewModel) {

		logger.info( "Sample thymeleaf controller" );

		springViewModel.addAttribute( "dateTime",
				LocalDateTime.now().format( DateTimeFormatter.ofPattern( "HH:mm:ss,   MMMM d  uuuu " ) ) );

		// templates are in: resources/templates/*.html
		// leading "/" is critical when running in a jar
		return "/missingTemplate";
	}

	@RequestMapping("/malformedTemplate")
	public String malformedTemplate(Model springViewModel) {

		logger.info( "Sample thymeleaf controller" );

		springViewModel.addAttribute( "dateTime",
				LocalDateTime.now().format( DateTimeFormatter.ofPattern( "HH:mm:ss,   MMMM d  uuuu " ) ) );

		// templates are in: resources/templates/*.html
		// leading "/" is critical when running in a jar
		return "/MalformedExample";
	}

}
