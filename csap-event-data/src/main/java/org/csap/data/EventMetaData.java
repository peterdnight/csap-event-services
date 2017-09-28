package org.csap.data;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.DistinctIterable;

import org.csap.data.db.EventDataReader;
import org.csap.helpers.CsapRestTemplateFactory;
import org.javasimon.SimonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventMetaData {

	private Logger logger = LoggerFactory.getLogger( getClass() );

	private List appIds = new ArrayList();
	private List categories = new ArrayList();
	private List hosts = new ArrayList();
	private List<String> lifecycles = new ArrayList();
	private List projects = new ArrayList();
	private List uiUsers = new ArrayList();

	@Override
	public String toString() {
		return "EventMetaData{" + "appIds=" + appIds + ", \ncategories=" + categories + ", \nhosts=" + hosts
				+ ", \nlifecycles=" + lifecycles + ", \nprojects=" + projects + ",\n uiUsers=" + uiUsers + '}';
	}

	public List getAppIds() {
		return appIds;
	}

	public void setAppIds(DistinctIterable<String> appIdsIterable) {
		try {
			appIdsIterable.iterator().forEachRemaining( appId -> appIds.add( appId ) );
		} catch ( Exception e ) {
			SimonManager.getCounter( EventDataReader.SEARCH_FILTER_KEY + "errors" ).increase();
			logger.error( "Failed to load: {}", CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}
	}

	public void setAppIds(List appIds) {
		this.appIds = appIds;
	}

	public List getCategories() {
		return categories;
	}

	public void setCategories(DistinctIterable<String> categoriesIterable) {
		try {
			categoriesIterable.iterator().forEachRemaining( category -> categories.add( category ) );
		} catch ( Exception e ) {
			SimonManager.getCounter( EventDataReader.SEARCH_FILTER_KEY + "errors" ).increase();
			logger.error( "Failed to load: {}", CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}
	}

	public void setCategories(List categories) {
		this.categories = categories;
	}

	public List getProjects() {
		return projects;
	}

	public void setProjects(DistinctIterable<String> projectsIterable) {
		try {
			projectsIterable.iterator().forEachRemaining( project -> projects.add( project ) );
		} catch ( Exception e ) {
			SimonManager.getCounter( EventDataReader.SEARCH_FILTER_KEY + "errors" ).increase();
			logger.error( "Failed to load: {}", CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}
	}

	public void setProjects(List projects) {
		this.projects = projects;
	}

	public List getLifecycles() {
		return lifecycles;
	}

	public void setLifecycles(DistinctIterable<String> lifecyclesIterable) {
		try {
			lifecyclesIterable.iterator().forEachRemaining( life -> lifecycles.add( life ) );
		} catch ( Exception e ) {
			SimonManager.getCounter( EventDataReader.SEARCH_FILTER_KEY + "errors" ).increase();
			logger.error( "Failed to load: {}", CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}
	}

	public void setLifecycles(List lifecycles) {
		this.lifecycles = lifecycles;
	}

	public List getUiUsers() {
		return uiUsers;
	}

	public void setUiUsers(DistinctIterable<String> uiUsersIterable) {
		try {
			
			if ( uiUsersIterable == null ) {
				uiUsers.add( "NoMatchFound" )  ;
			} else {

				uiUsersIterable.iterator().forEachRemaining( uiUser -> {
					
					logger.debug( "user adding: {}", uiUser );
					uiUsers.add( uiUser )  ;
				});
				
			}
		} catch ( Exception e ) {
			SimonManager.getCounter( EventDataReader.SEARCH_FILTER_KEY + "errors" ).increase();
			logger.error( "Failed to load: {}", CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}
	}

	public void setUiUsers(List uiUsers) {
		this.uiUsers = uiUsers;
	}

	public List getHosts() {
		return hosts;
	}

	public void setHosts(DistinctIterable<String> hostsIterable) {
		try {
			hostsIterable.iterator().forEachRemaining( host -> hosts.add( host ) );
		} catch ( Exception e ) {
			SimonManager.getCounter( EventDataReader.SEARCH_FILTER_KEY + "errors" ).increase();
			logger.error( "Failed to load: {}", CsapRestTemplateFactory.getFilteredStackTrace( e, "csap" ) );
		}
	}

	public void setHosts(List hosts) {
		this.hosts = hosts;
	}

}
