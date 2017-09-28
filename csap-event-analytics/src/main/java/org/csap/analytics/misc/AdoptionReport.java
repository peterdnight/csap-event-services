package org.csap.analytics.misc;

import javax.inject.Inject;

import org.csap.analytics.db.CsapAdoptionReportBuilder;
import org.javasimon.SimonManager;
import org.javasimon.Split;
import org.javasimon.utils.SimonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


@Service
public class AdoptionReport {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	
	@Inject
	private CsapAdoptionReportBuilder adoptionReportBuilder;
	
	// Every night 15 minutes after midnight
	@Async
	@Scheduled(cron="0 15 0 * * ?")
	public void dailySchedulerForAdoptionReport(){
		
		Split adoptionTimer = SimonManager.getStopwatch( "AdoptionReport" ).start();
		logger.info("Daily Adoption Report - Starting");
		adoptionReportBuilder.buildAdoptionReportAndSaveToDB(1);
		
		adoptionTimer.stop() ;
		
		logger.info( "Daily Adoption Report - Completed, Time Taken: {}",
				SimonUtils.presentNanoTime( adoptionTimer.runningFor() ) );
		
	}
	

}
