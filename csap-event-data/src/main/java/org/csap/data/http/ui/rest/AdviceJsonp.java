package org.csap.data.http.ui.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.AbstractJsonpResponseBodyAdvice;


/**
 * Will append function if JSONP is present
 * 
 * @author pnightin
 *
 */
@ControllerAdvice
public class AdviceJsonp extends AbstractJsonpResponseBodyAdvice {
	private Logger logger = LoggerFactory.getLogger(getClass());
    public AdviceJsonp() {
        super("callback");
        //logger.info("in here");
    }

}
