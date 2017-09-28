package org.csap.analytics.http.ui.rest;

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

    public AdviceJsonp() {
        super("callback");
    }

}
