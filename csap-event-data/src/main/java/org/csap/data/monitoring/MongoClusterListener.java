/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.csap.data.monitoring;

import com.mongodb.ReadPreference;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author pnightin
 */
public class MongoClusterListener implements ClusterListener {

	final Logger logger = LoggerFactory.getLogger( getClass() );

	private final ReadPreference readPreference;
	private boolean isWritable;
	private boolean isReadable;

	public MongoClusterListener(final ReadPreference readPreference) {
		this.readPreference = readPreference;
	}

	@Override
	public void clusterOpening(final ClusterOpeningEvent clusterOpeningEvent) {
		logger.info( "{}",
				clusterOpeningEvent.getClusterId() );

	}

	@Override
	public void clusterClosed(final ClusterClosedEvent clusterClosedEvent) {
		logger.info( "{}",
				clusterClosedEvent.getClusterId() );
	}

	/**
	 * Verbose as timings will change on every iteration. To avoid overloading logs, use state in class to limit output.
	 *
	 * @param event
	 */
	@Override
	public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {

		logger.debug( "{}", event );
//		if ( (event.getNewDescription().hasWritableServer() != event.getPreviousDescription().hasWritableServer())
//				|| (event.getNewDescription().hasReadableServer( readPreference ) != event.getPreviousDescription().hasReadableServer( readPreference ))
//				|| (event.getPreviousDescription().getShortDescription().contains( "UNKNOWN")) ) {

		if ( event.getNewDescription().getShortDescription().length() !=  event.getPreviousDescription().getShortDescription().length() ) {
			StringBuilder mongoStatus = new StringBuilder( "\n Status Change:" );

			for ( ServerDescription desc : event.getNewDescription().getServerDescriptions() ) {
				mongoStatus.append( "\n " + desc.getShortDescription() );
			}

			logger.warn( mongoStatus.toString() );
		}

//		// mongoStatus.append( "\n" +event.getNewDescription().getShortDescription() );
//		if ( !isWritable ) {
//			if ( event.getNewDescription().hasWritableServer() ) {
//				isWritable = true;
//				mongoStatus.append( "\n\n Writable server available!" );
//			}
//		} else if ( !event.getNewDescription().hasWritableServer() ) {
//			isWritable = false;
//			mongoStatus.append( "\n\n No writable server available!" );
//		}
//
//		if ( !isReadable ) {
//			if ( event.getNewDescription().hasReadableServer( readPreference ) ) {
//				isReadable = true;
//				mongoStatus.append( "\n\n Readable server available!" );
//			}
//		} else if ( !event.getNewDescription().hasReadableServer( readPreference ) ) {
//			isReadable = false;
//			mongoStatus.append( "\n\n No readable server available!" );
//		}
	}
}
