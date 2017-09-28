require( ["table", "search", "admin"], function ( table, searchModule, adminModule ) {

	console.log( "Main module" );

	$( document ).ready( function () {
		initialize();
	} );

	function initialize() {

		CsapCommon.configureCsapAlertify();
		registerUiEvents();
		searchModule.initialize();
		adminModule.initialize();
		table.initialize();
	}
	function registerUiEvents() {
		$( '[title!=""]' ).qtip( {
			style: {
				classes: 'qtip-rounded csapqtip',
				width: "500"
			},
			position: {
				my: "left top",
				at: "bottom right"
						// target: $('#searchButton')

			}

		} );
		/*
		 * $('#instanceTable tbody tr').qtip({ content: { title: "Version
		 * Information", button: 'Close' }, style: { classes: 'qtip-rounded
		 * csapqtip', width: "500" }, position: { my: 'bottom left', // Position
		 * my top left... at: 'top left' } });
		 */
	}


} );

function getParameterByName( name ) {
	name = name.replace( /[\[]/, "\\\[" ).replace( /[\]]/, "\\\]" );
	var regexS = "[\\?&]" + name + "=([^&#]*)", regex = new RegExp( regexS ), results = regex
			.exec( window.location.href );
	if ( results == null ) {
		return "";
	} else {
		return decodeURIComponent( results[1].replace( /\+/g, " " ) );
	}
}

function precise_round( value, decPlaces ) {
	var val = value * Math.pow( 10, decPlaces );
	var fraction = (Math.round( (val - parseInt( val )) * 10 ) / 10);
	if ( fraction == -0.5 )
		fraction = -0.6;
	val = Math.round( parseInt( val ) + fraction ) / Math.pow( 10, decPlaces );
	return val;
}

if ( typeof String.prototype.startsWith != 'function' ) {
	// see below for better implementation!
	String.prototype.startsWith = function ( str ) {
		return this.indexOf( str ) == 0;
	};
}
