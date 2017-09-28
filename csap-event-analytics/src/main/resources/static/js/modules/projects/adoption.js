$( document ).ready( function () {
	var summary = new Summary();
	summary.appInit();

} );


function Summary() {
	
	var $minimumHosts=$("#minimumHosts");

	this.appInit = function () {
		console.log( "Init in summary" );
	};
	$( '[title!=""]' ).qtip( {
		style: {
			classes: 'qtip-rounded csapqtip'
		}
	} );
	$( '[data-qtipleft!=""]' ).qtip( {
		content: {
			attr: 'data-qtipleft'
		},
		style: {
			classes: 'qtip-rounded csapqtip'
		},
		position: {
			my: 'top right', // Position my top left...
			at: 'bottom left'
		},
		hide: {
			inactive: 5000
		}
	} );


	$( '#explore' ).click( function () {
		document.location.href = baseUrl + 'trends';
	} );


	$minimumHosts.change( function () {
		getCsapAdoptionReport();
	} );

	$.when( loadDisplayNames() ).then( getCsapAdoptionReport ).then( buildAdoptionTotals );

	$( "#explore" ).hover( function () {
		$( this ).css( "text-decoration", "underline" );
		$( this ).css( 'cursor', 'pointer' );
	}, function () {
		$( this ).css( "text-decoration", "none" );
		$( this ).css( 'cursor', 'default' );
	} );

	var metricsDataMap = null;
	function getMetricsSamplesForBaselines() {
		console.log( "Getting reports to build size estimates" );
		metricsDataMap = new Object();


		$.getJSON( api + "/report/reportCounts", { } )
				.done( function ( dataJson ) {

					metricsDataMap['hostServiceDataJson'] = dataJson;

				} );


		var life = globalLife;
//		if ( metricsDataMap['hostServiceDataJson'].life == null ) {
//			life = "dev";
//		}
		console.log( "life::" + life );
		var host = "";
		if ( life == "dev" ) {
			host = "csap-dev01";
		} else {
			host = "csap-prd01";
		}
		var params = {
			numberOfDays: "1",
			appId: "csapeng.gen",
			life: life,
			dateOffSet: "1",
			padLatest: false
		}

		var hostUrl = baseUrl + "api/metrics/" + host;
		$.getJSON( hostUrl + "/resource_30", params )
				.done( function ( dataJson ) {
					var totalHostLength = 0;
					for ( var key in dataJson.data ) {
						if ( key === "timeStamp" )
							continue;
						totalHostLength = totalHostLength + dataJson.data[key].length;
					}
					metricsDataMap['totalHostLength'] = totalHostLength;
				} );


		$.getJSON( hostUrl + "/service_30", params )
				.done( function ( dataJson ) {
					var serviceReportLength = 0;
					for ( var key in dataJson.data ) {
						if ( key === "timeStamp" )
							continue;
						if ( key.indexOf( "CsAgent" ) > 1 ) {
							serviceReportLength = serviceReportLength + dataJson.data[key].length;
						}
					}
					metricsDataMap['serviceReportLength'] = serviceReportLength;
				} );

		$.getJSON( hostUrl + "/jmx_30", params )
				.done( function ( dataJson ) {
					var jmxReportLength = 0;
					for ( var key in dataJson.data ) {
						if ( key === "timeStamp" )
							continue;
						if ( key.indexOf( "CsAgent" ) > 1 ) {
							jmxReportLength = jmxReportLength + dataJson.data[key].length;
						}
					}
					metricsDataMap['jmxReportLength'] = jmxReportLength;
				} );


	}

	function buildAdoptionTotals() {
		console.log( "buildAdoptionTotals" )

		getMetricsSamplesForBaselines();

		$( document ).ajaxStop( function () {
			console.log( "buildAdoptionTotals - all completed" )
			//console.log("metricsDataMap &&&--->"+JSON.stringify(metricsDataMap));
			var toolTip = "Reports:     " + metricsDataMap['hostServiceDataJson'].hostReportCount + " <br>Points per day: " + metricsDataMap['totalHostLength'];
			$( "#hostDataPoints" ).attr( "title", toolTip );

			var totalDataPoints = metricsDataMap['totalHostLength'] * metricsDataMap['hostServiceDataJson'].hostReportCount / 1000000000;
			$( "#hostData" ).html( totalDataPoints.toFixed( 3 ) );

			var serviceToolTip = "Reports:     " + metricsDataMap['hostServiceDataJson'].serviceReportCount
					+ " <br> Service points per day: " + metricsDataMap['serviceReportLength']
					+ " <br> Jmx points per day: " + metricsDataMap['jmxReportLength']
					;

			$( "#serviceDataPoints" ).attr( "title", serviceToolTip );
			//multiply by 2 to get service and jmx reports total
			var avgServiceReport = totalInstanceCount / (metricsDataMap['hostServiceDataJson'].serviceReportCountOneDay * 2);

			var totalServiceDataPoints = (metricsDataMap['serviceReportLength'] + metricsDataMap['jmxReportLength']) *
					metricsDataMap['hostServiceDataJson'].serviceReportCount * avgServiceReport / 1000000000;
			$( "#serviceData" ).html( totalServiceDataPoints.toFixed( 3 ) );
		} );
	}


	var displayNameData;
	function loadDisplayNames() {
		var r = $.Deferred();
		$.getJSON( api + "/report/displayNames", {
		} )
				.success( function ( loadJson ) {
					displayNameData = loadJson;
				} );
		setTimeout( function () {
			console.log( 'loading display names  done' );
			r.resolve();
		}, 500 );
		return r;
	}

	function getCsapAdoptionReport() {

		var loadingImage = '<img title="Querying Events for counts" id="loadCountImage" width="14" src="' + baseUrl + 'images/animated/loadSmall.gif"/>';
		$( ".value" ).html( loadingImage );
		console.log( "Loading Adoption Summary" )
		$( 'body' ).css( 'cursor', 'wait' );
		var params = { "days": $( '#numDaysSelect' ).val() };
		$.getJSON( api + "/report/adoption/current", params )

				.done( function ( loadJson ) {
					processAdoptionData( loadJson );
				} )

				.fail( function ( jqXHR, textStatus, errorThrown ) {

					handleConnectionError( "Getting analytics", errorThrown );

				} );
	}

	var totalInstanceCount = 0;
	function processAdoptionData( dataJson ) {
		var totalBusinessPrograms = 0;
		var totalHosts = 0;
		var totalCpus = 0;
		var totalEngineers = 0;
		var totalServices = 0;
		var totalInstances = 0;
		var totalActivity = 0;
		var hostsPie = [];
		var cpuPie = [];
		var servicesPie = [];
		var serviceInstancesPie = [];
		var usersPie = [];
		var userActivityPie = [];
		var minimumHostFilter = $minimumHosts.val();
		console.log( "vmFilter" + minimumHostFilter );
		var colorMap = constructColorMap( dataJson );

		var numFilteredHosts = 0
		for ( var i = 0; i < dataJson.length; i++ ) {
			if ( dataJson[i].vms < minimumHostFilter || isHidden )
				continue;
			numFilteredHosts++;
		}

		if ( numFilteredHosts == 0 ) {
			alertify.notify( "Reseting minimum hosts to 1" );
			$minimumHosts.val( 1 );
			minimumHostFilter = 1;
		}

		for ( var i = 0; i < dataJson.length; i++ ) {
			var localPackName = dataJson[i]._id;
			var isHidden = false;

			if ( displayNameData[dataJson[i]._id] && displayNameData[dataJson[i]._id].hidden ) {
				isHidden = displayNameData[dataJson[i]._id].hidden;
			}

			if ( 'total' == dataJson[i]._id )
				continue;
			totalInstanceCount += dataJson[i].instanceCount;
			if ( dataJson[i].vms < minimumHostFilter || isHidden )
				continue;


			var key = dataJson[i]._id;
			totalHosts += dataJson[i].vms;
			totalCpus += dataJson[i].cpuCount;
			totalEngineers += dataJson[i].activeUsers;
			totalServices += dataJson[i].serviceCount;
			totalInstances += dataJson[i].instanceCount;
			totalActivity += dataJson[i].totalActivity;

			totalBusinessPrograms++;

			var displayLabel = "";
			if ( displayNameData[dataJson[i]._id] && displayNameData[dataJson[i]._id].displayName ) {
				displayLabel = displayNameData[dataJson[i]._id].displayName;
			} else {
				displayLabel = dataJson[i]._id;
			}

			var hostPieElement = new Object();
			hostPieElement.label = displayLabel;
			hostPieElement.data = precise_round( dataJson[i].vms, 0 );
			hostPieElement.color = colorMap[key];
			hostPieElement.url = baseUrl + 'trends?numberOfDays=1&projectName=' + dataJson[i]._id;
			hostsPie.push( hostPieElement );

			var cpuPieElement = new Object();
			cpuPieElement.label = displayLabel;
			cpuPieElement.data = precise_round( dataJson[i].cpuCount, 0 );
			cpuPieElement.color = colorMap[key];
			cpuPieElement.url = baseUrl + 'trends?numberOfDays=1&projectName=' + dataJson[i]._id;
			cpuPie.push( cpuPieElement );

			var servicesPieElement = new Object();
			servicesPieElement.label = displayLabel;
			servicesPieElement.data = precise_round( dataJson[i].serviceCount, 0 );
			servicesPieElement.color = colorMap[key];
			servicesPieElement.url = baseUrl + 'trends?numberOfDays=1&projectName=' + dataJson[i]._id;
			servicesPie.push( servicesPieElement );

			var serviceInstancePieElement = new Object();
			serviceInstancePieElement.label = displayLabel;
			serviceInstancePieElement.data = precise_round( dataJson[i].instanceCount, 0 );
			serviceInstancePieElement.color = colorMap[key];
			serviceInstancePieElement.url = 'services?projName=' + dataJson[i]._id + '&appId=' + dataJson[i].appId;
			//serviceInstancePieElement.url='analytics?projectName='+dataJson[i]._id;
			serviceInstancesPie.push( serviceInstancePieElement );

			var usersPieElement = new Object();
			usersPieElement.label = displayLabel;
			usersPieElement.data = precise_round( dataJson[i].activeUsers, 0 );
			usersPieElement.color = colorMap[key];
			usersPieElement.url = baseUrl + 'trends?numberOfDays=1&projectName=' + dataJson[i]._id;
			usersPie.push( usersPieElement );

			var userActivityPieElement = new Object();
			userActivityPieElement.label = displayLabel;
			userActivityPieElement.data = precise_round( dataJson[i].totalActivity, 0 );
			userActivityPieElement.color = colorMap[key];
			userActivityPieElement.url = baseUrl + 'trends?numberOfDays=1&projectName=' + dataJson[i]._id;
			userActivityPie.push( userActivityPieElement );

		}

		sortOnLabel( hostsPie );
		sortOnLabel( cpuPie );
		sortOnLabel( servicesPie );
		sortOnLabel( serviceInstancesPie );
		sortOnLabel( usersPie );
		sortOnLabel( userActivityPie );

		updateSummaryTable( totalBusinessPrograms, totalHosts,
				totalCpus, totalServices,
				totalInstances, totalEngineers,
				totalActivity );
		plotInteractivePie( 'hostsPieChart', hostsPie, false );
		plotInteractivePie( 'cpuPieChart', cpuPie, false );
		plotInteractivePie( 'servicesPieChart', servicesPie, false );
		plotInteractivePie( 'serviceInstancesPieChart', serviceInstancesPie, true );
		plotInteractivePie( 'userPieChart', usersPie, false );
		plotInteractivePie( 'userActivityPieChart', userActivityPie, false );

		$( 'body' ).css( 'cursor', 'default' );

	}

	function constructColorMap( dataJson ) {
		var keys = [];
		for ( var i = 0; i < dataJson.length; i++ ) {
			var key = dataJson[i]._id;
			keys.push( key );
		}
		keys.sort();
		/*var colors = ['#B0171F','#32CD32',
		 '#FFE7BA','#87CEFF','#FFFF00',
		 '#EEB4B4','#00FF7F','#DAA520',
		 '#FFF68F','#FF83FA','#FF8C00',
		 '#4876FF','#00868B','#FF4500',
		 '#00EE76','#71C671','#1A1A1A'
		 ];*/
		var colors = ["#00E5EE", "#FAA43A", "#60BD68",
			"#F17CB0", "#B2912F", "#B276B2",
			"#DECF3F", "#F15854", "#4D4D4D",
			'#87CEFF', '#FFFF00',
			'#EEB4B4', '#00FF7F', '#DAA520',
			'#FFF68F', '#FF83FA', '#FF8C00',
			'#4876FF', '#00868B', '#FF4500',
			'#00EE76', '#71C671', '#1A1A1A'
		];
		var colorMap = new Object();
		colorMap['total'] = '#00BFFF';
		for ( var i = 0; i < keys.length; i++ ) {
			if ( 'total' == keys[i] )
				continue;
			colorMap[keys[i]] = colors[i];
		}
		console.log( colorMap );
		return colorMap;
	}

	function sortOnLabel( pieElement ) {
		pieElement.sort( function ( a, b ) {
			//console.log(a);
			var x = a.label.toLowerCase();
			var y = b.label.toLowerCase();
			return x < y ? -1 : x > y ? 1 : 0;
		} );
	}

	function plotPieChart( divId, data ) {
		$.plot( $( '#' + divId ), data, {
			series: {
				pie: {
					show: true,
					label: {
						show: true,
						radius: 1 / 2,
						// Added custom formatter here...
						formatter: function ( label, series ) {
							//alertify.log(data);
							//return(point.percent.toFixed(2) + '%');
							//return (series.data[0][1]);
							return '<div style="font-size:16pt;text-align:center;padding:4px;color:black;">' + series.data[0][1] + '</div>';
						}

					}
				}
			}, legend: {
				show: true
			}
		} );
	}
	function plotInteractivePie( divId, data, newWindow ) {

		console.log( "Building Graph: ", divId, data );
		$.plot( $( '#' + divId ), data, {
			series: {
				pie: {
					show: true,
					label: {
						show: true,
						radius: 1 / 2,
						// Added custom formatter here...
						formatter: function ( label, series ) {
							//alertify.log(data);
							//return(point.percent.toFixed(2) + '%');
							//return (series.data[0][1]);
							return '<div style="font-size:16pt;text-align:center;padding:4px;color:black;">' + series.data[0][1] + '</div>';
						}

					}
				}
			}, legend: {
				show: true
			}, grid: {
				hoverable: true,
				clickable: true
			}
		} );
		$( '#' + divId ).hover( function () {
			$( this ).css( 'cursor', 'pointer' );
		},
				function () {
					$( this ).css( 'cursor', 'normal' );
				} );

		$( "#" + divId ).unbind( "plotclick" );
		$( '#' + divId ).bind( "plotclick", function ( event, pos, obj ) {
			if ( newWindow ) {
				window.open( data[obj.seriesIndex].url );
			} else {
				window.location.replace( data[obj.seriesIndex].url );
			}
		} );
	}
	function updateSummaryTable( totalProjects, totalHosts, totalCpus, totalServices, totalServiceInstances, totalUsers, totalUserActivity ) {
		$( "#projects" ).html( precise_round( totalProjects, 0 ) );
		$( "#hosts" ).html( precise_round( totalHosts, 0 ) + ' (' + precise_round( totalCpus, 0 ) + ')' );
		$( "#services" ).html( precise_round( totalServices, 0 ) );
		$( "#instances" ).html( precise_round( totalServiceInstances, 0 ) );
		$( "#users" ).html( precise_round( totalUsers, 0 ) );
		$( "#userActivity" ).html( precise_round( totalUserActivity, 0 ) );
	}

	function precise_round( value, decPlaces ) {
		var val = value * Math.pow( 10, decPlaces );
		var fraction = (Math.round( (val - parseInt( val )) * 10 ) / 10);
		if ( fraction == -0.5 )
			fraction = -0.6;
		val = Math.round( parseInt( val ) + fraction ) / Math.pow( 10, decPlaces );
		return val;
	}
}