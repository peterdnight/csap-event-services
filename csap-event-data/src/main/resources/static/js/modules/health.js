define([], function () {

	 console.log("health module loaded");

	 return {
		  //
		  constructHealthTable: function (dataJson) {
				constructHealthTable(dataJson)
		  }
	 }
	 
	 function constructHealthTable(dataJson) {
		  console.log("Constructing health table from module");
		  if (dataJson.data.length == 0)
				return;
		  $("#healthReportsTable").css("display", "table");
		  var healthTable = $("#healthReportsTable");
		  $("#healthReportsTable tbody").empty();
		  var healthBody = jQuery('<tbody/>', {});
		  healthTable.append(healthBody);
		  var latestDate = dataJson.data[0].createdOn.date;
		  var projectParameter = '';
		  projectParameter = getParameterByName("project");
		  var categoryParameter = '';
		  categoryParameter = getParameterByName("category");
		  var dateParameter = '';
		  dateParameter = getParameterByName("date");
		  for (var i = 0; i < dataJson.data.length; i++) {
				if (projectParameter != '' && dateParameter != '' && categoryParameter == '/csap/reports/health') {
					 //display all records.
				} else {
					 if (latestDate != dataJson.data[i].createdOn.date)
						  continue;
				}
				//if(latestDate != dataJson.data[i].createdOn.date && projectParameter == '' && categoryParameter == '/csap/reports/health') continue;
				var healthRow = jQuery('<tr/>', {});
				var hostName = dataJson.data[i].host;
//				if (dataJson.data[i].host.indexOf("yourcompany.com") > 0) {
//					 hostName = dataJson.data[i].host;
//				} else {
//					 hostName = dataJson.data[i].host + ".yourcompany.com";
//				}
				var hostHref = "<a target='_blank' class='simple' href=http://" + hostName + ":8011/CsAgent/os/HostDashboard?offset=420&" + ">" + dataJson.data[i].host + "</a>";
				healthRow.append(jQuery('<td/>', {
					 html: hostHref
				}));
				healthRow.append(jQuery('<td/>', {
					 text: dataJson.data[i].createdOn.date
				}));
				healthRow.append(jQuery('<td/>', {
					 text: dataJson.data[i].data.UnHealthyEventCount
				}));
				var errorMessage = "";
				var timeStamp = "";
				for (var j = 0; j < dataJson.data[i].data.healthStatus.length; j++) {
					 if ('Success' == dataJson.data[i].data.healthStatus[j].status[0]) {
						  errorMessage = errorMessage + '<pre> </pre>';
						  timeStamp = timeStamp + '<pre>' + dataJson.data[i].data.healthStatus[j].time + ' <img src="images/Ok.gif" height="12px" width="12px"> </pre>';
					 } else {
						  timeStamp = timeStamp + '<pre>' + dataJson.data[i].data.healthStatus[j].time + ' <img src="images/media-record.png" height="12px" width="12px"> </pre>';
						  errorMessage = errorMessage + '<pre>' + (dataJson.data[i].data.healthStatus[j].status[0]).replace(dataJson.data[i].host + ':', ' ') + '</pre>';
					 }

					 //timeStamp = timeStamp + '<pre>' + dataJson.data[i].data.healthStatus[j].time + '</pre>';
				}
				healthRow.append(jQuery('<td/>', {
					 html: timeStamp

				}));
				healthRow.append(jQuery('<td/>', {
					 html: errorMessage

				}));

				healthBody.append(healthRow);
		  }
		  $("#healthReportsTable").tablesorter({
				sortList: [[0, 0]]
				, theme: 'csapSummary'
				, headers: {
					 // first column
					 1: {
						  sorter: false
					 },
					 3: {
						  sorter: false
					 },
					 4: {
						  sorter: false
					 }
				}
		  });
		  var healthTableId = "#healthReportsTable";
		  $.tablesorter.computeColumnIndex($("tbody tr", healthTableId));
		  //$( healthTableId ).trigger("tablesorter-initialized") ;
		  $(healthTableId).trigger("updateAll");
	 }
});