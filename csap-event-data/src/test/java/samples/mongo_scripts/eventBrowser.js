

// Get Users and hosts for UI selection and filtering
db.eventRecords.distinct(
		"host",
		{ appId: "csapssp.gen", lifecycle: "dev", 'createdOn.date': { $gte: "2016-08-16", $lte: "2016-08-31" } }
)
db.eventRecords.distinct(
		"host",
		{ appId: "csapeng.gen", lifecycle: "dev", 'createdOn.date': { $gte: "2016-08-16", $lte: "2016-08-31" } }
)


db.runCommand(
		{ distinct: "eventRecords",
			key: "host",
			query: {
				appId: "csapeng.gen", lifecycle: "dev", 'createdOn.date': { $gte: "2016-08-16", $lte: "2016-08-31" }
			}
		}
)


// USERS
db.eventRecords.distinct(
		"metaData.uiUser",
		{
			appId: "csapssp.gen", lifecycle: "prod",
			'createdOn.date': { $gte: "2016-08-16", $lte: "2016-08-31" }
		}
)
// .explain("executionStats") ;

db.eventRecords.distinct(
		"metaData.uiUser", { appId: "csapeng.gen", lifecycle: "dev" }
)

// query plan
db.runCommand( { distinct: "eventRecords", key: "metaData.uiUser", query: { appId: "csapeng.gen", lifecycle: "dev" } } )

db.runCommand(
		{ distinct: "eventRecords",
			key: "metaData.uiUser",
			query: {
				appId: "csapeng.gen", lifecycle: "dev", 'createdOn.date': { $gte: "2016-08-16", $lte: "2016-08-31" }
			}
		}
)


// Search counts
// command event.eventRecords command: count { count: "eventRecords", query: { category: /^/csap/ui//i, lifecycle: "dev", appId: "csapssp.gen", project: "SNTC and PSS", createdOn.date: { $gte: "2016-09-02", $lt: "2016-09-10" } }, maxTimeMS: 20000 } planSummary: IXSCAN { appId: 1.0, project: 1.0, lifecycle: 1.0, createdOn.lastUpdatedOn: -1.0 } keyUpdates:0 writeConflicts:0 exception: operation exceeded time limit code:50 numYields:5513 reslen:89 locks:{ Global: { acquireCount: { r: 11028 }, acquireWaitCount: { r: 86 }, timeAcquiringMicros: { r: 72571 } }, Database: { acquireCount: { r: 5514 } }, Collection: { acquireCount: { r: 5514 } } } protocol:op_query 20147ms

db.runCommand(
        { count: "eventRecords",
            query: {
                appId: "csapeng.gen", 
                lifecycle: "prod", 
                'createdOn.date': { 
                    $gte: "2016-08-16", 
                    $lte: "2016-08-31" 
                },
                category:  /^\/csap\/ui/
            }
        }
)

db.runCommand(
        { count: "eventRecords",
            query: {
                appId: "csapssp.gen", 
                lifecycle: "dev", 
				project: "SNTC and PSS",
                'createdOn.date': { 
                    $gte: "2016-08-16", 
                    $lte: "2016-08-31" 
                },
                category:  /^\/csap\/ui/
            }
        }
)