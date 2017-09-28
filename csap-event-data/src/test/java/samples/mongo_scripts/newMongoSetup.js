db = connect("localhost:27017/admin");
//This is required as data and analytics project uses old authentication schmea
db.system.version.insert({ "_id" : "authSchema", "currentVersion" : 3 });
db.createUser({user: "dataBaseReadWriteUser",pwd: "password",roles: [{ role: "readWriteAnyDatabase", db: "admin" },{ role:"clusterAdmin", db: "admin" },{role: "userAdminAnyDatabase",db: "admin"}]})
db.auth("dataBaseReadWriteUser","password");
db = db.getSiblingDB("metricsDb");
//creates capped collection with size specified
db.createCollection( "metrics", { capped: true, size: 350000000 } );
//creates index with specified values. background true helps in not stopping all other operations
db.metrics.createIndex({"attributes.hostName":1,"attributes.id":1,"createdOn.lastUpdatedOn":-1},{ background: true });
db.metrics.createIndex({"attributes.hostName":1,"createdOn.date":1},{ background: true });
db.metrics.createIndex({"createdOn.date":1},{ background: true });

db.createCollection("metricsAttributes");
db.metricsAttributes.ensureIndex({"hostName": 1, "id": 1, "createdOn.lastUpdatedOn": 1}, {"background": true});


db = db.getSiblingDB("event");
db.createCollection("eventRecords");
db.eventRecords.ensureIndex({"lifecycle":1});
db.eventRecords.ensureIndex({"summary":1});
db.eventRecords.ensureIndex({"appId":1});
db.eventRecords.ensureIndex({"project":1});
db.eventRecords.ensureIndex({"host":1});
db.eventRecords.ensureIndex({"category":1});

// index for typical user history https://docs.mongodb.org/manual/core/index-compound/
db.eventRecords.ensureIndex({"metaData.uiUser":1,"createdOn.date":-1}, {"background": true});

db.eventRecords.ensureIndex({"metaData.uiUser":1});
db.eventRecords.ensureIndex({"createdOn.lastUpdatedOn":-1});
db.eventRecords.ensureIndex({"appId":1,"lifecycle":1});
db.eventRecords.ensureIndex({"category":1,"summary":1,"metaData.uiUser":1});
db.eventRecords.ensureIndex({"appId":1,"createdOn.mongoDate":-1});
//expireAfterSeconds 0 means it will expire at the time specified by the expiresAt attribute
db.eventRecords.createIndex( { "expireAt": 1 }, { expireAfterSeconds: 0 } );
db.eventRecords.createIndex({"appId":1,"project":1,"lifecycle":1,"createdOn.lastUpdatedOn":-1},{"background": true})
db.eventRecords.createIndex({"appId":1,"project":1,"lifecycle":1,"data.summary.serviceName":1},{"background": true})
db.eventRecords.ensureIndex({"createdOn.date":-1},{"background": true});


// index for trending
db.eventRecords.createIndex({"appId":1,"project":1,"lifecycle":1,"category":1,"createdOn.date":-1},{"background": true})


// index for event browser search dialpg
db.eventRecords.createIndex({"appId":1,"lifecycle":1,"createdOn.date":-1},{"background": true})
 