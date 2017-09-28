//Explain the query showing which query plan it is trying to use
db.eventRecords.explain().count({"createdOn.date":{$gte:"2016-02-07",$lt:"2016-02-08"},"appId":"csapssp.gen"})
//This is not able to make use of index and very slow op
db.eventRecords.explain("executionStats").count({"createdOn.date":{$gte:"2016-02-07",$lt:"2016-02-08"},"appId":"csapssp.gen"})
//Only date will make use of index and faster
db.eventRecords.explain("executionStats").count({"createdOn.date":{$gte:"2016-02-07",$lt:"2016-02-08"}})
//this makes use of index and faster than other count queries
db.eventRecords.aggregate([{$match:{"createdOn.date":{$gte:"2016-02-01",$lt:"2016:02-08"}}},{ $group: { _id: { "project" : "$project" , "lifecycle" : "$lifecycle"}, count: { $sum: 1 } }} ])


// query samples
db.eventRecords.find({"createdOn.date":{$gte:"2016-03-07",$lt:"2016-03-08"}})
db.metrics.find({"createdOn.date":{$gte:"2016-03-07",$lt:"2016-03-08"}})



//
db.eventRecords
db.eventRecords.distinct(  "host",  { appId: "csapssp.gen", lifecycle: "dev" } )

db.eventRecords.distinct(  "metaData.uiUser",  { appId: "csapssp.gen", lifecycle: "prod" } )

