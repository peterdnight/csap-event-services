//Creating user . Run it on admin data base
db.createUser({user: "dataBaseReadWriteUser",pwd: "password",roles: [{ role: "readWriteAnyDatabase", db: "admin" },{ role:"clusterAdmin", db: "admin" },{role: "userAdminAnyDatabase",db: "admin"}]})
//Grant role to existing user. Run it on admin data base
//dbAdminAnyDatabase role is required for running profile level and such
db.grantRolesToUser( "dataBaseReadWriteUser", [ {role: "dbAdminAnyDatabase",db: "admin"} ])
//replica set status
rs.status()
//Should run this before running anything when connected to secondary
rs.slaveOk()
//Force primary to become secondary
rs.stepDown()
//find any one document
db.eventRecords.findOne()
//Find document matching particular category. Exclude category and id from result. Limit 10 documents
db.eventRecords.find({"category":"/csap/reports/process/daily"},{"category":0,"_id":0}).limit(10)
//Count document matching particular category
db.eventRecords.find({"category":"/csap/reports/process/daily"}).count()
//Count all documents
db.eventRecords.count()
//Find all disticnt categories
db.eventRecords.distinct("category")
//Get db stats
db.stats()
//Get collection stats
db.eventRecords.stats()
//Get indexes on particular collection
db.eventRecords.getIndexes()
//take back up of whole data base
//mongodump -u dataBaseReadWriteUser  -p password --authenticationDatabase admin -d event -c eventRecords
//restore db. Run from where it is backedup
//mongorestore -u dataBaseReadWriteUser  -p password password --authenticationDatabase admin
//Mongo export with query
//mongoexport --host csap-dev01 -c eventRecords -q "{\"createdOn.date\":{\$gt:\"2015-03-01\"}}" -u dataBaseReadWriteUser -p password --authenticationDatabase admin -d event --out events.json
//import json into db
//mongoimport -h csap-dev01 -u dataBaseReadWriteUser -p password --authenticationDatabase admin -d event -c eventRecords --file events.json