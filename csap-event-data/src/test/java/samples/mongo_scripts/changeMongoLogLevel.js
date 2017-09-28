
//Set profiling level as 2 and slowns as 10
//http://docs.mongodb.org/manual/reference/method/db.setProfilingLevel/
db.setProfilingLevel(2,10)
//Typically for prod
db.setProfilingLevel(1,500)

//Get profiling level
db.getProfilingLevel()