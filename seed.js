db = db.getSiblingDB('beacon');
db.record.createIndex({"sid":1} ,{unique:true}) 
db.record.createIndex({"createdAt":1} ,{unique:false})
db.subRecord.createIndex( { "sid": 1, "index": 1 }, { unique: true } )
db.subRecord.createIndex({"sid":1} ,{unique:false})
db.subRecord.createIndex( {"data.time":1} ,{unique:false})
//new
db.record.createIndex( { pid: "text",tags:"text" } )
db.record.createIndex({"duration":1} ,{unique:false})
db.track.createIndex({"id":1},{"sid":1} ,{unique:true})