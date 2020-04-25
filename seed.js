db.record.createIndex({"sid":1} ,{unique:true}) 
db.record.createIndex({"createdAt":1} ,{unique:false})
db.subRecord.createIndex({"sid":1} ,{unique:false})