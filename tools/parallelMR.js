var parallelMR = function(sourceCollectionName, map, reduce ,options, fieldToSplit){
	// *************
	// aux functions
	// *************
	
	var fuseDocuments = db.system.js.findOne({_id:"fuseDocuments"}).value

	var cloneObject = function(modelObject){
		var newObject = {}
		for(var prop in modelObject){
			if(modelObject.hasOwnProperty(prop)){
				newObject[prop] = modelObject[prop]
			}
		}
	
		return newObject
	}

	// 
	var createQueryFromSplitPoints = function(field, splitPoints, S){
		var lowerKeyLimit, upperKeyLimit, thisFilterDoc={};
		thisFilterDoc[field] = null
	
		switch(S){	// change the query document in funcion 
			case 0:
				if(splitPoints.length){
					upperKeyLimit = splitPoints[S]
					thisFilterDoc[field] = {$lt:upperKeyLimit}
				}
				else{ // case when there are no splits
					delete thisFilterDoc[field]
				}
				break;
			case splitPoints.length:
				lowerKeyLimit = splitPoints[S-1]
				thisFilterDoc[field] = {$gte:lowerKeyLimit}
				break;
			default:
				lowerKeyLimit = splitPoints[S-1]
				upperKeyLimit = splitPoints[S]
				thisFilterDoc[field] = {$gte:lowerKeyLimit, $lt:upperKeyLimit}
		}
		print("Filter for "+(S+1)+(S ? "th:" : "st")+" thread")
		printjson(thisFilterDoc)
	
		return thisFilterDoc
	}
	
	// ***********
	// variables
	// ***********
	
	print(new Date() + " | INICIO - parallelMR")
	var testForParallelism = (db.system.js.findOne({_id:"defineScopedThread"}).value)()
	
    // CPU and processing information
	var memMB = db.hostInfo().system.memSizeMB
    var numMRJobs = db.hostInfo().system.numCores	// not the definitive qty of threads
	var successfulThreads = 0
	
    // constants
	var maxChunkSize = Math.floor(memMB*1024*1024 / 4 / numMRJobs)	//  1/4 of total memory only
    var maxObjectdPerChunk = 500000	// limit for the nbr of distinct keys to run un jsMode=true MR's option
	
    // Collection
	var dbName = db.getName()
    var collection = db[sourceCollectionName]
    var nameSpace = dbName+"."+sourceCollectionName
    var keyPattern = {}
    keyPattern[fieldToSplit] = 1
    
    // chunk splitting infomation
    var collSize = collection.stats(1).size
    var collCount = collection.stats(1).count
    var objsPerChunk = collCount / numMRJobs
    var chunkSize = collSize / numMRJobs
	
	// Adjust objsPerChunk (if necessary) in order to run in pure JS mode
	if(objsPerChunk > maxObjectdPerChunk){
		print("Redef objsPerChunk")
		objsPerChunk = maxObjectdPerChunk
		numMRJobs = Math.ceil(collCount / objsPerChunk)
		chunkSize = collSize / numMRJobs
	}
	
	// Adjust chunkSize (if necessary)
    if(chunkSize > maxChunkSize){
		print("Redef chunkSize")
    	chunkSize = maxChunkSize
		numMRJobs = Math.ceil(collSize / chunkSize)
		objsPerChunk = collCount / numMRJobs
    }
	
	// Format "out" object, and decide if a tmp collection has to be created (if action=="replace")
	var createTmpColl = false
	var outCollName
	if(typeof options.out === "string"){	// implicit "replace" action
		createTmpColl = true
		outCollName = options.out
		options.out = {merge: outCollName+"_tmp"}
	}
	else if(options.out.hasOwnProperty("replace")){	// explicit "replace" action
		createTmpColl = true
		outCollName = options.out.replace
		options.out = {merge: outCollName+"_tmp"}
	}
	options.out.db = dbName
	options.jsMode = true
	print("createTmpColl: "+createTmpColl)
	
	// verbosity
	printjson({
		numMRJobs: numMRJobs,
		collSize: collSize,
		collCount: collCount,
		objsPerChunk: objsPerChunk,
		chunkSize: chunkSize
	})
	
	// touch
	//db.runCommand({touch: sourceCollectionName, data:true, index:true})
	
	// Here we decide if run with 1 thread or more
	if(numMRJobs===1 || !testForParallelism){
		print("Running simple MR...")
		
		// avoid side effects 
		if(createTmpColl){
			options.out = {replace: outCollName}
		}
		// Run MR
		printjson(collection.mapReduce(map, reduce, options))
	}
	else{
		print("numMRJobs = "+numMRJobs)
		
		var splitPoints = []
		var splitVector_arg = {
			splitVector: nameSpace,
			keyPattern: keyPattern,
			maxChunkSizeBytes: chunkSize
		}
		
		collection.ensureIndex(keyPattern)
		printjson(splitVector_arg)
		
		// ------------------------
		// Get split points
	    db.runCommand(splitVector_arg).splitKeys.forEach(function(splitKey){
			splitPoints.push(splitKey[fieldToSplit])
		})
		printjson(splitPoints)
		numMRJobs = splitPoints.length+1
		
		printjson({
			numMRJobs: numMRJobs,
			collSize: collSize,
			collCount: collCount,
			objsPerChunk: objsPerChunk,
			chunkSize: chunkSize
		})
		
		// ------------------------
		// function to parallelize
		var mapRed = function(index, scopedArg){
			if(scopedArg){
				var dbName = scopedArg.dbName
				var sourceCollectionName = scopedArg.sourceCollectionName
				var map = scopedArg.map
				var reduce = scopedArg.reduce
				var options = scopedArg.options
				var fieldToSplit = scopedArg.fieldToSplit	//
				var splitPoints = scopedArg.splitPoints
				var numMRJobs = scopedArg.numMRJobs
				var createQueryFromSplitPoints = scopedArg.createQueryFromSplitPoints
				var cloneObject = scopedArg.cloneObject
				var fuseDocuments = scopedArg.fuseDocuments
				
				print("Thread "+(index+1)+"/"+numMRJobs+": start")
			}
			else{
				return 0
			}
			
			db = db.getSiblingDB(dbName)
			collection = db[sourceCollectionName]
			printjson("collName: "+collection)
			
			// we have to clone the "options" object, else other threads will see the changes and this will give unexpected results
			var thisOptions = cloneObject(options)
			thisOptions.query = fuseDocuments(options.query, createQueryFromSplitPoints(fieldToSplit, splitPoints, index))
			
			// perform MR and print results
			var mr_result = collection.mapReduce(map, reduce, thisOptions)
			mr_result.personalizedMsg = new Date() + ": Thread "+(index+1)+"/"+numMRJobs
			printjson(mr_result)
			
			return mr_result.ok
		}
		// .. and its arguments
		var scopedArg = {
			dbName: dbName,
			sourceCollectionName: sourceCollectionName,
			map: map,
			reduce: reduce,
			options: options,
			fieldToSplit: fieldToSplit,
			splitPoints: splitPoints,
			numMRJobs: numMRJobs,
			createQueryFromSplitPoints: createQueryFromSplitPoints,
			cloneObject: cloneObject,
			fuseDocuments: fuseDocuments
		}
		
		// ------------------------
		// create N=numMRJobs threads
		var threads = []
		for(var thrNbr = 0; thrNbr<numMRJobs; thrNbr++){
			var t = new ScopedThread(mapRed, thrNbr, scopedArg);
			threads.push(t);
			t.start();
		}
		
		// ------------------------
		// join threads and print results
		threads.forEach(function(t){
			t.join();
			successfulThreads += t.returnData(); 
		})
		
		print("finished threads: "+successfulThreads)
		
		// only if action=="replace"
		if(createTmpColl){
			// no side effects 
			options.out = {replace: outCollName}
			// ...replacement
			if(successfulThreads===numMRJobs){
				print("Performing replacement: "+outCollName+"_tmp"+" - >"+outCollName)
				db[outCollName+"_tmp"].renameCollection(outCollName,true)
			}
			else{
				print("An error ocurred. Dropping "+outCollName+"_tmp"+" collection.")
				db[outCollName+"_tmp"].drop()
			}
		}
	}
	
	print(new Date() + " | FIN - parallelMR")
}

db.system.js.update({_id:"parallelMR"},{$set:{value:parallelMR}},{upsert:true})