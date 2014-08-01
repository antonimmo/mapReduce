
var getLevenshteinFn = function(){
	
	var newLevenshteinMatrix = function(rows, columns){
		var matrix = []
		for(var i=0; i<rows; i++){
			matrix[i] = []
			matrix[i][0] = i	// 0th column filling
		
			if(i){
				for(var j=1; j<columns; j++){
					matrix[i][j] = 0
				}
			}
			else{	// i===0
				for(var j=1; j<columns; j++){
					matrix[0][j] = j	// 0th row filling
				}
			}
		}
	
		return matrix
	}
	
	return function(a,b){
	
		var lenA = a.length
		var lenB = b.length
	
		var matrixD = newLevenshteinMatrix(lenA+1, lenB+1)
	
		for(var j=1; j<=lenB; j++){
			for(var i=1; i<=lenA; i++){
				matrixD[i][j] = ( a[i-1] === b[j-1] ) ? matrixD[i-1][j-1] : Math.min(
					matrixD[i-1][j]+1,		// deletion
					matrixD[i][j-1]+1,		// insertion
					matrixD[i-1][j-1]+1)	// substitution
			}
		}

		return matrixD[lenA][lenB]
	}
	
}

var mapping1 = {
	NameFirst: {
		metric: getLevenshteinFn(),
		weight: 0.5
	},
	NameLast:{
		metric: getLevenshteinFn(),
		weight: 0.5
	}
}

var map1 = function(){
	for(prop in this){
		if(mapping1.hasOwnProperty(prop) && this.hasOwnProperty(prop)){
			emit(prop, {ID: this._id, value: this[prop]})
		}
	}
}

var reduce1 = function(key, values){
	
	
}



