var fs = require('fs');
var streamConcat = require('stream-concat');
var AWS = require("aws-sdk");

AWS.config.update({
	accessKeyId: 'Access Key ID',
	secretAccessKey: 'Secret Access Key',
	region: 'Region'
});

var DataNode = {};

var blockReport = {};

DataNode.deleteFileFromBlockReport = function(fileName) {
	if (blockReport.hasOwnProperty(fileName)) {
		delete blockReport[fileName];
	}
}

DataNode.read = function(file) {
	fs.readFile(file, { encoding: 'utf-8' }, function(err, data) {
		if (!err) {
			console.log(data);
		}
		else {
			console.log(err);
		}
	});
}

DataNode.removeFile = function(fileName) {
	if (fs.existsSync(fileName)) {
		fs.readdirSync(fileName).forEach(function(file, index) {
			var curPath = fileName + "/" + file;
			if (fs.lstatSync(curPath).isDirectory()) { // recurse
				DataNode.removeFile(curPath);
			}
			else { // delete file
				fs.unlinkSync(curPath);
				DataNode.removeFromReport(fileName, file);
			}
		});
		fs.rmdirSync(fileName);
		return true;
	}
	return false;
}

DataNode.addToReport = function(fileName, dataBlock) {
	if (!blockReport.hasOwnProperty(fileName)) {
		blockReport[fileName] = [];
	}
	blockReport[fileName].push(dataBlock);
	fs.appendFile("blockReport.txt", dataBlock + "\n", function(err) {
		if (err) {
			return console.log(err);
		}
		console.log("blockReport.txt updated.");
	});
}

DataNode.checkFileExists = function(blockName, fileName) {
	console.log("blockName:",  blockName);
	console.log("fileName:",  fileName);
	if(blockReport.hasOwnProperty(fileName)){
		console.log("I have ", fileName);
		if (blockReport[fileName].indexOf(blockName) === -1) {
			console.log("I don't have ", blockName);
			return false;
		}
		else {
			console.log("I have ", blockName)
			return true;
		}
	} 
	console.log("I don't have ", fileName);
	return false;
}

DataNode.removeFromReport = function(fileName, dataBlock) {
	if (blockReport[fileName].indexOf(dataBlock) === -1) {
		var blockIndex = blockReport[fileName].indexOf(dataBlock);
		blockReport[fileName].splice(blockIndex, 1);
	}
	DataNode.deleteFromReportFile(dataBlock);
}

DataNode.deleteFromReportFile = function(block) {
	fs.readFile("blockReport.txt", { encoding: 'utf-8' }, function(err, data) {
		if (err) {
			return console.log(err);
		}
		console.log("report read for deletion.");
		var resultRaw = data.replace(block, "");
		var result = resultRaw.replace(/^\n/gm, "")
		fs.writeFile("blockReport.txt", result, { encoding: 'utf-8' }, function(err) {
			if (err) {
				return console.log(err);
			}
			console.log("block deleted.");
		});
	});
}

DataNode.getReport = function() {
	return blockReport;
}

DataNode.makeDir = function(dirPath) {
	try {
		fs.mkdirSync(dirPath);
	}
	catch (err) {
		if (err.code !== 'EEXIST') throw err
	}
}

DataNode.deleteFile = function(fileName) {
	fs.unlink(fileName, function(err) {
		if (err) {
			return console.log(err);
		}
		console.log("The file has been deleted.");
	});
}

// takes in an array of streams and combines them into a single
// concatenated write stream that outputs to a file. Can be changed
// to a read stream.
DataNode.concatStream = function(streamArray) {
	var output = fs.createWriteStream('combined.txt');
	var combinedStream = new streamConcat(streamArray);
	combinedStream.pipe(output);
}

module.exports = DataNode;
