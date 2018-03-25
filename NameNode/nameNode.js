var express = require('express');
var path = require('path');
var fs = require('fs');
var http = require('http');
var app = express();
var port = Number(8081);

app.get('/', function(req, res) {});

var port = Number(process.env.PORT || 5000);
app.listen(port);


var nameNode = {};

var aliveDataNodes = {};

var nameNodeData = {};

var missingBlocks = {};

var nameNodeMetaData = {
    // "priceVolume.zip":{
    //      "blocks":{
    //          "priceVolume.zip.part-0": ["172.31.36.200", "172.31.36.100", "172.31.36.300"],
    //          "priceVolume.zip.part-1": [],
    //          "priceVolume.zip.part-2": [],
    //          "priceVolume.zip.part-3": [],
    //          }
    //      "filePath":"",
    //      "fileSize":""}
    // }
};

//Add new Node:IP entry to the nameNodeData
nameNode.addNewDNEntry = function(nodeName, nodeIP, nameNodeData) {
    nameNodeData[nodeName] = nodeIP;
    return nameNodeData;
}

//Add info for a new file into nameNodeMetaData
nameNode.addNewFileEntry = function(nodeMetaData, fileName, blockList, fileSize) {
    nameNodeMetaData['files'][fileName] = {};
    nameNodeMetaData['files'][fileName]['blocks'] = blockList;
    for (var i = 0; i < blockList.length; i++) {
        nameNodeMetaData['files'][fileName]['blocks'][blockList[i]] = [];
    }
    nameNodeMetaData['files'][fileName]['fileSize'] = fileSize;
    nameNode.writeBackup(nameNodeMetaData);
    return nameNodeMetaData;
}

//get the IPs of the nodes that contain the various data blocks
nameNode.getBlockIPs = function(nodeMetaData, fileName, blockname) {
    return nameNodeMetaData['files'][fileName]['blocks'][blockname];
}

//parses block report file and appends it to the metadata. Only goes through a single fileName
//so a must be called in a for loop with different filenames each time.
nameNode.appendBlockReport = function(blockArray, reportIP, fileName) {
    if (!nameNodeMetaData.hasOwnProperty(fileName)) {
        nameNodeMetaData[fileName] = {};
        nameNodeMetaData[fileName]['blocks'] = {};
        nameNodeMetaData[fileName]['filePath'] = '';
        nameNodeMetaData[fileName]['fileSize'] = '';
    }
    for (var i = 0; i < blockArray.length; i++) {
        if (!nameNodeMetaData[fileName]['blocks'].hasOwnProperty(blockArray[i])) {
            nameNodeMetaData[fileName]['blocks'][blockArray[i]] = [];
        }
        if (nameNodeMetaData[fileName]['blocks'][blockArray[i]].indexOf(reportIP) == -1) {
            nameNodeMetaData[fileName]['blocks'][blockArray[i]].push(reportIP);
        }
    }
    console.log("Metadata: ", JSON.stringify(nameNodeMetaData));
    nameNode.writeBackup(nameNodeMetaData);

}


nameNode.deleteFileFromMetaData = function(fileName) {
    if (nameNodeMetaData.hasOwnProperty(fileName)) {
        delete nameNodeMetaData[fileName];
    }
    nameNode.writeBackup(nameNodeMetaData);
}

nameNode.getAliveDataNodes = function() {
    return aliveDataNodes;
}


nameNode.getMetaData = function() {
    return nameNodeMetaData;
}

// remove dead datanode ips
nameNode.updateMetaDataOnDataNodeFailures = function(deadIP, availableDataNodes) {
    console.log("Dead ip:", deadIP);
    missingBlocks = {};
    for (var fileName in nameNodeMetaData) {
        for (var blockArray in nameNodeMetaData[fileName]['blocks']) {
            if (nameNodeMetaData[fileName]['blocks'][blockArray].indexOf(deadIP) !== -1) {
                var indx = nameNodeMetaData[fileName]['blocks'][blockArray].indexOf(deadIP);
                nameNodeMetaData[fileName]['blocks'][blockArray].splice(indx, 1);
            }

            console.log("Meta data: " + JSON.stringify(nameNodeMetaData));
            // nameNodeMetaData[fileName]['blocks'][blockArray] -> this gives you block name
            // this needs to be replicated in datanodes that does not have it

            console.log("Alive datanodes: ", availableDataNodes);
            for (var availableIPs in availableDataNodes) {
                console.log("Available IP: ", availableIPs);
                if (nameNodeMetaData[fileName]['blocks'][blockArray].indexOf(availableIPs) === -1) {
                    if(!missingBlocks.hasOwnProperty(fileName)) { 
                        missingBlocks[fileName]={};
                    }
                    if(!missingBlocks[fileName].hasOwnProperty(blockArray)) { 
                        missingBlocks[fileName][blockArray] = {};
                    }
                    missingBlocks[fileName][blockArray]['IP'] = availableIPs
                }
            }
        }
    }
    nameNode.writeBackup(nameNodeMetaData);
}

nameNode.getMissingBlocks = function() {
    return missingBlocks;
}

//parses metadata and removes the ip in parameter from any blocks that have it. Only goes through a single fileName
//so a must be called in a for loop with different filenames each time.
nameNode.replaceBlockReport = function(blockArray, reportIP, fileName, newIP) {
    if (nameNodeMetaData.hasOwnProperty(fileName)) {
        for (var j = 0; j < blockArray.length; j++) {
            for (var k in nameNodeMetaData[fileName]['blocks']) {
                if (nameNodeMetaData[fileName]['blocks'][k].indexOf(reportIP) != -1) {
                    var targetIndex = nameNodeMetaData[fileName]['blocks'][k].indexOf(reportIP);
                    nameNodeMetaData[fileName]['blocks'][k].splice(targetIndex, 1, newIP);
                }
            }
        }
        nameNode.writeBackup(nameNodeMetaData);
    }
    else
        console.log("Error. " + fileName + " does not exist in metaData. Remove failed.");
}

//adds an empty directory
nameNode.makeDir = function(dirPath) {
    try {
        var path = '/home/ec2-user/environment';
        path += dirPath;
        fs.mkdirSync(path);
    }
    catch (err) {
        if (err.code !== 'EEXIST')
            throw err;
        else
            console.log("File Already Exists. Creation failed.");
    }
}

//removes an empty directory
nameNode.deleteDir = function(dirPath) {
    try {
        var path = '/home/ec2-user/environment';
        path += dirPath;
        fs.rmdir(path);
    }
    catch (err) {
        if (err.code !== 'ENOTEMPTY')
            throw err;
        else
            console.log("Directory not empty. Deletion failed.");
    }
}

//reads out the contents within the specified directory.
nameNode.dirContents = function(dirPath) {
    var fileArray = [];
    var path = '/home/ec2-user/environment';
    path += dirPath;
    fs.readdirSync(path).forEach(file => {
        console.log(file);
        fileArray.push(file);
    })
    return fileArray;
}


nameNode.makeFile = function(filePath) {
    var path = '/home/ec2-user/environment';
    path += filePath;
    fs.writeFile(path, "", function(err) {
        if (err) {
            return console.log(err);
        }
        console.log("The file was saved!");
    });
}


nameNode.writeBackup = function(metaData) {
    var jsonString = JSON.stringify(metaData);
    fs.writeFile('nameNodebackup.txt', jsonString, function(err) {
        if (err) {
            return console.log(err);
        }
        console.log("The file was saved!");
    });
}


nameNode.readBackup = function() {
    fs.readFile('nameNodebackup.txt', { encoding: 'utf-8' }, function(err, data) {
        if (!err) {
            nameNodeMetaData = JSON.parse(data);
            console.log("Backup read and loaded: ", nameNodeData);
        }
        else {
            console.log(err);
        }
    });
    return nameNodeMetaData;
}


nameNode.deleteFile = function(filePath) {
    var path = '/home/ec2-user/environment';
    path += filePath;
    fs.unlink(path, function(err) {
        if (err) {
            return console.log(err);
        }
        console.log("The file has been deleted.");
    });
}



module.exports = nameNode;
//nameNode.writeBackup();
//nameNode.readBackup();
//console.log(nameNodeData);
//nameNode.dirContents('namenode');
//var blockArray = ['block 2', 'block 3', 'block 4'];
//nameNode.initMetaData(nameNodeMetaData);
//console.log(nameNode.addNewFileEntry(nameNodeMetaData,'file1', blockArray, "275"));
// nameNode.appendBlockReport(nameNodeMetaData, '172.31.37.120', 'file1');

// 172.31.27.120 is just a test value. It is NOT an actual IP address being used!

//nameNode.appendBlockReport(nameNodeMetaData, '172.31.37.120', 'file1');
//nameNode.removeBlockReport(nameNodeMetaData, '172.31.37.120', 'file1');
//console.log(nameNodeMetaData['files']['file1']['blocks']['block 3'][0]);
//console.log(nameNode.getFileBlocks(nameNodeMetaData, 'file1'));
