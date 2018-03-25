var express = require('express');
var app = express();
var server = require("http").Server(app);
var io = require("socket.io")(server);
var dgram = require('dgram');
var serverHeartbeat = dgram.createSocket('udp4');
var port = Number(8081);
var fs = require('fs');
var nameNode = require('./nameNode.js');
var createDataNode = require('./createDataNode.js')

app.get('/', function(req, res) {
    res.send('Hello, from NameNode!');
});

nameNode.aliveDataNodes = {};

server.listen(port);
console.log("Server Running on:", port);

// block size is 64 MB
var blockSize = 67108864;

// replicationFactor
var replicationFactor = 2;

var receivedFileSize = 0;

var nameNodeData = {
    "fileName": "",
    "sizeToSplit": "",
    "blockSize": "",
    "numDataNodes": "",
    "blockToDataNodes": "",
    "dataNodes": []
}

var DATANODE_HEARTBEAT_TRESHOLD = 3;

//variables needed for getDataByRange to work
var byteStart = 0; //no point in changing this if you want to start from beginning
var byteStop = blockSize; //change this for first split end only
var interval = blockSize; //change this for split size

io.on('connection', function(socket) {
    console.log("Received Connection");

    socket.on("fileSize", function(data) {
        receivedFileSize = Number(data.fileSize);
        nameNodeData['fileName'] = data.fileName;
        var numOfBlocks = Math.ceil(receivedFileSize / blockSize);
        var blockInfo = {};

        for (var i = 0; i < numOfBlocks * replicationFactor; i++) {
            if (i % numOfBlocks === 0) {
                var byteStart = 0;
                var byteStop = blockSize;
            }

            var blockName = nameNodeData['fileName'] + ".part-" + (i % numOfBlocks);
            blockInfo[i] = {};
            blockInfo[i]['key'] = blockName;
            blockInfo[i]['ipValue'] = nameNodeData['dataNodes'][i % nameNodeData['numDataNodes']];
            blockInfo[i]['byteStart'] = byteStart;
            blockInfo[i]['byteStop'] = byteStop;

            byteStart = byteStop + 1;
            if ((byteStop + interval) > receivedFileSize) {
                byteStop = receivedFileSize;
            }
            else {
                byteStop = byteStop + interval;
            }
        }

        nameNodeData['sizeToSplit'] = numOfBlocks;
        nameNodeData['blockSize'] = blockSize;
        nameNodeData['blockToDataNodes'] = blockInfo;
    });

    socket.emit('nameNodeData', nameNodeData);

    socket.on('getMetaData', function(data) {
        var metaData = nameNode.getMetaData();
        console.log(data.fileName);
        var fileData = metaData[data.fileName];
        console.log(metaData);
        socket.emit('sendFileData', metaData);
    });

    socket.on('fileDirectoryPath', function(data) {
        nameNode.makeDir(data);
        socket.emit('directoryCreated');
    });

    socket.on('createFileInDirectory', function(data) {
        //var path = data;
        nameNode.makeFile(data);
        socket.emit('directoryFileCreated');
    });

    socket.on('listDirectoryContents', function(data) {
        var returnArray = nameNode.dirContents(data);
        socket.emit('sendingDirectoryContent', returnArray);
    });

    socket.on('deleteDirectory', function(data) {
        nameNode.deleteDir(data);
        socket.emit('directoryDeleted');
    });
    
    socket.on('removeFileFromMetaData', function(data) { 
       nameNode.deleteFileFromMetaData(data);
       socket.emit('removedFile');
    });

    socket.on('removeFile', function(data) {
        nameNode.deleteFile(data);
        socket.emit('fileDeleted');
    });

    socket.on('getIPData', function(data) {
        socket.emit('receiveDNInfo', nameNodeData.dataNodes);
    });

});

/*
 * LISTENING FOR HEARTBEAT MESSAGE 
 */
serverHeartbeat.on("message", function(msg, rinfo) {
    //code here for attempting to write msg into a file. TOF
    console.log("NameNode got block report from " + rinfo.address);
    var output = fs.createWriteStream(rinfo.address);
    var blockReport = JSON.parse(msg.toString());
    console.log("Block report: ", blockReport);
    for (var i in blockReport) {
        var fileName = i;
        var blockList = blockReport[fileName];
        nameNode.appendBlockReport(blockList, rinfo.address, fileName);
    }

    // if we receive a new datanode ip address, then add it into our arrays of 
    // available datanodes

    if (nameNodeData['dataNodes'].indexOf(rinfo.address) === -1) {
        nameNodeData['dataNodes'].push(rinfo.address);
        nameNode.aliveDataNodes[rinfo.address] = 0;
    }
    else {
        nameNode.aliveDataNodes[rinfo.address] += 1;
    }

    nameNodeData['numDataNodes'] = nameNodeData['dataNodes'].length;
});

serverHeartbeat.on("listening", function() {
    var address = server.address();
    console.log("server listening " +
        address.address + ":" + address.port);
});

serverHeartbeat.bind(port);

/*
 * DETECTING DATANODE FAILURES
 */
var beat_period = 5;
var heartbeatCounter = {};
var heartBeatChecker = {};

// Snapshot of alive datanodes
setInterval(function() {
    for (var i in nameNode.aliveDataNodes) {
        heartbeatCounter[i] = nameNode.aliveDataNodes[i];
    }
}, 15 * 1000);

//Check available datanodes every 5 seconds
setInterval(function() {
    // we keep track of each datanodes that we have stored. If we don't 
    // receive a heartbeat from our datanodes, then their counter will be static.
    // Once they reached our treshold, then we will declare it dead and remove it
    // from our available datanodes list.

    var isDataNodeFailure = false;

    console.log("Heartbeat Checker: " + JSON.stringify(heartBeatChecker));
    console.log("Heartbeat Counter: " + JSON.stringify(heartbeatCounter));

    for (var datanodeIPs in nameNode.aliveDataNodes) {
        if (heartbeatCounter[datanodeIPs] < nameNode.aliveDataNodes[datanodeIPs]) {
            heartBeatChecker[datanodeIPs] = 0;
        }
        else if (heartbeatCounter[datanodeIPs] == nameNode.aliveDataNodes[datanodeIPs]) {
            if (!heartBeatChecker.hasOwnProperty(datanodeIPs)) {
                heartBeatChecker[datanodeIPs] = 0;
            }
            else {
                heartBeatChecker[datanodeIPs] += 1;
            }
            if (heartBeatChecker[datanodeIPs] == DATANODE_HEARTBEAT_TRESHOLD) {
                console.log("Datanode with IP: " + datanodeIPs + " is dead");
                var indx = nameNodeData['dataNodes'].indexOf(datanodeIPs);
                nameNodeData['dataNodes'].splice(indx, 1);

                delete nameNode.aliveDataNodes[datanodeIPs];
                delete heartbeatCounter[datanodeIPs];
                delete heartBeatChecker[datanodeIPs];

                nameNode.updateMetaDataOnDataNodeFailures(datanodeIPs, nameNode.aliveDataNodes);
                var missingBlocks = nameNode.getMissingBlocks();
                isDataNodeFailure = true;
            }
        }
    }

    var clientIO = require("socket.io-client");
    var clientSocket = clientIO.connect("http://172.31.3.75:8082");

    if (isDataNodeFailure) {
        console.log("Hey, I'm here!");
        sendMissingBlocksToClient(missingBlocks);
        isDataNodeFailure = false;
        // closeClientConnection();
    }
    
    //sendMissingBlocksToClient(missingBlocks);

    /*
     * Acts as a client to connect with namenode
     */
    function sendMissingBlocksToClient(missingBlocks) {
        var blocks = JSON.stringify(missingBlocks);
        clientSocket.on('connect', function() {
            clientSocket.emit("missingBlocks", missingBlocks);
        });
    }

    function closeClientConnection() {
        clientSocket.disconnect();
    }

    // console.log("Alive datanodes: " + JSON.stringify(nameNode.aliveDataNodes));
}, beat_period * 1000);


