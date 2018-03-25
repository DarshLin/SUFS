var express = require('express');
var router = express.Router();
var fs = require("fs");
var readline = require("readline");
var stream = require("stream");
var request = require('request');
var async = require("async");
var http = require("http");
var streamConcat = require('stream-concat');
var app = express();
var server = require("http").Server(app);

var NAMENODE_PORT = 8081;
var NAMENODE_IP = 'IP';
var NAMENODE_ADDRESS = 'http://' + NAMENODE_IP + ':' + NAMENODE_PORT;

var AWS = require("aws-sdk");
AWS.config.update({ region: 'us-west-2' });

/*
 * S3 Configurations (will not be configurable)
 */
var s3 = new AWS.S3({
    apiVersion: '2006-03-01',
    signatureVersion: 'v4',
    accessKeyId: 'Access Key ID',
    secretAccessKey: 'Secret Access Key',
    region: 'region'
});

var bucketParams = {
    Bucket: "Bucket",
    Key: "Key",
};

var paramsWithRange = {
    Bucket: bucketParams.Bucket,
    Key: bucketParams.Key,
};

//content size of file
var fileInfo = {};

router.get('/getData', function(req, res) {

    var io = req.app.get('socketio');
    var socket = io.connect(NAMENODE_ADDRESS, { reconnect: true });
    var filePath = req.param('filePath');

    
    s3.headObject(bucketParams, function(err, data) {
        if (err) {
            console.log(err, err.stack);
            res.json('Could not read file');
        }
        else {
            fileInfo['fileName'] = bucketParams.Key;
            fileInfo['fileSize'] = data.ContentLength;
            fileInfo['filePath'] = data.filePath;
        }

        //emit that data back to NameNode
        socket.emit('fileSize', fileInfo);
        res.json('Finished getting data from NameNode');
    });
});

router.get('/writeDataFromS3', function(req, res) {
    //TODO: takes two argument, the s3 file and the desination path
    //Bucket name and key will also be specified
})

router.get('/writeData', function(req, res) {
    var ss = require("socket.io-stream");
    var io = req.app.get('socketio');
    var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });

    var nameNodeData = {};

    async.waterfall([
        checkForDataNodes,
        readFileFromS3
    ], function(err, result) {
        if (err !== null) {
            res.json(err);
        }
        else {
            nameNodeSocket.disconnect();
            res.json("Finished Writing to DN's");
        }
    });

    function checkForDataNodes(cb) {
        nameNodeSocket.on('nameNodeData', function(data) {
            nameNodeData = data;
            console.log(nameNodeData);
            cb(null, nameNodeData);
        });
        if (nameNodeData.numDataNodes === 0) {
            cb("No datanodes found");
        }
    }

    function readFileFromS3(nameNodeData, cb) {
        for (var i in nameNodeData['blockToDataNodes']) {
            var IP = nameNodeData['blockToDataNodes'][i]['ipValue'];
            var dataNodeSocket = io.connect('http://' + IP + ":5000");

            var byteStartRange = nameNodeData['blockToDataNodes'][i]['byteStart'];
            var byteStopRange = nameNodeData['blockToDataNodes'][i]['byteStop'];
            paramsWithRange['Range'] = "bytes=" + byteStartRange.toString() + "-" + byteStopRange.toString();

            var getFileName = nameNodeData['blockToDataNodes'][i]['key'].split('/');
            var fileData = {
                'mainFile': paramsWithRange['Key'].split('/')[1],
                'bucketName': bucketParams.Bucket,
                'bucketKey': bucketParams.Key,
                'blockName': getFileName[1],
                'contentRange': paramsWithRange['Range']
            };

            console.log("fileData: " + JSON.stringify(fileData));

            dataNodeSocket.emit('sendFileInformation', fileData);
        }
        cb(null);
    }
});

router.get('/listDataNodes', function(req, res) {
    var fileName = req.param('filePath');
    var fileInfo = {};
    fileInfo['fileName'] = fileName;
    var io = req.app.get('socketio');
    var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });

    nameNodeSocket.emit('getMetaData', fileInfo);
    nameNodeSocket.on('sendFileData', function(data) {
        if (data.hasOwnProperty(fileName)) {
            for (var k in data[fileName]['blocks']) {
                console.log(k, ": ", data[fileName]['blocks'][k]);
            }
        }
        else {
            res.json('File Does not Exist');
        }
        res.json('Successful');
    });
});

router.get('/readFile', function(req, res) {
    var fileName = req.param('filePath');
    var fileInfo = {};
    fileInfo['fileName'] = fileName;

    var requestInfo = {};
    requestInfo['fileName'] = fileName;

    var io = req.app.get('socketio');
    var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });

    nameNodeSocket.emit('getMetaData', fileInfo);

    nameNodeSocket.on('sendFileData', function(data) {
        //var ss = require("socket.io-stream");
        if (!data[fileName]) {
            res.json('File does not exist');
        }
        else {
            var fileMetaData = data[fileName];
            requestInfo['blockName'] = fileMetaData['blocks'];
            nameNodeSocket.disconnect();

            for (var i in requestInfo['blockName']) {
                var blockName = i;
                var IP = requestInfo['blockName'][i][0];
                var fileData = JSON.stringify({
                    blockName: blockName,
                    fileName: requestInfo['fileName']
                });

                makeRequest(fileData, IP);

            }
            res.json(fileName);
        }

        function makeRequest(data, IP) {
            var options = {
                host: IP,
                port: 5000,
                path: '/read',
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            };

            var req = http.request(options, function(res) {

                //var data = [];
                //var writeStream = fs.createWriteStream(fileName, { "flags": 'a' });
                res.pipe(fs.createWriteStream(fileName, { "flags": 'a' }));
            });

            req.write(data); //refers to POST data, not the above code
            req.end();
        }

    });

});

//Client command to create a directory/ sub directory
router.get('/makeDir', function(req, res) {
    // TODO: mkdir /foo creates foo. mkdir /foo/bar creates bar in foo
    // An idea is to create a separate data structure in namenode to store just
    // directory and the content of directory
    var path = req.param('filePath');
    var io = req.app.get('socketio');
    var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });

    nameNodeSocket.on('connect', function() {
        nameNodeSocket.emit('fileDirectoryPath', path);
        res.json(path);
    });

    nameNodeSocket.on('directoryCreated', function() {
        nameNodeSocket.disconnect();
    });

});


//Client command to create a file in directory
router.get('/createFileInDir', function(req, res) {
    var pathtoFile = req.param('filePath');
    var io = req.app.get('socketio');
    var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });

    nameNodeSocket.on('connect', function() {
        nameNodeSocket.emit('createFileInDirectory', pathtoFile);
        res.json(pathtoFile);
    });

    nameNodeSocket.on('directoryFileCreated', function() {
        nameNodeSocket.disconnect();
    });
});

//Client command to list the contents of directory
router.get('/listDir', function(req, res) {
    var pathtoFile = req.param('filePath');
    var io = req.app.get('socketio');
    var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });

    nameNodeSocket.on('connect', function() {
        nameNodeSocket.emit('listDirectoryContents', pathtoFile);
        //res.json(pathtoFile);
    });

    nameNodeSocket.on('sendingDirectoryContent', function(data) {
        res.json(data);
        nameNodeSocket.disconnect();
    });

});

//Client command to delete a directory
router.get('/deleteDir', function(req, res) {
    var pathtoFile = req.param('filePath');
    var io = req.app.get('socketio');
    var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });
    nameNodeSocket.on('connect', function() {
        nameNodeSocket.emit('deleteDirectory', pathtoFile);
    });

    nameNodeSocket.on('directoryDeleted', function() {
        nameNodeSocket.disconnect();
        res.json("Successfully Deleted");
    });

});

//Client command to remove a file from directory of name node
router.get('/removeFile', function(req, res) {
    var pathtoFile = req.param('filePath');
    var fileName = req.param('fileName');
    var io = req.app.get('socketio');
    var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });
    nameNodeSocket.on('connect', function() {
        nameNodeSocket.emit('removeFile', pathtoFile);
    });

    nameNodeSocket.on('fileDeleted', function() {
        nameNodeSocket.disconnect();
        res.json("Successfully Deleted File");
    });
});

router.get('/deleteFile', function(req, res) {

    var fileName = req.param('filePath');
    var fileInfo = {};
    fileInfo['fileName'] = fileName;

    var io = req.app.get('socketio');
    var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });

    var dataNodeInfo;

    nameNodeSocket.emit('getIPData', fileInfo);
    nameNodeSocket.on('receiveDNInfo', function(data) {
        nameNodeSocket.disconnect();
        if (data.length !== 0) {
            // TODO: make a connection with datanode and make a delete request
            // TODO: make a delete request to namenode
            dataNodeInfo = data;

            for (var i = 0; i < dataNodeInfo.length; i++) {
                makeRequest(fileInfo, dataNodeInfo[i]);
            }
            res.json('Successful');
        }
        else {
            res.json('File Does not Exist');
        }


    });

    function makeRequest(fileInfo, IP) {
        var options = {
            host: IP,
            port: 5000,
            path: '/deleteFile',
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(JSON.stringify(fileInfo))
            }
        }
        var req = http.request(options, function(response) {

        });

        req.on('error', function(err) {
            console.log(err);
        });
        req.write(JSON.stringify(fileInfo));
        req.end();
    }
});

module.exports = router;
