var express = require('express');
var app = express();
var server = require("http").Server(app);
var port = Number(5000);
var NAMENODE_PORT = 8081;
var NAMENODE_IP = 'IP';
var NAMENODE_ADDRESS = 'http://' + NAMENODE_IP + ':' + NAMENODE_PORT;
var fs = require('fs');
var dataNode = require('./DataNode.js');
var bodyParser = require("body-parser");
var cookieParser = require("cookie-parser");
var io = require("socket.io-client");

var AWS = require("aws-sdk");
AWS.config.update({ region: 'Region' });

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());

app.get('/', function(req, res) {
    res.send('Hello, from DataNode!');
});

app.post('/read', function(req, res) {
    var dataBody = req.body;
    fs.createReadStream(dataBody.fileName + '/' + dataBody.blockName).pipe(res);
});

app.delete('/deleteFile', function(req, res) {
    var dataBody = req.body;

    if (!dataBody) {
        var err = new Error('Deletion Unsucessful');
        res.send(err);
    }
    else {
        var deleteFile = dataNode.removeFile(dataBody.fileName);
    }

    if (deleteFile) {
        var nameNodeSocket = io.connect(NAMENODE_ADDRESS, { reconnect: true });
        nameNodeSocket.emit('removeFileFromMetaData', dataBody.fileName);
        nameNodeSocket.on('removedFile', function() {
            dataNode.deleteFileFromBlockReport(dataBody.fileName);
            nameNodeSocket.disconnect();
        });
    }
});

server.listen(port);

var s3 = new AWS.S3({
    apiVersion: '2006-03-01',
    signatureVersion: 'v4',
    accessKeyId: 'Access Key ID',
    secretAccessKey: 'Secret Access Key',
    region: 'Region'
});

var dataNodeIO = require('socket.io')(server);
var ss = require("socket.io-stream");
dataNodeIO.on('connection', function(socket) {
    console.log("Succesfully connected!");

    socket.on('readFile', function(data) {
        var stream = ss.createStream();
        ss(socket).emit('sendFileDatatoClient', stream);
        fs.createReadStream(data.fileName + '/' + data.blockName).pipe(stream);
    });

    socket.on('sendFileInformation', function(data) {
        dataNode.makeDir(data.mainFile);
        dataNode.addToReport(data.mainFile, data.blockName);
        var paramsWithRange = {
            Bucket: data.bucketName,
            Key: "input/" + data.mainFile,
            Range: data.contentRange
        }
        console.log("Bucket Val:", paramsWithRange);

        var check = dataNode.checkFileExists(data.mainFile, data.blockName);
        if (!check) {
            var stream = fs.createWriteStream(data.mainFile + '/' + data.blockName, { "flags": 'w' });
            s3.getObject(paramsWithRange).createReadStream().pipe(stream);
        }
        else {
            console.log("I'm not writing :D");
        }
    });
});

//Hearbeat Configuration Values
var dgram = require('dgram');
var beat_period = 5;
var debug = 1;
console.log("Sending heartbeat to IP " + NAMENODE_IP + " , port " + NAMENODE_PORT);

//Sends HearBeat every 5 seconds
setInterval(function() {
    var client = dgram.createSocket("udp4");
    var message = Buffer.from(JSON.stringify(dataNode.getReport()));
    client.send(message, 0, message.length, NAMENODE_PORT, NAMENODE_IP, function(err) {
        if (err) {
            console.log(err);
        }
        console.log('Message sent to host: "' + NAMENODE_IP + '", port: ' + NAMENODE_PORT)
        client.close();
    });
}, beat_period * 1000);
