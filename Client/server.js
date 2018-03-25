var express = require('express');
var bodyParser = require("body-parser");
var cookieParser = require("cookie-parser");
var app = express();

var loadData = require("./client/getData.js");

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());

app.use('/', loadData);

app.get('/', function(req, res) {
  res.send('Hello World');
});

var server = require("http").Server(app);
var io = require("socket.io-client");

app.set('socketio', io);

var port = Number(8080);
app.listen(port);

var replicationFactor = 1;
var bucketParams = {
  Bucket: "Bucket",
};

var AWS = require("aws-sdk");
AWS.config.update({ region: 'us-west-2' });

var s3 = new AWS.S3({
  apiVersion: '2006-03-01',
  signatureVersion: 'v4',
  accessKeyId: 'Access Key ID',
  secretAccessKey: 'Secret Access Key',
  region: 'Region'
});

var blockSize = 67108864;
var byteStart = 0;
var byteStop = blockSize;
var interval = blockSize;

var nameNodeData = {
  "fileName": "",
  "blockToDataNodes": "",
}

/*
 *  Client receives missing blocks from NameNode because of DataNode failures
 */
var clientIO = require('socket.io')(server);
clientIO.on('connection', function(socket) {
  console.log("Succesfully receives connection from NameNode!");
  socket.on("missingBlocks", function(missingBlocks) {
    console.log("\nmissingBlocks before:", JSON.stringify(missingBlocks));

    var counter = 0;

    function retrieveMissingDetailsFromNameNode() {
      console.log("Length:", Object.keys(missingBlocks).length);
      for (var fileName in missingBlocks) {
        var paramsWithRange = {
          Bucket: bucketParams.Bucket,
          Key: 'input/' + fileName
        };

        getFromS3(fileName, paramsWithRange);
      }
    }
    
    function getFromS3(fileName, paramsWithRange) {
      var fileInfo = {};
      // receives the data from s3
      s3.headObject(paramsWithRange, function(err, data) {
        if (err) {
          console.log(err, err.stack);
        }
        else {
          fileInfo['fileName'] = paramsWithRange.Key;
          fileInfo['fileSize'] = data.ContentLength;
          fileInfo['filePath'] = data.filePath;
        }
        nameNodeData['fileName'] = fileInfo['fileName'];

        console.log("File info filename: ", fileInfo['fileName']);
        console.log("File nameNodeData: ", nameNodeData);

        console.log("File size: ", fileInfo['fileSize']);

        var numOfBlocks = Math.ceil(fileInfo['fileSize'] / 67108864);
        var blockInfo = {};

        for (var i = 0; i < numOfBlocks * replicationFactor; i++) {
          if (i % numOfBlocks === 0) {
            var byteStart = 0;
            var byteStop = blockSize;
          }

          var blockName = nameNodeData['fileName'] + ".part-" + (i % numOfBlocks);
          blockInfo[i] = {};
          blockInfo[i]['key'] = blockName;
          blockInfo[i]['byteStart'] = byteStart;
          blockInfo[i]['byteStop'] = byteStop;

          byteStart = byteStop + 1;
          if ((byteStop + interval) > fileInfo['fileSize']) {
            byteStop = fileInfo['fileSize'];
          }
          else {
            byteStop = byteStop + interval;
          }
        }

        nameNodeData['blockToDataNodes'] = blockInfo;

        console.log("\nName node data on " + fileName + ": " + JSON.stringify(nameNodeData));
        for (var missingBlock in missingBlocks[fileName]) {
          var missingBlockName = "input/" + missingBlock;
          for (var i in nameNodeData['blockToDataNodes']) {
            if (missingBlockName === nameNodeData['blockToDataNodes'][i]['key']) {
              missingBlocks[fileName][missingBlock]['byteStart'] = nameNodeData['blockToDataNodes'][i]['byteStart'];
              missingBlocks[fileName][missingBlock]['byteStop'] = nameNodeData['blockToDataNodes'][i]['byteStop'];


            }
          }

        }
        console.log("\nmissingBlock after 1:", JSON.stringify(missingBlocks));
        counter++;
        if (counter === Object.keys(missingBlocks).length) {
          readFileFromS3(missingBlocks, paramsWithRange);
        }
      });
    }

    function readFileFromS3(missingBlocks, paramsWithRange) {
      for (var fileName in missingBlocks) {
        for (var missingBlock in missingBlocks[fileName]) {
          var IP = missingBlocks[fileName][missingBlock]['IP'];
          var dataNodeSocket = io.connect('http://' + IP + ":5000");
          var byteStartRange = missingBlocks[fileName][missingBlock]['byteStart'];
          var byteStopRange = missingBlocks[fileName][missingBlock]['byteStop'];
          console.log(byteStartRange);
          console.log(byteStopRange);
          paramsWithRange['Range'] = 'bytes=' + byteStartRange + '-' + byteStopRange;

          var fileData = {
            'mainFile': fileName,
            'bucketName': bucketParams.Bucket,
            'bucketKey': bucketParams.Key,
            'blockName': missingBlock,
            'contentRange': paramsWithRange['Range']
          };

          dataNodeSocket.emit('sendFileInformation', fileData);
        }
      }
    }

    retrieveMissingDetailsFromNameNode();

  });
});

var failurePort = Number(8082);
server.listen(failurePort);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

console.log("Server Running on:", port);
