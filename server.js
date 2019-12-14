'use strict';

// Module imports
var express = require('express')
  , https = require('https')
  , bodyParser = require('body-parser')
  , async = require('async')
  , _ = require('lodash')
  , Client = require('node-rest-client').Client
  , queue = require('block-queue')
  , log = require('npmlog-ts')
  , fs = require("fs")
  , cors = require('cors')
  , isJSON = require('is-valid-json')
  , uuidv4 = require('uuid/v4')
  , commandLineArgs = require('command-line-args')
  , getUsage = require('command-line-usage')
;

const PROCESSNAME = "IoTCS Integration Bridge"
    , VERSION     = "v1.0"
    , AUTHOR      = "Carlos Casares <carlos.casares@oracle.com>"
    , PROCESS     = 'PROCESS'
    , CONFIG      = 'CONFIG'
    , REST        = "REST"
    , QUEUE       = "QUEUE"
    , restURI     = '/iot/integration'
    , RESTPORT    = 5000
    , WEDOTARGET  = 'WEDO-Target'
    , TIMEOUT     = 5000
;

const CERTFOLDER = "/u01/ssl/";
//const CERTFOLDER = "/Users/ccasares/Documents/Oracle/Presales/Initiatives/Wedo/setup/wedoteam.io.certificate/2020/";

const sslOptions = {
  cert: fs.readFileSync(CERTFOLDER + "certificate.fullchain.crt").toString(),
  key: fs.readFileSync(CERTFOLDER + "certificate.key").toString()
};

var restapp    = express()
  , restserver = https.createServer(sslOptions, restapp)
  , client     = new Client()
  , q          = _.noop()
;

// ************************************************************************
// Main code STARTS HERE !!
// ************************************************************************

log.stream = process.stdout;
log.timestamp = true;

// Main handlers registration - BEGIN
// Main error handler
process.on('uncaughtException', function (err) {
  log.info("","Uncaught Exception: " + err);
  log.info("","Uncaught Exception: " + err.stack);
});
// Detect CTRL-C
process.on('SIGINT', function() {
  log.info("","Caught interrupt signal");
  log.info("","Exiting gracefully");
  process.exit(2);
});
// Main handlers registration - END

// Initialize input arguments
const optionDefinitions = [
  { name: 'help', alias: 'h', type: Boolean },
  { name: 'verbose', alias: 'v', type: Boolean, defaultOption: false }
];

const sections = [
  {
    header: PROCESSNAME,
    content: PROCESSNAME + ': forwards messages to its target from IoTCS'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'verbose',
        alias: 'v',
        description: 'Enable verbose logging.'
      },
      {
        name: 'help',
        alias: 'h',
        description: 'Print this usage guide.'
      }
    ]
  }
]
const options = commandLineArgs(optionDefinitions);
log.level = (options.verbose) ? 'verbose' : 'info';

// REST engine initial setup
restapp.use(bodyParser.urlencoded({ extended: true }));
restapp.use(bodyParser.json());
restapp.use(cors());

// Initializing QUEUE variables BEGIN
const QUEUECONCURRENCY = 1
;
// Initializing QUEUE variables END

async.series({
  queue: (next) => {
    log.info(QUEUE, "Initializing QUEUE system");
    q = queue(QUEUECONCURRENCY, (msg, done) => {
      try {
        log.verbose(QUEUE, "Dequeued message: %j", msg);
        if (!_.has(msg, 'target') || !_.has(msg, 'body')) {
          log.error(QUEUE, "ERROR. Dequeued invalid message: %j", msg);
          done();
          return;
        }
        var options = {
          headers: { "Content-Type": "application/json" },
          requestConfig: {
            timeout: TIMEOUT, //request timeout in milliseconds
            noDelay: true, //Enable/disable the Nagle algorithm
            keepAlive: true, //Enable/disable keep-alive functionality idle socket.
            keepAliveDelay: TIMEOUT //and optionally set the initial delay before the first keepalive probe is sent
          },
          responseConfig: {
            timeout: TIMEOUT //response timeout
          },
          data: msg.body
        };    
        var uniqueMethod = uuidv4();  // Just in case we're serving concurrent requests
        client.registerMethod(uniqueMethod, msg.target, 'POST');
        log.verbose(REST, "Sending request with UUID '%s'", uniqueMethod);
        let req = client.methods[uniqueMethod](options, (data, response) => {
          log.verbose(REST, "Request with UUID '%s' ended with a HTTP %d", uniqueMethod, response.statusCode);
          client.unregisterMethod(uniqueMethod);
          done();
        });
  
        req.on('requestTimeout', function (req) {
          log.error(REST, "Request with UUID '%s' has timed out", uniqueMethod);
          req.abort();
        });
        
        req.on('responseTimeout', function (res) {
          log.error(REST, "Response from request with UUID '%s' has timed out", uniqueMethod);
          req.abort();
        });
        
        //it's usefull to handle request errors to avoid, for example, socket hang up errors on request timeouts
        req.on('error', function (err) {
          log.error(REST, "Request with UUID '%s' is errored: %s", uniqueMethod, err.message);
          client.unregisterMethod(uniqueMethod);
          done();
        });  
      } catch(e) {
        log.error(PROCESS, "Unexpected error!! %s", e.message);
        done();
      }
    });
    log.info(QUEUE, "QUEUE system initialized successfully");
    next();
  },
  rest: (next) => {
    // Start REST server
    restserver.listen(RESTPORT, function() {
      log.info(PROCESS,"REST server running on https://localhost:" + RESTPORT + restURI);
      next(null);
    });
  }
}, (err, results) => {
  if (err) {
    log.error("", err.message);
    process.exit(2);
  }
});

restapp.post(restURI, function(req,res) {
  res.status(204).end();
  log.verbose(REST,"Incoming publish request with payload: '%j'", req.body);

  let target = req.get(WEDOTARGET);

  if (!target) {
    // No WEDOTARGET header, ignore
    log.error(REST,"Invalid incoming request without expected '%s' header", WEDOTARGET);
    return;
  }

  let data = _.noop();

  if (req.body) {
    data = (isJSON(req.body)) ? JSON.stringify(req.body) : req.body;
  } else {
    // No JSON body, ignore
    log.error(REST,"Invalid incoming request without JSON body");
    return;
  }

  var message = {
    target: target,
    body: data
  }

  q.push(message);

});
