var redis = require("redis"),
fs = require("fs"),
uuid = require("node-uuid");

const DATA_FILE = "insights-session.json",
NUMBER_OF_KEYS = 5000,
NUMBER_OF_ITERATIONS = 20000;

var generatedKeysCount = 0,
generateKeysStartTime;
function generateKeysClosure(err, data){
  if (generatedKeysCount === 0) {
    generateKeysStartTime = new Date();
  }
  generatedKeysCount++;
  if (generatedKeysCount === NUMBER_OF_KEYS){
    var timeElaspsed = new Date().valueOf() - generateKeysStartTime.valueOf()
    console.log(generatedKeysCount + " keys created in " + timeElaspsed / 1000 + " seconds with " + timeElaspsed / NUMBER_OF_KEYS + "ms average latency");
  }
}

function generateKeys(client, data) {
  //console.log("generateKeys start!");
  var keys = [];
  for (var i=0; i<NUMBER_OF_KEYS; i++){
    var key = uuid.v4();
    client.set(key, data, generateKeysClosure);
    keys.push(key);
  }
  //console.log("generateKeys end!");
  return keys;
}


function redisClient(connectionString) {
  var client = redis.createClient(connectionString);

  client.on("error", function (err) {
      throw err;
  });

  return client;
}

var readRandomKeysStartTime,
readKeysCount = 0;
function readRandomKeysClosure(err, data) {
    readKeysCount++;
    if (readKeysCount === NUMBER_OF_ITERATIONS) {
      var readKeysTimeElapsed = new Date().valueOf() - readRandomKeysStartTime.valueOf();
      console.log(NUMBER_OF_ITERATIONS + " keys accessed in " + readKeysTimeElapsed / 1000 + " seconds with " + readKeysTimeElapsed / NUMBER_OF_ITERATIONS + " ms average latency");
    }
}

function readRandomKeys(client, keys, numberOfIterations) {
  for (var i=0; i<numberOfIterations; i++) {
    readRandomKeysStartTime = new Date();
    client.get(keys[Math.floor(Math.random()*keys.length)], readRandomKeysClosure);
  }
}

var client = redisClient(process.argv[2]),
serializedData = fs.readFileSync(DATA_FILE, 'utf8');
client.flushdb();
var keys = generateKeys(client, serializedData);
readRandomKeys(client, keys, NUMBER_OF_ITERATIONS);

//client.quit();
//process.exit(1);

//client.quit();
