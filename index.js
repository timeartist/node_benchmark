var redis = require("redis"),
fs = require("fs"),
uuid = require("node-uuid");

const DATA_FILE = "insights-session.json",
NUMBER_OF_KEYS = 5000,
NUMBER_OF_ITERATIONS = 20000;

var generatedKeysCount = 0,
generateKeysStartTime;
function generateKeysClosure(err, data){
  /** Tracks the start and end of the process **/
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
  /** generate NUMBER_OF_KEYS keys with guid as the keyname and
  the contents of DATA_FILE as the value **/
  var keys = [];
  for (var i=0; i<NUMBER_OF_KEYS; i++){
    var key = uuid.v4();
    client.set(key, data, generateKeysClosure); //async response handled by generateKey
    keys.push(key); //Track the key names so they can be used to access later
  }
  return keys;
}


function redisClient(connectionString) {
  /** generate a redis client, test it, if it works return it **/
  var client = redis.createClient(connectionString);

  client.on("error", function (err) {
      throw err;
  });

  return client;
}

var readRandomKeysStartTime,
readKeysCount = 0;
function readRandomKeysClosure(err, data) {
  /** tracks when operation is complete and prints results then exits**/
    readKeysCount++;
    if (readKeysCount === NUMBER_OF_ITERATIONS) {
      var readKeysTimeElapsed = new Date().valueOf() - readRandomKeysStartTime.valueOf();
      console.log(NUMBER_OF_ITERATIONS + " keys accessed in " + readKeysTimeElapsed / 1000 + " seconds with " + readKeysTimeElapsed / NUMBER_OF_ITERATIONS + " ms average latency");
      process.exit(1);
    }
}

function readRandomKeys(client, keys, numberOfIterations) {
  /** read numberOfIterations times from available randomly selected keys **/
  readRandomKeysStartTime = new Date();
  for (var i=0; i<numberOfIterations; i++) {
    client.get(keys[Math.floor(Math.random()*keys.length)], readRandomKeysClosure); //calls readRandomKeysClosure
  }
}

/**make a client, load our data, clean out db then generate keys and read from them **/
var client = redisClient(process.argv[2]),
serializedData = fs.readFileSync(DATA_FILE, 'utf8');
client.flushdb();
var keys = generateKeys(client, serializedData);
readRandomKeys(client, keys, NUMBER_OF_ITERATIONS);
