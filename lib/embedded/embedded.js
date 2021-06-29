const EventEmitter = require('events');

let mongoAddon;
try {
  mongoAddon = require('./mongo-addon.node').mongoAddon;
  console.log('Mongo is loaded');
} catch (err) {
  console.log(err);
}
const BSON = require('bson');
const bson = new BSON();
let db, lib, status;
const client = {}
const queue = {};
const queueEmitter = {};
const isLock = {};
const NUMBER_OF_THREAD = 3

for (let i = 0; i < NUMBER_OF_THREAD; i++) {
  queueEmitter[i] = new EventEmitter()
  queue[i] = []
  isLock[i] = false
  queueEmitter[i].on('exec', () => {
    let currentQuery = queue[i].shift();
    if (currentQuery != undefined) {
      isLock[i] = true;
      const {query, cb} = currentQuery;
      mongoAddon.invoker(query, client[i], status, (err, result) => {
        if (err) {
          cb(err, null);
        }
        try {
          const _result = bson.deserialize(result);
          cb(null, Object.assign({ ..._result }, {message: {documents: [{ ..._result }]}}));
        } catch (err) {
          console.log(`Query ${JSON.stringify(query)} return error ${err}`);
        } finally {
          isLock[i] = false;
          queueEmitter[i].emit('exec');
        }
      })
    }
  });
}

function connect(dbPath = "dbpath=/Users/admin/tmp") {
  const params = mongoAddon.paramsHandler.mongo_embedded_v1_init_params();
  mongoAddon.paramsHandler.set_yaml_config(params, dbPath.length, dbPath);
  status = mongoAddon.statusHandler.mongo_embedded_v1_status_create();
  lib = mongoAddon.libHandler.mongo_embedded_v1_lib_init(params, status);
  db = mongoAddon.instanceHandler.mongo_embedded_v1_instance_create(lib, params, status);
  for (let i = 0; i < NUMBER_OF_THREAD; i++) {
    client[i] = mongoAddon.clientHandler.mongo_embedded_v1_client_create(db, status);
  }
}

let currentQueue = 0
function pushToQueue(query, cb) {
  queue[currentQueue].push({
    query,
    cb
  })
  if (!isLock[currentQueue]) {
    queueEmitter[currentQueue].emit('exec');
  }
  currentQueue = (currentQueue + 1 === NUMBER_OF_THREAD) ? 0 : currentQueue + 1
}

function invoke(query, dbName, cb) {
  const _query = bson.serialize(Object.assign(query, {$db: dbName}));
  pushToQueue(_query, cb);
}

module.exports = {
  connect,
  invoke
}
/*const query = "{insert: 'collection_name', documents: [{firstName: 'doc1FirstName', lastName:'doc1LastName', age: 30}, {firstName: 'doc2FirstName', lastName: 'doc2LastName', age: 20}]}";
const dbName = "myDB";
for (let i = 0; i < 5; i++) {
  const result = mongoAddon.invoker(query.length, query, dbName.length, dbName, client, status);
  console.log("HERE");
  console.log(result);
}
const query1 = "{find: 'collection_name'}";
const result = mongoAddon.invoker(query1.length, query1, dbName.length, dbName, client, status);
console.log(result);*/
