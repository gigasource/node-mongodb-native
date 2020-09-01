const EventEmitter = require('events');
const queueEmitter = new EventEmitter();

let mongoAddon;
try {
  console.log("Pre require");
  mongoAddon = require('./mongo-addon.node').mongoAddon;
  console.log('Mongo is loaded');
} catch (err) {
  console.log(err);
}
const BSON = require('bson');
const bson = new BSON();
let status, lib, db, client;
const queue = [];
let isLock = false;

queueEmitter.on('exec', () => {
  let currentQuery = queue.shift();
  if (currentQuery != undefined) {
    const { query, cb } = currentQuery;
    isLock = true;
    mongoAddon.invoker(query, client, status, (err, result) => {
      if (err) {
        cb(err, null);
      }
      try {
        const _result = bson.deserialize(result);
        // console.log(_result);
        cb(null, Object.assign(_result, {message: {documents: [_result]}}));
      } catch (err) {
        console.log(`Query ${JSON.stringify(query)} return error ${err}`);
      } finally {
        isLock = false;
        queueEmitter.emit('exec');
      }
    })
  }
});

function connect(dbPath = "dbpath=/Users/admin/tmp") {
  console.log("DB: ", dbPath);
  const params = mongoAddon.paramsHandler.mongo_embedded_v1_init_params();
  console.log("params", params);
  status = mongoAddon.statusHandler.mongo_embedded_v1_status_create();
  console.log("status", status);
  mongoAddon.paramsHandler.set_yaml_config(params, dbPath.length, dbPath);
  lib = mongoAddon.libHandler.mongo_embedded_v1_lib_init(params, status);
  console.log("lib", lib);
  db = mongoAddon.instanceHandler.mongo_embedded_v1_instance_create(lib, params, status);
  console.log("db", db);
  client = mongoAddon.clientHandler.mongo_embedded_v1_client_create(db, status);
  console.log("Client", client);
}

function pushToQueue(query, cb) {
  queue.push({
    query,
    cb
  })
  if (!isLock) {
    queueEmitter.emit('exec');
  }
}

function invoke(query, dbName, cb) {
  // console.log(query);
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
