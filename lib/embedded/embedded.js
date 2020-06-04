const mongoAddon = require('./mongo-addon.node').mongoAddon;
let status, lib, db;

function connect(dbPath = "dbpath=/Users/admin/tmp") {
  const params = mongoAddon.paramsHandler.mongo_embedded_v1_init_params();
  status = mongoAddon.statusHandler.mongo_embedded_v1_status_create();
  mongoAddon.paramsHandler.set_yaml_config(params, dbPath.length, dbPath);
  lib = mongoAddon.libHandler.mongo_embedded_v1_lib_init(params, status);
  db = mongoAddon.instanceHandler.mongo_embedded_v1_instance_create(lib, params, status);
}

function invoke(query, dbName, cb) {
  const client = mongoAddon.clientHandler.mongo_embedded_v1_client_create(db, status);
  const _query = JSON.stringify(query);
  console.log(_query);
  mongoAddon.invoker(_query.length, _query, dbName.length, dbName, client, status, (err, result) => {
    console.log('MTFK');
    if (err) {
      cb(err, null);
    }
    _result = JSON.parse(result);
    cb(null, Object.assign(_result, {message: {documents: [_result]}}));
  })
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
