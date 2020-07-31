const EventEmitter = require('events');
const queueEmitter = new EventEmitter();
const { NativeModules } = require('react-native')

let mongoAddon = NativeModules.RNMongooseGiga;

const BSON = require('bson');
const bson = new BSON();

function connect(yamlConfig = "dbpath=/Users/admin/tmp") {
  mongoAddon.create(yamlConfig);
}

function invoke(query, dbName, cb) {
  const _query = bson.serialize(Object.assign(query, {$db: dbName}));
  mongoAddon.invoker(_query, (err, result) => {
    if (err) {
      console.log(err);
      cb(err, null);
    } else {
      const _result = bson.deserialize(result);
      try {
        const _result = bson.deserialize(result);
        cb(null, Object.assign(_result, {message: {documents: [_result]}}));
      } catch (err) {
        console.log(`Query ${JSON.stringify(query)} return error ${err}`);
      }
    }
  });
}

module.exports = {
  connect,
  invoke
}
