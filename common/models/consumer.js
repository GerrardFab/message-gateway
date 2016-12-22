/**
 * Created by fab.yin on 09/12/2016.
 */
"use strict";
var kafka = require('kafka-node');
var winston = require('winston');
var id = 1;
//var connStr = '172.16.98.182:2181';
module.exports = (Consumer) => {
  Consumer.remoteMethod('createGroup', {
    http: {verb: 'post', path: '/create/group'},
    description: '',
    accessType: 'READ',
    accepts: [
      {arg: 'type', required: true, type: 'string'}
    ],
    returns: {arg: 'data', type: 'string', root: true}
  }, {});

  Consumer.remoteMethod('createSingle', {
    http: {verb: 'post', path:'/create'},
    description: '',
    accessType: 'READ',
    accepts: [
      {arg: 'type', required: true, type: 'string'}
    ],
    returns: {arg:'data', type: 'string', root: true}
  }, {});

  Consumer.remoteMethod('createHighLevel', {
    http: {verb: 'post', path: '/create/high'},
    description: '',
    accessType: 'READ',
    accepts: [
      {arg: 'id', required: true, type: 'string'},
      {arg: 'address', required: true, type: 'string'},
      {arg: 'type', required: true, type: 'string'}
    ],
    returns: {arg: 'data', type: 'string', root: true}
  });

  Consumer.on('dataSourceAttached', model => (Object.assign(model, {
    createSingle(type, cb){
      var Consumer = kafka.Consumer;
      var Offset = kafka.Offset;
      var Client = kafka.Client;
      var argv = require('optimist').argv;
      var topic = argv.topic || type;

      var client = new Client(process.env.KAFKA_URL);
      var topics = [
        {topic: topic}
      ];
      var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

      var consumer = new Consumer(client, topics, options);
      var offset = new Offset(client);

      consumer.on('message', function (message) {
        winston.info(message);
      });

      consumer.on('error', function (err) {
        winston.error(err);
      });

      cb(null,'Consumer create successfully...');
    },

    createGroup(type, cb){
      var async = require('async');
      var ConsumerGroup = kafka.ConsumerGroup;

      var consumerOptions = {
        host: process.env.KAFKA_URL,
        groupId: 'ExampleTestGroup',
        sessionTimeout: 15000,
        protocol: ['roundrobin'],
        fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
      };

      var topics = [type];

      var consumerGroup = new ConsumerGroup(Object.assign({id: 'consumer1'}, consumerOptions), topics);
      consumerGroup.on('error', onError);
      consumerGroup.on('message', onMessage);

      var consumerGroup2 = new ConsumerGroup(Object.assign({id: 'consumer2'}, consumerOptions), topics);
      consumerGroup2.on('error', onError);
      consumerGroup2.on('message', onMessage);

      var consumerGroup3 = new ConsumerGroup(Object.assign({id: 'consumer3'}, consumerOptions), topics);
      consumerGroup3.on('error', onError);
      consumerGroup3.on('message', onMessage);

      function onError (error) {
        console.error(error);
        console.error(error.stack);
      }

      function onMessage (message) {
        console.log('%s: %s read msg Topic="%s" Partition=%s Offset=%d value=%s', new Date().toLocaleTimeString(), this.client.clientId, message.topic, message.partition, message.offset, message.value);
        winston.info('%s: %s read msg Topic="%s" Partition=%s Offset=%d value=%s', new Date().toLocaleTimeString(), this.client.clientId, message.topic, message.partition, message.offset, message.value);
      }

      process.once('SIGINT', function () {
        async.each([consumerGroup, consumerGroup2, consumerGroup3], function (consumer, callback) {
          consumer.close(true, callback);
        });
      });

      console.log('Consumer group starts..');
      cb(null, 'Consumer group create successfully...');
    },

    createHighLevel(id, address, type, cb){
      var HighLevelConsumer = kafka.HighLevelConsumer;
      var Client = kafka.Client;
      var argv = require('optimist').argv;
      var topic = argv.topic || type;
      var client = new Client(address);
      var topics = [{ topic: topic }];
      var options = { id: id, autoCommit: true, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };
      var consumer = new HighLevelConsumer(client, topics, options);

      consumer.on('message', function (message) {
        console.log('%s: %s read msg Topic="%s" Partition=%s Offset=%d value=%s', new Date().toLocaleTimeString(), id, message.topic, message.partition, message.offset, message.value);
        winston.info('%s: %s read msg Topic="%s" Partition=%s Offset=%d value=%s', new Date().toLocaleTimeString(), this.client.clientId, message.topic, message.partition, message.offset, message.value);
      });

      consumer.on('error', function (err) {
        console.log('error', err);
      });

      cb(null, 'High Level Consumer create successfully...');
    }
  })));
};
// module.exports.default = Consumer;
// export default Consumer => {／
//   Consumer.remoteMethod('createGroup', {
//     http: {verb: 'post', path: '/create/group'},
//     description: '',
//     accessType: 'READ',
//     accepts: [
//       {arg: 'type', required: true, type: 'string'}
//     ],
//     returns: {arg: 'data', type: 'string', root: true}
//   }, {});
//
//   Consumer.remoteMethod('createSingle', {
//     http: {verb: 'post', path:'/create'},
//     description: '',
//     accessType: 'READ',
//     accepts: [
//       {arg: 'type', required: true, type: 'string'}
//     ],
//     returns: {arg:'data', type: 'string', root: true}
//   }, {});
//
//   Consumer.remoteMethod('createHighLevel', {
//     http: {verb: 'post', path: '/create/high'},
//     description: '',
//     accessType: 'READ',
//     accepts: [
//       {arg: 'id', required: true, type: 'string'},
//       {arg: 'address', required: true, type: 'string'},
//       {arg: 'type', required: true, type: 'string'}
//     ],
//     returns: {arg: 'data', type: 'string', root: true}
//   });
//
//   Consumer.on('dataSourceAttached', model => (Object.assign(model, {
//     createSingle(type, cb){
//       var Consumer = kafka.Consumer;
//       var Offset = kafka.Offset;
//       var Client = kafka.Client;
//       var argv = require('optimist').argv;
//       var topic = argv.topic || type;
//
//       var client = new Client(process.env.KAFKA_URL);
//       var topics = [
//         {topic: topic}
//       ];
//       var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };
//
//       var consumer = new Consumer(client, topics, options);
//       var offset = new Offset(client);
//
//       consumer.on('message', function (message) {
//         console.log(message);
//       });
//
//       consumer.on('error', function (err) {
//         console.log('error', err);
//       });
//
//       cb(null,'Consumer create successfully...');
//     },
//
//     createGroup(type, cb){
//       var async = require('async');
//       var ConsumerGroup = kafka.ConsumerGroup;
//
//       var consumerOptions = {
//         host: process.env.KAFKA_URL,
//         groupId: 'ExampleTestGroup',
//         sessionTimeout: 15000,
//         protocol: ['roundrobin'],
//         fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
//       };
//
//       var topics = [type];
//
//       var consumerGroup = new ConsumerGroup(Object.assign({id: 'consumer1'}, consumerOptions), topics);
//       consumerGroup.on('error', onError);
//       consumerGroup.on('message', onMessage);
//
//       var consumerGroup2 = new ConsumerGroup(Object.assign({id: 'consumer2'}, consumerOptions), topics);
//       consumerGroup2.on('error', onError);
//       consumerGroup2.on('message', onMessage);
//
//       var consumerGroup3 = new ConsumerGroup(Object.assign({id: 'consumer3'}, consumerOptions), topics);
//       consumerGroup3.on('error', onError);
//       consumerGroup3.on('message', onMessage);
//
//       function onError (error) {
//         console.error(error);
//         console.error(error.stack);
//       }
//
//       function onMessage (message) {
//         console.log('%s: %s read msg Topic="%s" Partition=%s Offset=%d value=%s', new Date().toLocaleTimeString(), this.client.clientId, message.topic, message.partition, message.offset, message.value);
//       }
//
//       process.once('SIGINT', function () {
//         async.each([consumerGroup, consumerGroup2, consumerGroup3], function (consumer, callback) {
//           consumer.close(true, callback);
//         });
//       });
//
//       console.log('Consumer group starts..');
//       cb(null, 'Consumer group create successfully...');
//     },
//
//     createHighLevel(id, address, type, cb){
//       var HighLevelConsumer = kafka.HighLevelConsumer;
//       var Client = kafka.Client;
//       var argv = require('optimist').argv;
//       var topic = argv.topic || type;
//       var client = new Client(address);
//       var topics = [{ topic: topic }];
//       var options = { id: id, autoCommit: true, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };
//       var consumer = new HighLevelConsumer(client, topics, options);
//
//       consumer.on('message', function (message) {
//         console.log('%s: %s read msg Topic="%s" Partition=%s Offset=%d value=%s', new Date().toLocaleTimeString(), id, message.topic, message.partition, message.offset, message.value);
//       });
//
//       consumer.on('error', function (err) {
//         console.log('error', err);
//       });
//
//       cb(null, 'High Level Consumer create successfully...');
//     }
//   })));
// };
