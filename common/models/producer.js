
var kafka = require('kafka-node');
//var connStr = '172.16.98.182:2181';
var partition = 0;
module.exports = (Producer) => {
  Producer.remoteMethod('create', {
    http: {verb: 'post', path: '/create'},
    description: '',
    accessType: 'READ',
    accepts: [
      {arg: 'address', required: true, type: 'string'},
      {arg: 'type', required: true, type: 'string'},
      {arg: 'content', required: true, type: 'string'}
    ],
    returns: {arg: 'data', type: 'string', root: true}
  }, {});

  Producer.remoteMethod('createSingle', {
    http: {verb: 'post', path: '/create/single'},
    description: '',
    accessType: 'READ',
    accepts: [
      {arg: 'address', required: true, type: 'string'},
      {arg: 'type', required: true, type: 'string'},
      {arg: 'content', required: true, type: 'string'}
    ],
    returns: {arg: 'data', type: 'string', root: true}
  }, {});

  // Producer.remoteMethod('createTopic', {
  //   http: {verb: 'post', path: '/create/topic'},
  //   description: '',
  //   accessType: 'READ',
  //   accepts: [
  //     {arg: 'topicName', required: true, type: 'string'}
  //   ],
  //   returns: {arg: 'data', type: 'string', root: true}
  // }, {});

  Producer.on('dataSourceAttached', model => (Object.assign(model, {
    create(address, type, content, cb){
      var Producer = kafka.Producer;
      var Client = kafka.Client;
      var client = new Client(address);
      var argv = require('optimist').argv;
      var topic = argv.topic || type;
      var p = 0;
      var producer = new Producer(client, { requireAcks: 1 });

      producer.on('ready', function () {
        setInterval(function () {
          p = Math.floor(Math.random()*(3-0)+0);
          producer.send([
            { topic: topic, partition: p, messages: [getMsg(p)], attributes: 0 }
          ], function (err, result) {
            //console.log(err || result);
            //process.exit();
          });

          function getMsg(index) {
            var methods = ['GET', 'POST', 'DELETE'];
            var msg = '{"unit": "milliseconds", "http_method": "'+methods[index]+'", "value": '+Math.floor(Math.random()*(1000-100)+100)+', "timestamp": "'+new Date()+'", "http_code": "'+Math.floor(Math.random()*(500-200)+200)+'", "page": "/list", "metricType": "request/latency", "server": "www'+ (index+1) + '.example.com"}';
            return msg;
          };
        }, 3000);
      });

      producer.on('error', function (err) {
        console.log('error', err);
      });

      console.log('Producer starts..');
      cb(null, 'Producer create successfully...');

    },

    createSingle(address, type, content, cb){
      var Producer = kafka.Producer;
      var Client = kafka.Client;
      var client = new Client(address);
      var topic = type;
      var p = 0;
      var producer = new Producer(client, { requireAcks: 1 });

      producer.on('ready', function () {
        producer.send([
          { topic: topic, partition: p, messages: [getMsgArr(p)], attributes: 0 }
        ], function (err, result) {
        });

        function getMsgArr(index) {
          var arr = new Array();
          var methods = ['GET', 'POST', 'DELETE'];
          var msg;
          var index1;
          for(var i=0;i<1000;i++){
            index1 = i % 3;
            msg = '{"unit": "milliseconds", "http_method": "'+methods[index1]+'", "value": '+Math.floor(Math.random()*(1000-100)+100)+', "timestamp": "'+new Date()+'", "http_code": "'+Math.floor(Math.random()*(500-200)+200)+'", "page": "/list", "metricType": "request/latency", "server": "www'+ (index1+1) + '.example.com"}';
            arr.push(msg);
          }
          return arr;
        };
      });
      producer.on('error', function (err) {
        console.log('error', err);
      });

      console.log('Producer starts..');
      cb(null, 'Producer create single successfully...');
    }
  })));
};
// export default Producer => {
//   Producer.remoteMethod('create', {
//     http: {verb: 'post', path: '/create'},
//     description: '',
//     accessType: 'READ',
//     accepts: [
//       {arg: 'address', required: true, type: 'string'},
//       {arg: 'type', required: true, type: 'string'},
//       {arg: 'content', required: true, type: 'string'}
//     ],
//     returns: {arg: 'data', type: 'string', root: true}
//   }, {});
//
//   Producer.on('dataSourceAttached', model => (Object.assign(model, {
//     create(address, type, content, cb){
//       var Producer = kafka.Producer;
//       var Client = kafka.Client;
//       var client = new Client(address);
//       var argv = require('optimist').argv;
//       var topic = argv.topic || type;
//       var p = 0;
//       var producer = new Producer(client, { requireAcks: 1 });
//
//       producer.on('ready', function () {
//         setInterval(function () {
//           p = Math.floor(Math.random()*(3-0)+0);
//           producer.send([
//             { topic: topic, partition: p, messages: [content + '-' + partition++], attributes: 0 }
//           ], function (err, result) {
//             //console.log(err || result);
//             //process.exit();
//           });
//         }, 3000);
//       });
//
//       producer.on('error', function (err) {
//         console.log('error', err);
//       });
//
//       console.log('Producer starts..');
//       cb(null, 'Producer create successfully...');
//
//     }
//   })));
//
// };
