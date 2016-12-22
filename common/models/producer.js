
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
            { topic: topic, partition: p, messages: [content + '-' + partition++], attributes: 0 }
          ], function (err, result) {
            //console.log(err || result);
            //process.exit();
          });
        }, 3000);
      });

      producer.on('error', function (err) {
        console.log('error', err);
      });

      console.log('Producer starts..');
      cb(null, 'Producer create successfully...');

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
