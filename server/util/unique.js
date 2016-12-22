/**
 * Created by fab.yin on 14/12/2016.
 */
'use strict';

const VERSION = 0;
const _ = require('lodash');

function assignUnique (topicPartition, groupMembers, callback) {
  var assignment = _(groupMembers).map('id').reduce(function (obj, id) {
    obj[id] = [];
    return obj;
  }, {});

  const topicMemberMap = topicToMemberMap(groupMembers);

}

function topicToMemberMap (groupMembers) {
  return groupMembers.reduce(function (result, member) {
    member.subscription.forEach(function (topic) {
      if (topic in result) {
        result[topic].push(member.id);
      } else {
        result[topic] = [member.id];
      }
    });
    return result;
  }, {});
}

module.exports = {
  assign: assignUnique,
  name: 'unique',
  version: VERSION
};
