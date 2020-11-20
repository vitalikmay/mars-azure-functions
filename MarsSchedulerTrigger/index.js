const axios = require('axios');
const moment = require('moment');
const later = require('later');
const AWS = require('aws-sdk');

AWS.config.update({
  "region": process.env["AWS_REGION"],
  "secretAccessKey": process.env["AWS_SECRET_ACCESS_KEY"],
  "accessKeyId": process.env["AWS_ACCESS_KEY_ID"],
});

function getBlockUrl(block, domain) {
  if (block === 'KeepAlive') {
    return (new URL(process.env["KEEP_ALIVE_URL_POSTFIX"], `https://${domain}`)).toString();
  }

  return (new URL(`/api/${block}`, `https://${domain}`)).toString();
}

function prepareLogItem(domain, block, status, statusCode, headers, response, timestamp) {
  return {
    app: domain,
    block_status_time: `${block}_${status}_${timestamp}`,
    block_time: `${block}_${timestamp}`,
    status_time: `${status}_${timestamp}`,
    status,
    statusCode,
    headers: headers ? JSON.stringify(headers) : '',
    response,
    timestamp,
  };
}

async function log(items) {
  const db = new AWS.DynamoDB.DocumentClient();
  return db.batchWrite({ RequestItems: { [process.env["AWS_LOG_TABLE_NAME"]]: items.map(i => ({ PutRequest: { Item: i } })) } }).promise();
}

module.exports = async function (context, timer) {
  const nextRun = moment.utc(timer.scheduleStatus.next);
  const now = context.bindings.schedulerCurrentRunIn && context.bindings.schedulerCurrentRunIn.run 
    ? moment.utc(context.bindings.schedulerCurrentRunIn.run)
    : moment.utc();

  let configs = null;
  try {
    const response = await axios.post(process.env["SCHEDULER_CONFIGS_URL"]);
    configs = response.data;
    context.bindings.schedulerConfigsOut = configs;  // cache config in blob
  } catch {
    configs = context.bindings.schedulerConfigsIn;   // fetch cached config in case of request error
  }

  context.bindings.schedulerCurrentRunOut = { run: timer.scheduleStatus.next };
  if (!configs || !Array.isArray(configs) || !configs.length) {
    context.done();
    return;
  }

  // Prepare URLs to execute
  const apps = {};
  configs.forEach(i => {
    apps[i.domain] = { blocksToCall: [], fallback: i.slackFallback };
    const app = apps[i.domain];

    if (!i.enabled || !i.tasks || !i.tasks.length) {
      app.blocksToCall.push('KeepAlive');
      return;
    }

    const tasks = i.tasks.filter(t => t.enabled);
    if (!tasks.length) {
      app.blocksToCall.push('KeepAlive');
      return;
    }

    tasks.forEach(t => {
      try {
        const cron = later.parse.cron(t.cron, true);
        const taskPrev = moment.utc(later.schedule(cron).prev(1));
        const taskNext = moment.utc(later.schedule(cron).next(1));
        const mustExecute = (taskNext.isSameOrAfter(now) && taskNext.isBefore(nextRun)) || taskPrev.isSame(now);
        if (mustExecute) {
          app.blocksToCall.push(t.block);
        }
      }
      catch {}
    });

    if (!app.blocksToCall.length) {
      app.blocksToCall.push('KeepAlive');
      return;
    }
  });

  // Flatten apps
  const appsFlatten = [];
  Object.keys(apps).forEach(app => {
    apps[app].blocksToCall.forEach(i => {
      appsFlatten.push({ app, fallback: apps[app].fallback, block: i, url: getBlockUrl(i, app) });
    });
  });

  // Call blocks
  const blockPromises = appsFlatten.map(i => axios.get(i.url));
  const results = await Promise.allSettled(blockPromises);

  // Log to AWS DynamoDB
  const timestamp = moment().unix();
  const logItems = results.map((i, ix) => {
    const app = appsFlatten[ix];
    const response = i.status === 'fulfilled' ? i.value : (i.reason.response ? i.reason.response : { status: i.reason.code, headers: null, data: i.reason.message });
    return prepareLogItem(app.app, app.block, i.status === 'rejected' ? 'Failed' : 'Success', response.status, response.headers, response.data, timestamp);
  });
  try { await log(logItems); } catch { }

  // prepare fallbacks
  const fallbacks = [];
  results.forEach((i, ix) => {
    if (i.status === 'rejected' && appsFlatten[ix].fallback) {
      fallbacks.push({ block: appsFlatten[ix].block, fallback: appsFlatten[ix].fallback });
    }
  });

  if (!fallbacks.length) {
    context.done();
    return;
  }

  // Send fallback notifications
  const fallbackPromises = fallbacks.map(i => {
    const options = {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      data: JSON.stringify({
        text: `Scheduler execution of the block *${i.block}* failed.`,
        channel: i.fallback.channel || undefined,
        username: i.fallback.username,
      }),
      url: i.fallback.webhookUrl,
    };
    return axios.request(options);
  });

  await Promise.allSettled(fallbackPromises);
  context.done();
};
