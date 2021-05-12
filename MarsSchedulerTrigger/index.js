const axios = require('axios');
const moment = require('moment');
const later = require('later');
const AWS = require('aws-sdk');

axios.interceptors.request.use(function (config) {
  config.metadata = config.metadata || {};
  config.metadata.startTime = (new Date()).getTime();
  return config;
}, function (error) {
  return Promise.reject(error);
});

axios.interceptors.response.use(function (response) {
  response.config.metadata.endTime = (new Date()).getTime();
  response.duration = response.config.metadata.endTime - response.config.metadata.startTime;
  return response;
}, function (error) {
  error.config.metadata.endTime = (new Date()).getTime();
  error.duration = error.config.metadata.endTime - error.config.metadata.startTime;
  return Promise.reject(error);
});

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

function prepareLogItem(domain, cron, block, status, statusCode, headers, response, timestamp, duration) {
  return {
    app: domain,
    block_status_time: `${block}_${status}_${timestamp}`,
    block_time: `${block}_${timestamp}`,
    status_time: `${status}_${timestamp}`,
    cron,
    block,
    status,
    statusCode,
    headers: headers ? JSON.stringify(headers) : '',
    response,
    timestamp,
    duration,
  };
}

async function log(items) {
  const db = new AWS.DynamoDB.DocumentClient();
  return db.batchWrite({ RequestItems: { [process.env["AWS_LOG_TABLE_NAME"]]: items.map(i => ({ PutRequest: { Item: i } })) } }).promise();
}

function isKeepAliveSuccess(headers, data) {
  if (!headers || !data) {
    return false;
  }

  if (!headers['cache-control'] || !headers['cache-control'].includes('max-age=0')) {
    return false;
  }

  if (!headers['x-cache'] || !headers['x-cache'].includes('Miss from cloudfront')) {
    return false;
  }

  if (!data.toString().toLowerCase().includes("i'm alive")) {
    return false;
  }

  return true;
};

module.exports = async function (context, timer) {
  const veryNow = moment.utc();
  const nextRun = moment.utc(timer.scheduleStatus.next).isSameOrBefore(veryNow) // is it an Azure function bug?..
    ? moment.utc(timer.scheduleStatus.next).add(1, 'minutes')
    : moment.utc(timer.scheduleStatus.next);

  const now = context.bindings.schedulerCurrentRunIn && context.bindings.schedulerCurrentRunIn.run 
    ? moment.utc(context.bindings.schedulerCurrentRunIn.run)
    : veryNow;
  
  let configs = null;
  try {
    const response = await axios.post(process.env["SCHEDULER_CONFIGS_URL"]);
    configs = response.data;
    context.bindings.schedulerConfigsOut = configs;  // cache config in blob
  } catch {
    configs = context.bindings.schedulerConfigsIn;   // fetch cached config in case of request error
  }

  context.bindings.schedulerCurrentRunOut = { run: nextRun.toISOString() };
  if (!configs || !Array.isArray(configs) || !configs.length) {
    context.done();
    return;
  }

  // Prepare URLs to execute
  const apps = {};
  configs.forEach(i => {
    apps[i.domain] = { blocksToCall: [], fallback: i.slackFallback };
    const app = apps[i.domain];

    if ((!i.enabled || !i.tasks || !i.tasks.length) && i.keepAlive) {
      app.blocksToCall.push({ block: 'KeepAlive', cron: '-', disableAlerts: !(i.keepAliveAlerts || false) });
      return;
    }

    const tasks = i.tasks.filter(t => t.enabled);
    if (!tasks.length && i.keepAlive) {
      app.blocksToCall.push({ block: 'KeepAlive', cron: '-', disableAlerts: !(i.keepAliveAlerts || false) });
      return;
    }

    tasks.forEach(t => {
      try {
        const cron = later.parse.cron(t.cron.replace('  ', ' ').trim(), true);
        const taskPrev = moment.utc(later.schedule(cron).prev(1));
        const taskNext = moment.utc(later.schedule(cron).next(1));
        const mustExecute = (taskNext.isSameOrAfter(now) && taskNext.isBefore(nextRun)) || taskPrev.isSame(now);
        if (mustExecute) {
          app.blocksToCall.push({ block: t.block, cron: t.cron, disableAlerts: t.disableAlerts });
        }
      } catch (ex) {
        context.log.error(`${t.block}: ` + ex.message);
      }
    });

    if (!app.blocksToCall.length && i.keepAlive) {
      app.blocksToCall.push({ block: 'KeepAlive', cron: '-', disableAlerts: !(i.keepAliveAlerts || false) });
      return;
    }
  });

  // Flatten apps
  const appsFlatten = [];
  Object.keys(apps).forEach(app => {
    apps[app].blocksToCall.forEach(i => {
      appsFlatten.push({
        app,
        fallback: apps[app].fallback,
        block: i.block,
        cron: i.cron,
        disableAlerts: i.disableAlerts || false,
        url: getBlockUrl(i.block, app),
      });
    });
  });

  // Call blocks
  const blockPromises = appsFlatten.map(i => axios.get(i.url));
  const results = await Promise.allSettled(blockPromises);

  // Log to AWS DynamoDB
  const timestamp = moment().valueOf();
  const logItems = results.map((i, ix) => {
    const app = appsFlatten[ix];
    let status = i.status;
    const response = status === 'fulfilled' ? i.value : (i.reason.response ? i.reason.response : { status: i.reason.code, headers: null, data: i.reason.message });
    if (app.block === 'KeepAlive' && status === 'fulfilled') {
      status = isKeepAliveSuccess(response.headers, response.data) ? 'fulfilled' : 'rejected';
    }

    return prepareLogItem(app.app, app.cron, app.block, status === 'rejected' ? 'Failed' : 'Success', response.status, response.headers, response.data, timestamp + ix, response.duration || 0);
  });

  try {
    await log(logItems);
  } catch (ex) {
    context.log.error(ex.message);
  }

  // prepare fallbacks
  const fallbacks = [];
  results.forEach((i, ix) => {
    if (!appsFlatten[ix].fallback || appsFlatten[ix].disableAlerts) {
      return;
    }

    if (i.status === 'rejected') {
      fallbacks.push({ block: appsFlatten[ix].block, fallback: appsFlatten[ix].fallback });
    } else if (appsFlatten[ix].block === 'KeepAlive') {
      const response = i.value;
      if (!isKeepAliveSuccess(response.headers, response.data)) {
        fallbacks.push({ block: appsFlatten[ix].block, fallback: appsFlatten[ix].fallback });
      }
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
