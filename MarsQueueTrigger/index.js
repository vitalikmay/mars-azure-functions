const axios = require('axios');

module.exports = async function (context, queueMessage) {
  let err = null, res = { status: 200 };
  try {
    const response = await axios.post(queueMessage.handler, queueMessage.body);
    res = {
      status: response.status,
      body: response.data,
    };
  } catch (error) {
    err = new Error(error.message);
    res = {
      status: 500,
      body: error.message,
    };

    if (error.response) {
      res.status = error.response.status;
    }

    context.log.error('Error: ', error);
  }

  context.done(err, res);
};