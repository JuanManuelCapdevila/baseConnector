// core/logger.js
const log4js = require('log4js');
const path = require('path');

log4js.configure({
  appenders: {
    console: {
      type: 'console',
      layout: {
        type: 'pattern',
        pattern: '%[[%d{yyyy-MM-dd hh:mm:ss}] [%p] [%c]%] %m'
      }
    },
    file: {
      type: 'dateFile',
      filename: path.join(__dirname, '..', 'logs', 'connector.log'),
      pattern: '-yyyy-MM-dd',
      keepFileExt: true,
      compress: true,
      layout: {
        type: 'pattern',
        pattern: '[%d{yyyy-MM-dd hh:mm:ss}] [%p] [%c] %m'
      }
    },
    error: {
      type: 'dateFile',
      filename: path.join(__dirname, '..', 'logs', 'error.log'),
      pattern: '-yyyy-MM-dd',
      keepFileExt: true,
      compress: true,
      layout: {
        type: 'pattern',
        pattern: '[%d{yyyy-MM-dd hh:mm:ss}] [%p] [%c] %m'
      }
    },
    errorFilter: {
      type: 'logLevelFilter',
      appender: 'error',
      level: 'error'
    }
  },
  categories: {
    default: {
      appenders: ['console', 'file', 'errorFilter'],
      level: 'debug'
    }
  }
});

const logger = log4js.getLogger('BaseConnector');

module.exports = logger;
