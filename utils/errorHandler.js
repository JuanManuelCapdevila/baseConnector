// utils/errorHandler.js
const logger = require('../core/logger');

class ErrorHandler {
  constructor() {
    this.sentry = null;
    this.metricsCounter = null;
    this.sentryEnabled = process.env.ENABLE_SENTRY === 'true';
    this.prometheusEnabled = process.env.ENABLE_PROMETHEUS === 'true';
    this.initializeMonitoring();
  }

  /**
   * Inicializa herramientas de monitoreo (Sentry, Prometheus)
   */
  initializeMonitoring() {
    // Inicializar Sentry si está habilitado
    if (this.sentryEnabled) {
      try {
        if (!process.env.SENTRY_DSN) {
          logger.warn('ENABLE_SENTRY=true pero falta SENTRY_DSN');
          return;
        }

        const Sentry = require('@sentry/node');
        Sentry.init({
          dsn: process.env.SENTRY_DSN,
          environment: process.env.NODE_ENV || 'development',
          tracesSampleRate: parseFloat(process.env.SENTRY_TRACES_SAMPLE_RATE || '1.0')
        });
        this.sentry = Sentry;
        logger.info(' Sentry inicializado correctamente');
      } catch (error) {
        logger.error(`Error inicializando Sentry: ${error.message}`);
      }
    }

    // Inicializar Prometheus si está habilitado
    if (this.prometheusEnabled) {
      try {
        const client = require('prom-client');
        this.metricsCounter = new client.Counter({
          name: 'connector_errors_total',
          help: 'Total de errores por contexto y tipo',
          labelNames: ['context', 'error_type', 'recoverable']
        });
        logger.info(' Métricas de Prometheus inicializadas');
      } catch (error) {
        logger.error(`Error inicializando Prometheus: ${error.message}`);
      }
    }
  }

  /**
   * Maneja errores de forma centralizada
   * @param {Error} error - El error a manejar
   * @param {string} context - Contexto donde ocurrió el error
   * @param {Object} extra - Información adicional
   * @returns {Object} - Objeto con información del error
   */
  handle(error, context = 'General', extra = {}) {
    const errorInfo = {
      context,
      message: error.message,
      stack: error.stack,
      timestamp: new Date().toISOString(),
      type: error.constructor.name,
      recoverable: this.isRecoverable(error),
      ...extra
    };

    logger.error(`[${context}] ${error.message}`);
    logger.debug(`Stack trace: ${error.stack}`);

    // Enviar a Sentry si está habilitado
    if (this.sentryEnabled && this.sentry) {
      this.sentry.captureException(error, {
        tags: { context },
        extra: errorInfo
      });
    }

    // Incrementar contador de Prometheus si está habilitado
    if (this.prometheusEnabled && this.metricsCounter) {
      this.metricsCounter.inc({
        context,
        error_type: error.constructor.name,
        recoverable: errorInfo.recoverable.toString()
      });
    }

    return errorInfo;
  }

  /**
   * Crea un error personalizado para HTTP
   * @param {number} statusCode - Código de estado HTTP
   * @param {string} message - Mensaje del error
   * @returns {Error}
   */
  createHttpError(statusCode, message) {
    const error = new Error(message);
    error.statusCode = statusCode;
    error.isHttpError = true;
    return error;
  }

  /**
   * Determina si un error es recuperable
   * @param {Error} error - El error a evaluar
   * @returns {boolean}
   */
  isRecoverable(error) {
    const recoverableErrors = [
      'ECONNRESET',
      'ETIMEDOUT',
      'ENOTFOUND',
      'EAI_AGAIN',
      'ECONNREFUSED'
    ];

    return (
      recoverableErrors.includes(error.code) ||
      (error.isHttpError && error.statusCode >= 500 && error.statusCode < 600) ||
      error.statusCode === 429 // Rate limit
    );
  }

  /**
   * Maneja errores de estrategias
   * @param {Error} error - El error a manejar
   * @param {string} strategyName - Nombre de la estrategia
   * @param {string} sourceName - Nombre de la fuente
   */
  handleStrategyError(error, strategyName, sourceName) {
    const context = `Strategy[${strategyName}] Source[${sourceName}]`;
    const errorInfo = this.handle(error, context, { strategyName, sourceName });

    if (errorInfo.recoverable) {
      logger.warn(`Error recuperable en ${context}. Se reintentará en la próxima ejecución.`);
    } else {
      logger.error(`Error no recuperable en ${context}. Requiere atención.`);
    }

    return errorInfo;
  }
}

module.exports = new ErrorHandler();
