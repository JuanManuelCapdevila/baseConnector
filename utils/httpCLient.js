// utils/httpClient.js
const axios = require('axios');
const logger = require('../core/logger');
const errorHandler = require('./errorHandler');

class HttpClient {
  constructor() {
    this.defaultTimeout = parseInt(process.env.HTTP_TIMEOUT || '30000', 10);
    this.maxRetries = parseInt(process.env.HTTP_MAX_RETRIES || '3', 10);
    this.retryDelay = parseInt(process.env.HTTP_RETRY_DELAY || '1000', 10);
  }

  /**
   * Realiza una petición HTTP con reintentos automáticos
   * @param {Object} config - Configuración de axios
   * @param {number} retryCount - Contador de reintentos
   * @returns {Promise<Object>}
   */
  async request(config, retryCount = 0) {
    const finalConfig = {
      timeout: this.defaultTimeout,
      ...config
    };

    try {
      logger.debug(`HTTP ${config.method?.toUpperCase() || 'GET'} ${config.url}`);
      const response = await axios(finalConfig);

      if (retryCount > 0) {
        logger.info(`Petición exitosa después de ${retryCount} reintentos: ${config.url}`);
      }

      return response.data;
    } catch (error) {
      const shouldRetry = this.shouldRetry(error, retryCount);

      if (shouldRetry) {
        const delay = this.calculateBackoff(retryCount);
        logger.warn(`Reintentando petición (${retryCount + 1}/${this.maxRetries}) en ${delay}ms: ${config.url}`);

        await this.sleep(delay);
        return this.request(config, retryCount + 1);
      }

      const httpError = errorHandler.createHttpError(
        error.response?.status || 500,
        error.message
      );
      httpError.originalError = error;

      throw httpError;
    }
  }

  /**
   * Determina si se debe reintentar la petición
   * @param {Error} error - Error de la petición
   * @param {number} retryCount - Número de reintentos realizados
   * @returns {boolean}
   */
  shouldRetry(error, retryCount) {
    if (retryCount >= this.maxRetries) {
      return false;
    }

    // Reintentar en errores de red
    if (!error.response) {
      return true;
    }

    // Reintentar en errores 5xx o 429 (rate limit)
    const status = error.response.status;
    return status >= 500 || status === 429;
  }

  /**
   * Calcula el tiempo de espera con backoff exponencial
   * @param {number} retryCount - Número de reintentos realizados
   * @returns {number}
   */
  calculateBackoff(retryCount) {
    return this.retryDelay * Math.pow(2, retryCount);
  }

  /**
   * Espera un tiempo determinado
   * @param {number} ms - Milisegundos a esperar
   * @returns {Promise<void>}
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Realiza una petición GET
   * @param {string} url - URL del recurso
   * @param {Object} config - Configuración adicional
   * @returns {Promise<Object>}
   */
  async get(url, config = {}) {
    return this.request({ ...config, method: 'GET', url });
  }

  /**
   * Realiza una petición POST
   * @param {string} url - URL del recurso
   * @param {Object} data - Datos a enviar
   * @param {Object} config - Configuración adicional
   * @returns {Promise<Object>}
   */
  async post(url, data, config = {}) {
    return this.request({ ...config, method: 'POST', url, data });
  }

  /**
   * Realiza una petición PUT
   * @param {string} url - URL del recurso
   * @param {Object} data - Datos a enviar
   * @param {Object} config - Configuración adicional
   * @returns {Promise<Object>}
   */
  async put(url, data, config = {}) {
    return this.request({ ...config, method: 'PUT', url, data });
  }

  /**
   * Realiza una petición DELETE
   * @param {string} url - URL del recurso
   * @param {Object} config - Configuración adicional
   * @returns {Promise<Object>}
   */
  async delete(url, config = {}) {
    return this.request({ ...config, method: 'DELETE', url });
  }
}

module.exports = new HttpClient();
