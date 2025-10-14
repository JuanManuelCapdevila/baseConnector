// utils/httpClient.js
const axios = require('axios');
const { defer, from, throwError, timer } = require('rxjs');
const { map, retry, catchError, tap } = require('rxjs/operators');
const logger = require('../core/logger');
const errorHandler = require('./errorHandler');

class HttpClient {
  constructor() {
    this.defaultTimeout = parseInt(process.env.HTTP_TIMEOUT || '30000', 10);
    this.maxRetries = parseInt(process.env.HTTP_MAX_RETRIES || '3', 10);
    this.retryDelay = parseInt(process.env.HTTP_RETRY_DELAY || '1000', 10);
  }

  /**
   * Realiza una petici�n HTTP con reintentos autom�ticos
   * @param {Object} config - Configuraci�n de axios
   * @returns {Observable<Object>}
   */
  request(config) {
    // `defer` asegura que la llamada a axios se haga solo al momento de la suscripción
    return defer(() => {
      const finalConfig = {
        timeout: this.defaultTimeout,
        ...config
      };
      logger.debug(`HTTP ${finalConfig.method?.toUpperCase() || 'GET'} ${finalConfig.url}`);
      // `from` convierte la promesa de axios en un Observable
      return from(axios(finalConfig));
    }).pipe(
      map(response => response.data), // Extraemos solo los datos de la respuesta
      retry({
        count: this.maxRetries,
        delay: (error, retryCount) => {
          // `retry` nos da el control para decidir si reintentar y con qué demora
          if (!this.shouldRetry(error, retryCount)) {
            // Si no debemos reintentar, lanzamos el error para que lo capture `catchError`
            return throwError(() => error);
          }
          const delay = this.calculateBackoff(retryCount - 1);
          logger.warn(`Reintentando petición (${retryCount}/${this.maxRetries}) en ${delay}ms: ${config.url}`);
          return timer(delay);
        }
      }),
      catchError(error => {
        // Centralizamos el manejo de errores aquí
        const httpError = errorHandler.createHttpError(
          error.response?.status || 500,
          error.message
        );
        httpError.originalError = error;
        // Lanzamos un nuevo error formateado para que lo consuman las estrategias
        return throwError(() => httpError);
      })
    );
  }

  /**
   * Determina si se debe reintentar la petici�n
   * @param {Error} error - Error de la petici�n
   * @param {number} retryCount - N�mero de reintentos realizados
   * @returns {boolean}
   */
  shouldRetry(error, retryCount) {
    if (retryCount + 1 >= this.maxRetries) {
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
   * @param {number} retryCount - N�mero de reintentos realizados
   * @returns {number}
   */
  calculateBackoff(retryCount) {
    return this.retryDelay * Math.pow(2, retryCount);
  }

  /**
   * Realiza una petici�n GET
   * @param {string} url - URL del recurso
   * @param {Object} config - Configuraci�n adicional
   * @returns {Observable<Object>}
   */
  get(url, config = {}) {
    return this.request({ ...config, method: 'GET', url });
  }

  /**
   * Realiza una petici�n POST
   * @param {string} url - URL del recurso
   * @param {Object} data - Datos a enviar
   * @param {Object} config - Configuraci�n adicional
   * @returns {Observable<Object>}
   */
  post(url, data, config = {}) {
    return this.request({ ...config, method: 'POST', url, data });
  }

  /**
   * Realiza una petici�n PUT
   * @param {string} url - URL del recurso
   * @param {Object} data - Datos a enviar
   * @param {Object} config - Configuraci�n adicional
   * @returns {Observable<Object>}
   */
  put(url, data, config = {}) {
    return this.request({ ...config, method: 'PUT', url, data });
  }

  /**
   * Realiza una petici�n DELETE
   * @param {string} url - URL del recurso
   * @param {Object} config - Configuraci�n adicional
   * @returns {Observable<Object>}
   */
  delete(url, config = {}) {
    return this.request({ ...config, method: 'DELETE', url });
  }
}

module.exports = new HttpClient();
