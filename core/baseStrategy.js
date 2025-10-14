// core/baseStrategy.js
const logger = require('./logger');
const errorHandler = require('../utils/errorHandler');
const httpClient = require('../utils/httpClient');
const dataFormatter = require('../utils/dataFormatter');
const { of, from, throwError, EMPTY } = require('rxjs');
const { switchMap, map, tap, catchError, finalize } = require('rxjs/operators');

/**
 * Clase base abstracta para todas las estrategias de extracci�n de datos
 * Cada fuente externa debe extender esta clase e implementar los m�todos abstractos
 */
class BaseStrategy {
  /**
   * @param {Object} config - Configuraci�n de la fuente
   * @param {string} config.name - Nombre de la fuente
   * @param {string} config.type - Tipo de estrategia
   * @param {string} config.url - URL base del API
   * @param {Object} config.auth - Configuraci�n de autenticaci�n
   * @param {number} config.interval - Intervalo de ejecuci�n en ms
   * @param {Object} config.mapping - Schema de mapeo de datos
   */
  constructor(config) {
    if (this.constructor === BaseStrategy) {
      throw new Error('BaseStrategy es una clase abstracta y no puede ser instanciada directamente');
    }

    this.config = config;
    this.name = config.name;
    this.type = config.type;
    this.isRunning = false;
    this.lastExecution = null;
    this.lastError = null;
    this.executionCount = 0;
  }

  /**
   * M�todo abstracto: Debe ser implementado por cada estrategia
   * Extrae datos de la fuente externa
   * @returns {Observable<Array|Object>}
   */
  extract() {
    throw new Error(`El m�todo extract() debe ser implementado en ${this.constructor.name}`);
  }

  /**
   * M�todo abstracto: Debe ser implementado por cada estrategia
   * Transforma los datos extra�dos al formato deseado
   * @param {*} rawData - Datos crudos extra�dos
   * @returns {Array|Object}
   */
  transform(rawData) {
    throw new Error(`El m�todo transform() debe ser implementado en ${this.constructor.name}`);
  }

  /**
   * M�todo del ciclo ETL completo: Extract -> Transform -> Load (via Kafka)
   * Este m�todo orquesta todo el proceso y maneja errores
   * @param {Object} kafkaPublisher - Instancia del publicador de Kafka
   * @returns {Observable<Object>}
   */
  execute(kafkaPublisher) {
    if (this.isRunning) {
      logger.warn(`[${this.name}] Ejecuci�n en progreso, saltando esta iteraci�n`);
      return of({ skipped: true, reason: 'Ejecucin en progreso' });
    }

    const startTime = Date.now();

    return of(undefined).pipe(
      tap(() => {
        this.isRunning = true;
        logger.info(`[${this.name}] Iniciando extraccin de datos`);
      }),
      // 1. Extract
      switchMap(() => from(this.extract())), // `from` convierte Promise a Observable si es necesario
      switchMap(rawData => {
        if (!rawData || (Array.isArray(rawData) && rawData.length === 0)) {
          logger.info(`[${this.name}] No hay datos para procesar`);
          return of({ success: true, recordsProcessed: 0, duration: Date.now() - startTime });
        }

        // 2. Transform
        const transformedData = this.transform(rawData);

        // 3. Load
        const topic = this.config.kafkaTopic || `connector.${this.type}.${this.name}`;
        return kafkaPublisher.publish(topic, transformedData, {
          source: this.name,
          type: this.type
        }).pipe(
          map(() => {
            const duration = Date.now() - startTime;
            const recordCount = Array.isArray(transformedData) ? transformedData.length : 1;
            return { success: true, recordsProcessed: recordCount, duration };
          })
        );
      }),
      tap(result => {
        if (result.success) {
          this.lastExecution = new Date();
          this.lastError = null;
          this.executionCount++;
        }
      }),
      catchError(error => {
        const duration = Date.now() - startTime;
        this.lastError = errorHandler.handleStrategyError(error, this.constructor.name, this.name);
        const errorResult = { success: false, error: this.lastError, duration };
        // Devolvemos un observable que emite el error y completa, para no romper la cadena del scheduler
        return of(errorResult);
      }),
      finalize(() => {
        this.isRunning = false;
      })
    );
  }

  /**
   * M�todo helper para hacer peticiones HTTP autenticadas
   * @param {string} endpoint - Endpoint a consumir (relativo a la URL base)
   * @param {Object} options - Opciones adicionales de la petici�n
   * @returns {Observable<Object>}
   */
  makeRequest(endpoint, options = {}) {
    const url = `${this.config.url}${endpoint}`;
    const requestConfig = {
      ...options,
      headers: {
        ...this.getAuthHeaders(),
        ...options.headers
      },
      url
    };

    return httpClient.request(requestConfig);
  }

  /**
   * Obtiene los headers de autenticaci�n seg�n la configuraci�n
   * @returns {Object}
   */
  getAuthHeaders() {
    const { auth } = this.config;

    if (!auth || !auth.type) {
      return {};
    }

    switch (auth.type) {
      case 'bearer':
        return { Authorization: `Bearer ${auth.token}` };

      case 'apikey':
        return { [auth.headerName || 'X-API-Key']: auth.apiKey };

      case 'basic':
        const credentials = Buffer.from(`${auth.username}:${auth.password}`).toString('base64');
        return { Authorization: `Basic ${credentials}` };

      default:
        logger.warn(`[${this.name}] Tipo de autenticaci�n no soportado: ${auth.type}`);
        return {};
    }
  }

  /**
   * Normaliza datos usando el schema de mapeo configurado
   * @param {*} data - Datos a normalizar
   * @returns {*}
   */
  normalize(data) {
    if (!this.config.mapping) {
      return data;
    }

    return dataFormatter.normalize(data, this.config.mapping);
  }

  /**
   * Valida que los datos tengan los campos requeridos
   * @param {Object} data - Datos a validar
   * @param {Array<string>} requiredFields - Campos requeridos
   * @returns {boolean}
   */
  validate(data, requiredFields) {
    const validation = dataFormatter.validate(data, requiredFields);

    if (!validation.valid) {
      logger.warn(`[${this.name}] Validaci�n fallida. Campos faltantes: ${validation.missing.join(', ')}`);
    }

    return validation.valid;
  }

  /**
   * Obtiene el estado actual de la estrategia
   * @returns {Object}
   */
  getStatus() {
    return {
      name: this.name,
      type: this.type,
      isRunning: this.isRunning,
      lastExecution: this.lastExecution,
      lastError: this.lastError,
      executionCount: this.executionCount,
      interval: this.config.interval
    };
  }
}

module.exports = BaseStrategy;
