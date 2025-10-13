// core/baseStrategy.js
const logger = require('./logger');
const errorHandler = require('../utils/errorHandler');
const httpClient = require('../utils/httpClient');
const dataFormatter = require('../utils/dataFormatter');

/**
 * Clase base abstracta para todas las estrategias de extracción de datos
 * Cada fuente externa debe extender esta clase e implementar los métodos abstractos
 */
class BaseStrategy {
  /**
   * @param {Object} config - Configuración de la fuente
   * @param {string} config.name - Nombre de la fuente
   * @param {string} config.type - Tipo de estrategia
   * @param {string} config.url - URL base del API
   * @param {Object} config.auth - Configuración de autenticación
   * @param {number} config.interval - Intervalo de ejecución en ms
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
   * Método abstracto: Debe ser implementado por cada estrategia
   * Extrae datos de la fuente externa
   * @returns {Promise<Array|Object>}
   */
  async extract() {
    throw new Error(`El método extract() debe ser implementado en ${this.constructor.name}`);
  }

  /**
   * Método abstracto: Debe ser implementado por cada estrategia
   * Transforma los datos extraídos al formato deseado
   * @param {*} rawData - Datos crudos extraídos
   * @returns {Promise<Array|Object>}
   */
  async transform(rawData) {
    throw new Error(`El método transform() debe ser implementado en ${this.constructor.name}`);
  }

  /**
   * Método del ciclo ETL completo: Extract -> Transform -> Load (via Kafka)
   * Este método orquesta todo el proceso y maneja errores
   * @param {Object} kafkaPublisher - Instancia del publicador de Kafka
   * @returns {Promise<Object>}
   */
  async execute(kafkaPublisher) {
    if (this.isRunning) {
      logger.warn(`[${this.name}] Ejecución en progreso, saltando esta iteración`);
      return { skipped: true, reason: 'Ejecución en progreso' };
    }

    this.isRunning = true;
    const startTime = Date.now();

    try {
      logger.info(`[${this.name}] Iniciando extracción de datos`);

      // 1. Extract: Obtener datos de la fuente
      const rawData = await this.extract();

      if (!rawData || (Array.isArray(rawData) && rawData.length === 0)) {
        logger.info(`[${this.name}] No hay datos para procesar`);
        return { success: true, recordsProcessed: 0 };
      }

      // 2. Transform: Transformar datos
      const transformedData = await this.transform(rawData);

      // 3. Load: Publicar a Kafka
      const topic = this.config.kafkaTopic || `connector.${this.type}.${this.name}`;
      await kafkaPublisher.publish(topic, transformedData, {
        source: this.name,
        type: this.type
      });

      const duration = Date.now() - startTime;
      const recordCount = Array.isArray(transformedData) ? transformedData.length : 1;

      logger.info(`[${this.name}] Ejecución exitosa: ${recordCount} registros en ${duration}ms`);

      this.lastExecution = new Date();
      this.lastError = null;
      this.executionCount++;

      return {
        success: true,
        recordsProcessed: recordCount,
        duration
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      this.lastError = errorHandler.handleStrategyError(error, this.constructor.name, this.name);

      return {
        success: false,
        error: this.lastError,
        duration
      };
    } finally {
      this.isRunning = false;
    }
  }

  /**
   * Método helper para hacer peticiones HTTP autenticadas
   * @param {string} endpoint - Endpoint a consumir (relativo a la URL base)
   * @param {Object} options - Opciones adicionales de la petición
   * @returns {Promise<Object>}
   */
  async makeRequest(endpoint, options = {}) {
    const url = `${this.config.url}${endpoint}`;
    const config = {
      ...options,
      headers: {
        ...this.getAuthHeaders(),
        ...options.headers
      }
    };

    return httpClient.request({ ...config, url });
  }

  /**
   * Obtiene los headers de autenticación según la configuración
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
        logger.warn(`[${this.name}] Tipo de autenticación no soportado: ${auth.type}`);
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
      logger.warn(`[${this.name}] Validación fallida. Campos faltantes: ${validation.missing.join(', ')}`);
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
