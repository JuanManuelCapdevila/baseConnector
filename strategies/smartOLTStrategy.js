// strategies/smartOLTStrategy.js
const BaseStrategy = require('../core/baseStrategy');
const logger = require('../core/logger');
const { of } = require('rxjs');
const { map, delay, switchMap } = require('rxjs/operators');

/**
 * Estrategia para extraer datos de dispositivos (ONUs) desde SmartOLT.
 * Realiza una petición para obtener el listado completo de dispositivos.
 */
class SmartOLTStrategy extends BaseStrategy {
  constructor(config) {
    super(config);
  }

  /**
   * Extrae la lista de ONUs desde la API de SmartOLT.
   * @returns {Observable<Array>}
   */
  extract() {
    // Introducimos un retardo configurable para ser respetuosos con la API.
    const executionDelay = this.config.executionDelay || 0;
    
    return of(undefined).pipe(
      delay(executionDelay),
      switchMap(() => {
        // Asumimos un endpoint estándar para obtener las ONUs.
        // Este endpoint podría necesitar ajuste según la API real de SmartOLT.
        const endpoint = '/api/onus';
        logger.info(`[${this.name}] Extrayendo ONUs desde ${this.config.url}${endpoint}`);
        
        // makeRequest ya maneja la autenticación 'basic' y los reintentos (incluyendo 429).
        return this.makeRequest(endpoint, { method: 'GET' });
      }),
      // Asumimos que la respuesta de la API es un objeto con una propiedad 'onus' que es un array.
      map(response => response.onus || [])
    );
  }

  /**
   * Transforma los datos crudos de las ONUs al formato estándar.
   * @param {Array} rawOnus - Array de ONUs crudas desde la API.
   * @returns {Array}
   */
  transform(rawOnus) {
    if (!Array.isArray(rawOnus)) {
      logger.warn(`[${this.name}] transform() esperaba un array de ONUs.`);
      return [];
    }

    const transformed = rawOnus.map(onu => this.defaultMapping(onu));
    logger.info(`[${this.name}] Transformados ${transformed.length} dispositivos.`);
    return transformed;
  }

  /**
   * Mapeo por defecto para un dispositivo ONU de SmartOLT.
   * @param {Object} onu - El objeto de ONU crudo.
   * @returns {Object}
   */
  defaultMapping(onu) {
    return {
      deviceId: onu.id,
      board: onu.board,
      port: onu.port,
      onuType: onu.onu_type,
      serialNumber: onu.sn,
      status: onu.status, // ej: 'online', 'offline'
      zone: onu.zone,
      name: onu.name,
      signal: onu.signal,
      // ... cualquier otro campo relevante que quieras incluir
    };
  }
}

module.exports = SmartOLTStrategy;