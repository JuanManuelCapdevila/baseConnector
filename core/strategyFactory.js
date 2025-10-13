// core/strategyFactory.js
const logger = require('./logger');
const path = require('path');
const fs = require('fs');

/**
 * Factory responsable de crear instancias de estrategias
 * Carga dinámicamente las clases de estrategia desde el directorio strategies/
 */
class StrategyFactory {
  constructor() {
    this.strategiesPath = path.join(__dirname, '..', 'strategies');
    this.availableStrategies = new Map();
    this.loadAvailableStrategies();
  }

  /**
   * Carga todas las estrategias disponibles en el directorio strategies/
   */
  loadAvailableStrategies() {
    try {
      const files = fs.readdirSync(this.strategiesPath);

      files.forEach(file => {
        if (file.endsWith('Strategy.js')) {
          const strategyName = file.replace('Strategy.js', '');
          const strategyPath = path.join(this.strategiesPath, file);

          try {
            const StrategyClass = require(strategyPath);
            this.availableStrategies.set(strategyName, StrategyClass);
            logger.debug(`Estrategia cargada: ${strategyName}`);
          } catch (error) {
            logger.warn(`No se pudo cargar estrategia ${strategyName}: ${error.message}`);
          }
        }
      });

      logger.info(` ${this.availableStrategies.size} estrategias disponibles: ${Array.from(this.availableStrategies.keys()).join(', ')}`);
    } catch (error) {
      logger.error(`Error cargando estrategias: ${error.message}`);
      throw error;
    }
  }

  /**
   * Crea una instancia de estrategia basada en la configuración
   * @param {Object} config - Configuración de la fuente
   * @param {string} config.type - Tipo de estrategia (debe coincidir con nombre de archivo)
   * @returns {BaseStrategy}
   */
  createStrategy(config) {
    if (!config || !config.type) {
      throw new Error('Configuración de estrategia inválida: falta el campo "type"');
    }

    const StrategyClass = this.availableStrategies.get(config.type);

    if (!StrategyClass) {
      throw new Error(
        `Estrategia '${config.type}' no encontrada. Estrategias disponibles: ${Array.from(this.availableStrategies.keys()).join(', ')}`
      );
    }

    try {
      const strategy = new StrategyClass(config);
      logger.info(`Estrategia '${config.type}' instanciada para fuente '${config.name}'`);
      return strategy;
    } catch (error) {
      logger.error(`Error creando estrategia '${config.type}': ${error.message}`);
      throw error;
    }
  }

  /**
   * Crea múltiples instancias de estrategias desde un array de configuraciones
   * @param {Array<Object>} configs - Array de configuraciones
   * @returns {Array<BaseStrategy>}
   */
  createStrategies(configs) {
    if (!Array.isArray(configs)) {
      throw new Error('createStrategies espera un array de configuraciones');
    }

    return configs.map(config => {
      try {
        return this.createStrategy(config);
      } catch (error) {
        logger.error(`Error creando estrategia para fuente '${config.name}': ${error.message}`);
        return null;
      }
    }).filter(strategy => strategy !== null);
  }

  /**
   * Lista las estrategias disponibles
   * @returns {Array<string>}
   */
  listAvailableStrategies() {
    return Array.from(this.availableStrategies.keys());
  }

  /**
   * Verifica si una estrategia está disponible
   * @param {string} type - Tipo de estrategia
   * @returns {boolean}
   */
  hasStrategy(type) {
    return this.availableStrategies.has(type);
  }
}

module.exports = StrategyFactory;
