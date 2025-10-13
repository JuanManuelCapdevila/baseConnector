// core/scheduler.js
const logger = require('./logger');
const KafkaPublisher = require('./kafkaPublisher');
const StrategyFactory = require('./strategyFactory');

/**
 * Scheduler que orquesta la ejecución periódica de las estrategias
 * Maneja múltiples fuentes y sus intervalos de ejecución
 */
class Scheduler {
  constructor(sourcesConfig) {
    this.sourcesConfig = sourcesConfig;
    this.kafkaPublisher = new KafkaPublisher();
    this.strategyFactory = new StrategyFactory();
    this.strategies = [];
    this.timers = new Map();
    this.isRunning = false;
  }

  /**
   * Inicializa y arranca el scheduler
   * @returns {Promise<void>}
   */
  async start() {
    if (this.isRunning) {
      logger.warn('Scheduler ya está en ejecución');
      return;
    }

    try {
      // 1. Conectar a Kafka
      logger.info('Conectando a Kafka...');
      await this.kafkaPublisher.connect();

      // 2. Crear estrategias desde la configuración
      logger.info('Creando estrategias...');
      this.strategies = this.strategyFactory.createStrategies(this.sourcesConfig);

      if (this.strategies.length === 0) {
        throw new Error('No se pudo crear ninguna estrategia válida');
      }

      // 3. Programar ejecuciones periódicas
      this.strategies.forEach(strategy => {
        this.scheduleStrategy(strategy);
      });

      this.isRunning = true;
      logger.info(` Scheduler iniciado con ${this.strategies.length} estrategias activas`);
    } catch (error) {
      logger.error(`Error iniciando scheduler: ${error.message}`);
      throw error;
    }
  }

  /**
   * Programa la ejecución periódica de una estrategia
   * @param {BaseStrategy} strategy - Estrategia a programar
   */
  scheduleStrategy(strategy) {
    const interval = strategy.config.interval || 60000; // Default: 1 minuto

    // Ejecutar inmediatamente
    this.executeStrategy(strategy);

    // Programar ejecuciones periódicas
    const timer = setInterval(() => {
      this.executeStrategy(strategy);
    }, interval);

    this.timers.set(strategy.name, timer);

    logger.info(`Estrategia '${strategy.name}' programada cada ${interval / 1000}s`);
  }

  /**
   * Ejecuta una estrategia individual
   * @param {BaseStrategy} strategy - Estrategia a ejecutar
   * @returns {Promise<void>}
   */
  async executeStrategy(strategy) {
    try {
      const result = await strategy.execute(this.kafkaPublisher);

      if (result.skipped) {
        logger.debug(`[${strategy.name}] ${result.reason}`);
      } else if (result.success) {
        logger.info(`[${strategy.name}]  ${result.recordsProcessed} registros procesados en ${result.duration}ms`);
      } else {
        logger.error(`[${strategy.name}] L Error en ejecución`);
      }
    } catch (error) {
      logger.error(`[${strategy.name}] Error inesperado: ${error.message}`);
    }
  }

  /**
   * Detiene el scheduler y limpia recursos
   * @returns {Promise<void>}
   */
  async stop() {
    if (!this.isRunning) {
      return;
    }

    logger.info('Deteniendo scheduler...');

    // 1. Cancelar todos los timers
    this.timers.forEach((timer, name) => {
      clearInterval(timer);
      logger.debug(`Timer de '${name}' cancelado`);
    });
    this.timers.clear();

    // 2. Desconectar Kafka
    await this.kafkaPublisher.disconnect();

    this.isRunning = false;
    logger.info(' Scheduler detenido');
  }

  /**
   * Obtiene el estado del scheduler y todas las estrategias
   * @returns {Object}
   */
  getStatus() {
    return {
      isRunning: this.isRunning,
      kafkaConnected: this.kafkaPublisher.isHealthy(),
      strategies: this.strategies.map(s => s.getStatus()),
      activeTimers: this.timers.size
    };
  }

  /**
   * Ejecuta manualmente una estrategia específica
   * @param {string} strategyName - Nombre de la estrategia
   * @returns {Promise<Object>}
   */
  async executeManually(strategyName) {
    const strategy = this.strategies.find(s => s.name === strategyName);

    if (!strategy) {
      throw new Error(`Estrategia '${strategyName}' no encontrada`);
    }

    logger.info(`Ejecutando manualmente estrategia '${strategyName}'`);
    return await strategy.execute(this.kafkaPublisher);
  }

  /**
   * Pausa la ejecución de una estrategia específica
   * @param {string} strategyName - Nombre de la estrategia
   */
  pauseStrategy(strategyName) {
    const timer = this.timers.get(strategyName);

    if (!timer) {
      throw new Error(`No se encontró timer para estrategia '${strategyName}'`);
    }

    clearInterval(timer);
    this.timers.delete(strategyName);
    logger.info(`Estrategia '${strategyName}' pausada`);
  }

  /**
   * Reanuda la ejecución de una estrategia pausada
   * @param {string} strategyName - Nombre de la estrategia
   */
  resumeStrategy(strategyName) {
    const strategy = this.strategies.find(s => s.name === strategyName);

    if (!strategy) {
      throw new Error(`Estrategia '${strategyName}' no encontrada`);
    }

    if (this.timers.has(strategyName)) {
      logger.warn(`Estrategia '${strategyName}' ya está activa`);
      return;
    }

    this.scheduleStrategy(strategy);
    logger.info(`Estrategia '${strategyName}' reanudada`);
  }
}

module.exports = Scheduler;
