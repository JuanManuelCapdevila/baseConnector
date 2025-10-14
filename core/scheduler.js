// core/scheduler.js
const logger = require('./logger');
const KafkaPublisher = require('./kafkaPublisher');
const StrategyFactory = require('./strategyFactory');
const { of, from, forkJoin, timer, throwError, Subject } = require('rxjs');
const { switchMap, tap, catchError, takeUntil, exhaustMap, map } = require('rxjs/operators');

/**
 * Scheduler que orquesta la ejecuci�n peri�dica de las estrategias
 * Maneja m�ltiples fuentes y sus intervalos de ejecuci�n
 */
class Scheduler {
  constructor(sourcesConfig) {
    this.sourcesConfig = sourcesConfig;
    this.kafkaPublisher = new KafkaPublisher();
    this.strategyFactory = new StrategyFactory();
    this.strategies = [];
    this.isRunning = false;
    this.shutdown$ = new Subject();
  }

  /**
   * Inicializa y arranca el scheduler
   * @returns {Observable<void>}
   */
  start() {
    if (this.isRunning) {
      logger.warn('Scheduler ya est� en ejecuci�n');
      return of(undefined);
    }

    return of(undefined).pipe(
      tap(() => logger.info('Conectando a Kafka...')),
      switchMap(() => this.kafkaPublisher.connect()),
      tap(() => {
        logger.info('Creando estrategias...');
        this.strategies = this.strategyFactory.createStrategies(this.sourcesConfig);
        if (this.strategies.length === 0) {
          throw new Error('No se pudo crear ninguna estrategia vlida');
        }
      }),
      tap(() => {
        this.strategies.forEach(strategy => this.scheduleStrategy(strategy));
        this.isRunning = true;
        logger.info(`Scheduler iniciado con ${this.strategies.length} estrategias activas`);
      }),
      catchError(error => {
        logger.error(`Error iniciando scheduler: ${error.message}`);
        return throwError(() => error);
      })
    );
  }

  /**
   * Programa la ejecuci�n peri�dica de una estrategia
   * @param {BaseStrategy} strategy - Estrategia a programar
   */
  scheduleStrategy(strategy) {
    const interval = strategy.config.interval || 60000;

    logger.info(`Estrategia '${strategy.name}' programada cada ${interval / 1000}s`);

    timer(0, interval).pipe(
      takeUntil(this.shutdown$),
      exhaustMap(() => this.executeStrategy(strategy))
    ).subscribe({
      error: err => logger.error(`[${strategy.name}] Flujo de ejecución detenido por error: ${err.message}`)
    });
  }

  /**
   * Ejecuta una estrategia individual
   * @param {BaseStrategy} strategy - Estrategia a ejecutar
   * @returns {Observable<void>}
   */
  executeStrategy(strategy) {
    return strategy.execute(this.kafkaPublisher).pipe(
      tap(result => {
        if (result.skipped) {
          logger.debug(`[${strategy.name}] ${result.reason}`);
        } else if (result.success) {
          logger.info(`[${strategy.name}] ${result.recordsProcessed} registros procesados en ${result.duration}ms`);
        }
      }),
      catchError(error => {
        // El error ya fue logueado dentro de strategy.execute
        // Solo retornamos un observable vacío para que el timer no se detenga.
        return of(undefined);
      })
    );
  }

  /**
   * Detiene el scheduler y limpia recursos
   * @returns {Observable<void>}
   */
  stop() {
    if (!this.isRunning) {
      return of(undefined);
    }

    logger.info('Deteniendo scheduler...');

    // 1. Emitir señal de cierre para detener todos los timers
    this.shutdown$.next();
    this.shutdown$.complete();

    // 2. Desconectar Kafka
    return this.kafkaPublisher.disconnect().pipe(
      tap(() => {
        this.isRunning = false;
        logger.info('Scheduler detenido');
      })
    );
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
    };
  }

  /**
   * Ejecuta manualmente una estrategia espec�fica
   * @param {string} strategyName - Nombre de la estrategia
   * @returns {Observable<Object>}
   */
  executeManually(strategyName) {
    const strategy = this.strategies.find(s => s.name === strategyName);

    if (!strategy) {
      return throwError(() => new Error(`Estrategia '${strategyName}' no encontrada`));
    }

    logger.info(`Ejecutando manualmente estrategia '${strategyName}'`);
    return this.executeStrategy(strategy);
  }
}

module.exports = Scheduler;
