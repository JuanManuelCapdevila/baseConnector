// core/kafkaPublisher.js
const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { Observable, from, forkJoin, throwError, of } = require('rxjs');
const { map, switchMap, catchError, tap, mergeMap } = require('rxjs/operators');
const logger = require('./logger');
const dataFormatter = require('../utils/dataFormatter');
const fs = require('fs');
const path = require('path');

/**
 * Publisher de Kafka responsable de enviar mensajes a los tópicos
 * Soporta envío de mensajes individuales y en batch
 */
class KafkaPublisher {
  constructor() {
    this.kafka = null;
    this.producer = null;
    this.isConnected = false;
    this.config = this.loadConfig();
  }

  /**
   * Carga la configuración de Kafka desde el archivo kafka.json
   * @returns {Object}
   */
  loadConfig() {
    try {
      const configPath = path.join(__dirname, '..', 'config', 'kafka.json');
      const rawData = fs.readFileSync(configPath, 'utf-8');
      return JSON.parse(rawData);
    } catch (error) {
      logger.error(`Error cargando configuración de Kafka: ${error.message}`);
      throw error;
    }
  }

  /**
   * Conecta al broker de Kafka
   * @returns {Observable<void>}
   */
  connect() {
    if (this.isConnected) {
      logger.warn('KafkaPublisher ya está conectado');
      return of(undefined);
    }

    return of(undefined).pipe(
      switchMap(() => {
        try {
          this.kafka = new Kafka({
            clientId: this.config.clientId || 'base-connector',
            brokers: this.config.brokers || ['localhost:9092'],
            logLevel: logLevel[this.config.logLevel?.toUpperCase() || 'INFO'],
            retry: {
              initialRetryTime: this.config.retry?.initialRetryTime || 300,
              retries: this.config.retry?.retries || 8
            }
          });

          const partitioner = this.config.producer?.partitioner === 'default'
            ? Partitioners.DefaultPartitioner
            : Partitioners.LegacyPartitioner;

          this.producer = this.kafka.producer({
            createPartitioner: partitioner,
            maxInFlightRequests: this.config.producer?.maxInFlightRequests || 5,
            idempotent: this.config.producer?.idempotent || true,
            transactionalId: this.config.producer?.transactionalId
          });

          return from(this.producer.connect()).pipe(
            tap(() => {
              this.isConnected = true;
              logger.info('✓ Kafka Producer conectado exitosamente');
            })
          );
        } catch (error) {
          logger.error(`Error inicializando Kafka: ${error.message}`);
          return throwError(() => error);
        }
      }),
      catchError(error => {
        logger.error(`Error conectando a Kafka: ${error.message}`);
        return throwError(() => error);
      })
    );
  }

  /**
   * Desconecta del broker de Kafka
   * @returns {Observable<void>}
   */
  disconnect() {
    if (!this.isConnected) {
      return of(undefined);
    }

    return from(this.producer.disconnect()).pipe(
      tap(() => {
        this.isConnected = false;
        logger.info('Kafka Producer desconectado');
      }),
      catchError(error => {
        logger.error(`Error desconectando de Kafka: ${error.message}`);
        return throwError(() => error);
      })
    );
  }

  /**
   * Publica un mensaje o batch de mensajes a un tópico de Kafka
   * @param {string} topic - Nombre del tópico
   * @param {Object|Array} data - Datos a publicar
   * @param {Object} metadata - Metadata adicional (source, type, etc)
   * @returns {Observable<void>}
   */
  publish(topic, data, metadata = {}) {
    if (!this.isConnected) {
      return throwError(() => new Error('KafkaPublisher no está conectado. Llama a connect() primero.'));
    }

    return this.prepareMessages(data, metadata).pipe(
      switchMap(messages =>
        from(this.producer.send({ topic, messages })).pipe(
          tap(() => {
            logger.debug(`Mensaje publicado a tópico '${topic}': ${messages.length} mensaje(s)`);
          }),
          catchError(error => {
            logger.error(`Error publicando a Kafka: ${error.message}`);
            return throwError(() => error);
          })
        )
      )
    );
  }

  /**
   * Prepara los mensajes en el formato requerido por Kafka
   * @param {Object|Array} data - Datos a formatear
   * @param {Object} metadata - Metadata adicional
   * @returns {Observable<Array>}
   */
  prepareMessages(data, metadata) {
    const dataArray = Array.isArray(data) ? data : [data];

    const observables = dataArray.map(item =>
      dataFormatter.formatForKafka(
        item,
        metadata.source || 'unknown',
        metadata.type || 'data'
      ).pipe(
        map(formattedData => ({
          key: this.generateKey(item, metadata),
          value: JSON.stringify(formattedData),
          headers: {
            source: metadata.source || 'unknown',
            type: metadata.type || 'data',
            timestamp: new Date().toISOString()
          }
        }))
      )
    );

    return forkJoin(observables);
  }

  /**
   * Genera una clave para el mensaje (útil para particionamiento consistente)
   * Los mensajes con la misma key van siempre a la misma partición
   * @param {Object} data - Datos del mensaje
   * @param {Object} metadata - Metadata adicional
   * @returns {string}
   */
  generateKey(data, metadata) {
    // Por defecto usar el ID del dato si existe, sino el source
    if (data.id) {
      return String(data.id);
    }

    if (data._id) {
      return String(data._id);
    }

    return metadata.source || 'default';
  }

  /**
   * Publica múltiples mensajes a diferentes tópicos en batch
   * @param {Array<Object>} topicMessages - Array de { topic, messages, metadata }
   * @returns {Observable<void>}
   */
  publishBatch(topicMessages) {
    if (!this.isConnected) {
      return throwError(() => new Error('KafkaPublisher no está conectado. Llama a connect() primero.'));
    }

    const batchObservables = topicMessages.map(({ topic, messages, metadata }) =>
      this.prepareMessages(messages, metadata).pipe(
        map(preparedMessages => ({ topic, messages: preparedMessages }))
      )
    );

    return forkJoin(batchObservables).pipe(
      switchMap(batch =>
        from(this.producer.sendBatch({ topicMessages: batch })).pipe(
          tap(() => {
            logger.debug(`Batch publicado: ${batch.length} tópico(s)`);
          }),
          catchError(error => {
            logger.error(`Error publicando batch a Kafka: ${error.message}`);
            return throwError(() => error);
          })
        )
      )
    );
  }

  /**
   * Verifica el estado de la conexión
   * @returns {boolean}
   */
  isHealthy() {
    return this.isConnected;
  }
}

module.exports = KafkaPublisher;
