// core/kafkaPublisher.js
const { Kafka, Partitioners, logLevel } = require('kafkajs');
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
   * @returns {Promise<void>}
   */
  async connect() {
    if (this.isConnected) {
      logger.warn('KafkaPublisher ya está conectado');
      return;
    }

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

      // Seleccionar partitioner según configuración
      // - DefaultPartitioner: Distribuye uniformemente (round-robin cuando no hay key)
      // - LegacyPartitioner: Usa hash de key (compatible con versiones antiguas)
      const partitioner = this.config.producer?.partitioner === 'default'
        ? Partitioners.DefaultPartitioner
        : Partitioners.LegacyPartitioner;

      this.producer = this.kafka.producer({
        createPartitioner: partitioner,
        maxInFlightRequests: this.config.producer?.maxInFlightRequests || 5,
        idempotent: this.config.producer?.idempotent || true,
        transactionalId: this.config.producer?.transactionalId
      });

      await this.producer.connect();
      this.isConnected = true;

      logger.info(' Kafka Producer conectado exitosamente');
    } catch (error) {
      logger.error(`Error conectando a Kafka: ${error.message}`);
      throw error;
    }
  }

  /**
   * Desconecta del broker de Kafka
   * @returns {Promise<void>}
   */
  async disconnect() {
    if (!this.isConnected) {
      return;
    }

    try {
      await this.producer.disconnect();
      this.isConnected = false;
      logger.info('Kafka Producer desconectado');
    } catch (error) {
      logger.error(`Error desconectando de Kafka: ${error.message}`);
      throw error;
    }
  }

  /**
   * Publica un mensaje o batch de mensajes a un tópico de Kafka
   * @param {string} topic - Nombre del tópico
   * @param {Object|Array} data - Datos a publicar
   * @param {Object} metadata - Metadata adicional (source, type, etc)
   * @returns {Promise<void>}
   */
  async publish(topic, data, metadata = {}) {
    if (!this.isConnected) {
      throw new Error('KafkaPublisher no está conectado. Llama a connect() primero.');
    }

    try {
      const messages = await this.prepareMessages(data, metadata);

      await this.producer.send({
        topic,
        messages
      });

      logger.debug(`Mensaje publicado a tópico '${topic}': ${messages.length} mensaje(s)`);
    } catch (error) {
      logger.error(`Error publicando a Kafka: ${error.message}`);
      throw error;
    }
  }

  /**
   * Prepara los mensajes en el formato requerido por Kafka
   * @param {Object|Array} data - Datos a formatear
   * @param {Object} metadata - Metadata adicional
   * @returns {Promise<Array>}
   */
  async prepareMessages(data, metadata) {
    const dataArray = Array.isArray(data) ? data : [data];

    const messages = await Promise.all(
      dataArray.map(async (item) => {
        const formattedData = await dataFormatter.formatForKafka(
          item,
          metadata.source || 'unknown',
          metadata.type || 'data'
        );

        return {
          key: this.generateKey(item, metadata),
          value: JSON.stringify(formattedData),
          headers: {
            source: metadata.source || 'unknown',
            type: metadata.type || 'data',
            timestamp: new Date().toISOString()
          }
        };
      })
    );

    return messages;
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
   * @returns {Promise<void>}
   */
  async publishBatch(topicMessages) {
    if (!this.isConnected) {
      throw new Error('KafkaPublisher no está conectado. Llama a connect() primero.');
    }

    try {
      const batch = await Promise.all(
        topicMessages.map(async ({ topic, messages, metadata }) => ({
          topic,
          messages: await this.prepareMessages(messages, metadata)
        }))
      );

      await this.producer.sendBatch({ topicMessages: batch });

      logger.debug(`Batch publicado: ${batch.length} tópico(s)`);
    } catch (error) {
      logger.error(`Error publicando batch a Kafka: ${error.message}`);
      throw error;
    }
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
