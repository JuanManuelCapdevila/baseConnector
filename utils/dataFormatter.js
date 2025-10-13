// utils/dataFormatter.js
const logger = require('../core/logger');

class DataFormatter {
  constructor() {
    this.useSchemaRegistry = process.env.KAFKA_USE_SCHEMA_REGISTRY === 'true';
    this.schemaRegistryClient = null;

    if (this.useSchemaRegistry) {
      this.initializeSchemaRegistry();
    }
  }

  /**
   * Inicializa el cliente de Schema Registry
   */
  initializeSchemaRegistry() {
    try {
      const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
      this.schemaRegistryClient = new SchemaRegistry({
        host: process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081'
      });
      logger.info(' Schema Registry inicializado');
    } catch (error) {
      logger.error(`Error inicializando Schema Registry: ${error.message}`);
      this.useSchemaRegistry = false;
    }
  }

  /**
   * Formatea datos en un formato estándar para Kafka
   * @param {Object} data - Datos a formatear
   * @param {string} source - Nombre de la fuente
   * @param {string} type - Tipo de dato
   * @param {Object} options - Opciones adicionales { schemaId, subject }
   * @returns {Promise<Object>}
   */
  async formatForKafka(data, source, type, options = {}) {
    const message = {
      metadata: {
        source,
        type,
        timestamp: new Date().toISOString(),
        version: '1.0'
      },
      payload: data
    };

    // Si usamos Schema Registry, codificar el mensaje
    if (this.useSchemaRegistry && this.schemaRegistryClient) {
      try {
        const subject = options.subject || `${source}-${type}-value`;
        const encodedPayload = await this.schemaRegistryClient.encode(
          options.schemaId || subject,
          message
        );
        return encodedPayload;
      } catch (error) {
        logger.error(`Error codificando con Schema Registry: ${error.message}`);
        logger.warn('Enviando mensaje sin codificar');
      }
    }

    return message;
  }

  /**
   * Decodifica un mensaje usando Schema Registry
   * @param {Buffer} encodedMessage - Mensaje codificado
   * @returns {Promise<Object>}
   */
  async decodeFromKafka(encodedMessage) {
    if (this.useSchemaRegistry && this.schemaRegistryClient) {
      try {
        return await this.schemaRegistryClient.decode(encodedMessage);
      } catch (error) {
        logger.error(`Error decodificando con Schema Registry: ${error.message}`);
        throw error;
      }
    }

    // Si no hay Schema Registry, intentar parsear como JSON
    try {
      return JSON.parse(encodedMessage.toString());
    } catch (error) {
      logger.error(`Error parseando mensaje: ${error.message}`);
      throw error;
    }
  }

  /**
   * Normaliza datos de diferentes fuentes a un formato común
   * Soporta múltiples tipos de mapeo:
   * - String: acceso directo o dot notation ("user.name")
   * - Function: transformación custom ((data) => data.user.name.toUpperCase())
   * - Object: mapeo complejo con defaultValue, transform, required
   *
   * @param {Object|Array} rawData - Datos crudos (puede ser objeto o array)
   * @param {Object} schema - Esquema de mapeo flexible
   * @returns {Object|Array}
   *
   * @example
   * // Mapeo simple con dot notation
   * schema = { userName: 'user.name', userId: 'user.id' }
   *
   * // Mapeo con función custom
   * schema = { userName: (data) => data.user.name.toUpperCase() }
   *
   * // Mapeo complejo
   * schema = {
   *   userName: {
   *     source: 'user.name',
   *     transform: (val) => val.toUpperCase(),
   *     defaultValue: 'Unknown',
   *     required: true
   *   }
   * }
   */
  normalize(rawData, schema) {
    if (!schema || typeof schema !== 'object') {
      logger.warn('No se proporcionó schema para normalización, devolviendo datos originales');
      return rawData;
    }

    // Si rawData es un array, normalizar cada elemento
    if (Array.isArray(rawData)) {
      return rawData.map(item => this.normalizeObject(item, schema));
    }

    return this.normalizeObject(rawData, schema);
  }

  /**
   * Normaliza un objeto individual
   * @private
   */
  normalizeObject(rawData, schema) {
    const normalized = {};

    for (const [targetField, mapping] of Object.entries(schema)) {
      try {
        let value;

        // Mapeo simple: string con dot notation
        if (typeof mapping === 'string') {
          value = this.getNestedValue(rawData, mapping);
        }
        // Mapeo con función custom
        else if (typeof mapping === 'function') {
          value = mapping(rawData);
        }
        // Mapeo complejo
        else if (typeof mapping === 'object') {
          const sourceValue = mapping.source
            ? this.getNestedValue(rawData, mapping.source)
            : rawData;

          value = sourceValue;

          // Aplicar transformación si existe
          if (mapping.transform && typeof mapping.transform === 'function') {
            value = mapping.transform(value, rawData);
          }

          // Aplicar valor por defecto si es null/undefined
          if ((value === null || value === undefined) && mapping.defaultValue !== undefined) {
            value = mapping.defaultValue;
          }

          // Validar campo requerido
          if (mapping.required && (value === null || value === undefined)) {
            logger.warn(`Campo requerido '${targetField}' no encontrado en datos de fuente`);
          }
        }

        normalized[targetField] = value;
      } catch (error) {
        logger.error(`Error mapeando campo '${targetField}': ${error.message}`);
        normalized[targetField] = null;
      }
    }

    return normalized;
  }

  /**
   * Obtiene un valor anidado de un objeto usando dot notation
   * @param {Object} obj - Objeto origen
   * @param {string} path - Ruta en formato 'a.b.c' o 'a[0].b'
   * @returns {*}
   */
  getNestedValue(obj, path) {
    if (!path) return obj;

    return path.split('.').reduce((current, key) => {
      // Soporte para arrays: "items[0].name"
      const arrayMatch = key.match(/^(\w+)\[(\d+)\]$/);
      if (arrayMatch) {
        const [, arrayKey, index] = arrayMatch;
        return current?.[arrayKey]?.[parseInt(index, 10)];
      }
      return current?.[key];
    }, obj);
  }

  /**
   * Limpia datos eliminando campos nulos, undefined o vacíos
   * @param {Object} data - Datos a limpiar
   * @param {boolean} removeEmpty - Si eliminar strings vacíos
   * @returns {Object}
   */
  clean(data, removeEmpty = true) {
    if (Array.isArray(data)) {
      return data
        .map(item => this.clean(item, removeEmpty))
        .filter(item => item !== null && item !== undefined);
    }

    if (data !== null && typeof data === 'object') {
      const cleaned = {};

      for (const [key, value] of Object.entries(data)) {
        const cleanedValue = this.clean(value, removeEmpty);

        const shouldInclude =
          cleanedValue !== null &&
          cleanedValue !== undefined &&
          (!removeEmpty || cleanedValue !== '');

        if (shouldInclude) {
          cleaned[key] = cleanedValue;
        }
      }

      return cleaned;
    }

    return data;
  }

  /**
   * Valida que los datos tengan los campos requeridos
   * @param {Object} data - Datos a validar
   * @param {Array<string>} requiredFields - Campos requeridos
   * @returns {Object} { valid: boolean, missing: Array<string> }
   */
  validate(data, requiredFields) {
    const missing = [];

    for (const field of requiredFields) {
      const value = this.getNestedValue(data, field);
      if (value === null || value === undefined) {
        missing.push(field);
      }
    }

    return {
      valid: missing.length === 0,
      missing
    };
  }

  /**
   * Transforma un array de datos aplicando una función a cada elemento
   * @param {Array} dataArray - Array de datos
   * @param {Function} transformFn - Función de transformación
   * @returns {Array}
   */
  transformArray(dataArray, transformFn) {
    if (!Array.isArray(dataArray)) {
      logger.warn('transformArray esperaba un array pero recibió:', typeof dataArray);
      return [];
    }

    return dataArray.map((item, index) => {
      try {
        return transformFn(item, index);
      } catch (error) {
        logger.error(`Error transformando elemento ${index}: ${error.message}`);
        return null;
      }
    }).filter(item => item !== null);
  }

  /**
   * Convierte datos a formato JSON de forma segura
   * @param {*} data - Datos a convertir
   * @param {boolean} pretty - Si formatear el JSON
   * @returns {string}
   */
  toJSON(data, pretty = false) {
    try {
      return JSON.stringify(data, null, pretty ? 2 : 0);
    } catch (error) {
      logger.error(`Error al convertir datos a JSON: ${error.message}`);
      return '{}';
    }
  }

  /**
   * Aplana un objeto anidado
   * @param {Object} obj - Objeto a aplanar
   * @param {string} prefix - Prefijo para las claves
   * @returns {Object}
   */
  flatten(obj, prefix = '') {
    const flattened = {};

    for (const [key, value] of Object.entries(obj)) {
      const newKey = prefix ? `${prefix}.${key}` : key;

      if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
        Object.assign(flattened, this.flatten(value, newKey));
      } else {
        flattened[newKey] = value;
      }
    }

    return flattened;
  }
}

module.exports = new DataFormatter();
