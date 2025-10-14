// app.js
const fs = require('fs');
const path = require('path');
const { from } = require('rxjs');
const Scheduler = require('./core/scheduler');
const logger = require('./core/logger');

// --- Carga de configuración ---
const loadConfig = () => {
  try {
    const configPath = path.join(__dirname, 'config', 'sources.json');
    const rawData = fs.readFileSync(configPath, 'utf-8');
    const config = JSON.parse(rawData);

    if (!config.sources || !Array.isArray(config.sources) || config.sources.length === 0) {
      throw new Error('No se encontraron fuentes válidas en config/sources.json');
    }

    return config;
  } catch (error) {
    logger.error(`Error al cargar configuración: ${error.message}`);
    process.exit(1);
  }
};

// --- Manejo de cierre controlado ---
const handleExit = (scheduler, signal, subscription) => {
  logger.info(`Recibida señal ${signal}. Cerrando el conector...`);
  if (subscription) {
    subscription.unsubscribe();
  }
  scheduler.stop().subscribe({
    complete: () => process.exit(0),
    error: () => process.exit(1)
  });
};

// --- Función principal ---
const main = () => {
  const config = loadConfig();
  const scheduler = new Scheduler(config.sources);

  logger.info(`🔌 Iniciando Connector Service con ${config.sources.length} fuentes activas...`);

  const subscription = scheduler.start().subscribe({
    next: () => logger.info('✅ Servicio de conectores en ejecución.'),
    error: err => {
      logger.error(`❌ Error crítico en la inicialización: ${err.message}`);
      process.exit(1);
    },
  });

  process.on('SIGINT', () => handleExit(scheduler, 'SIGINT', subscription));
  process.on('SIGTERM', () => handleExit(scheduler, 'SIGTERM', subscription));
};

main();
