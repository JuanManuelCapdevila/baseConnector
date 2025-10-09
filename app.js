// app.js
const fs = require('fs');
const path = require('path');
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
const handleExit = async (scheduler, signal) => {
  logger.info(`Recibida señal ${signal}. Cerrando el conector...`);
  await scheduler.stop();
  process.exit(0);
};

// --- Función principal ---
(async () => {
  const config = loadConfig();
  const scheduler = new Scheduler(config.sources);

  try {
    logger.info(`🔌 Iniciando Connector Service con ${config.sources.length} fuentes activas...`);
    await scheduler.start();

    // Manejo de señales del sistema
    process.on('SIGINT', () => handleExit(scheduler, 'SIGINT'));
    process.on('SIGTERM', () => handleExit(scheduler, 'SIGTERM'));

    logger.info('✅ Servicio de conectores en ejecución.');
  } catch (err) {
    logger.error(`❌ Error crítico en la inicialización: ${err.message}`);
    process.exit(1);
  }
})();
