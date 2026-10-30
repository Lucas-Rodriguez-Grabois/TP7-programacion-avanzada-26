/**
 * =================================================================================================
 * PROYECTO: RutaSur - Plataforma de Logística Orientada a Eventos
 * ARCHIVO: reprogramacion/index.js
 * RUTA: C:\Users\lrtr3\OneDrive\Desktop\facultad\programacion avanzada\TP7\reprogramacion\index.js
 * SERVICIO: Reprogramación (Gestión de Entregas Fallidas - Caso de Uso 2)
 * =================================================================================================
 * RESPONSABILIDADES:
 * 1. Consumir eventos del topic 'envios.estados' (groupId: 'reprogramacion').
 * 2. Filtrar únicamente los eventos de tipo 'ENTREGA_FALLIDA' (otros tipos se ignoran y comitean).
 * 3. Consultar MongoDB ('rutasur.envios') para verificar el contador de 'intentos' del paquete.
 *    - Manejo proactivo de condiciones de carrera mediante sondeo con reintentos para asegurar
 *      que el Proyector ya haya impactado el intento en la colección.
 * 4. Aplicar reglas de negocio:
 *    - Si intentos < 3: Emite evento 'REPROGRAMADO' a 'envios.reprogramaciones' (nueva fecha +24h sim).
 *    - Si intentos >= 3: Emite evento 'DEVOLUCION_ORIGEN' a 'envios.reprogramaciones'.
 * 5. Confirmación manual (autoCommit: false) SOLO DESPUÉS de publicar con éxito el evento resultante.
 * =================================================================================================
 */

const { Kafka, CompressionTypes } = require('kafkajs');
const { MongoClient } = require('mongodb');
const crypto = require('crypto');

// ============================================================================
// CONFIGURACIÓN DE ENTORNO
// ============================================================================
const CONFIG = {
  kafkaBrokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  kafkaGroupId: process.env.KAFKA_GROUP_ID || 'reprogramacion',
  topicEstados: 'envios.estados',
  topicReprogramaciones: 'envios.reprogramaciones',

  // Conexión a MongoDB Replica Set local (rs0)
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/rutasur?replicaSet=rs0&directConnection=true',
  dbName: 'rutasur',
  collectionName: 'envios',

  // Horas simuladas para reprogramar el próximo intento
  horasReprogramacionSimuladas: 24,

  // Configuración de reintentos para mitigar condiciones de carrera con el Proyector
  maxSondeoRetries: 6,
  sondeoDelayMs: 250,
};

// ============================================================================
// CLIENTES KAFKA Y MONGODB
// ============================================================================
const kafka = new Kafka({
  clientId: 'rutasur-reprogramacion',
  brokers: CONFIG.kafkaBrokers,
  retry: {
    retries: 8,
    initialRetryTime: 300,
  },
});

const consumer = kafka.consumer({
  groupId: CONFIG.kafkaGroupId,
  sessionTimeout: 15000,
  heartbeatInterval: 3000,
});

const producer = kafka.producer({
  allowAutoTopicCreation: true,
  idempotent: false,
});

const mongoClient = new MongoClient(CONFIG.mongoUri);

// Helper para pausas
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ============================================================================
// MITIGACIÓN DE CONDICIÓN DE CARRERA: OBTENER INTENTOS ACTUALIZADOS
// ============================================================================
/**
 * Consulta en MongoDB el documento del envío.
 * Como el Proyector y este servicio consumen 'envios.estados' en paralelo,
 * sondea brevemente hasta comprobar que el 'eventId' del fallo actual ya fue
 * registrado en el 'historial' del documento, garantizando que 'intentos' esté al día.
 */
async function obtenerIntentosActualizados(collection, shipmentId, eventId) {
  for (let intento = 1; intento <= CONFIG.maxSondeoRetries; intento++) {
    const doc = await collection.findOne({ shipmentId });

    if (doc && Array.isArray(doc.historial) && doc.historial.some((h) => h.eventId === eventId)) {
      // El Proyector ya persistió y sumó el intento
      return {
        intentos: doc.intentos || 1,
        doc,
        sincronizado: true,
      };
    }

    if (intento < CONFIG.maxSondeoRetries) {
      await sleep(CONFIG.sondeoDelayMs);
    }
  }

  // Fallback de resiliencia: si el sondeo agotó los intentos (por ejemplo, si el Proyector
  // estuvo temporalmente detenido), se calcula el incremento sobre el último valor conocido.
  const docFallback = await collection.findOne({ shipmentId });
  const intentosCalculados = (docFallback?.intentos || 0) + 1;

  console.warn(
    `⚠️ [CARRERA DETECTADA] Proyector no confirmó eventId=${eventId} para ${shipmentId} tras ${
      CONFIG.maxSondeoRetries * CONFIG.sondeoDelayMs
    }ms. Asumiendo fallback: intentos=${intentosCalculados}`
  );

  return {
    intentos: intentosCalculados,
    doc: docFallback,
    sincronizado: false,
  };
}

// ============================================================================
// LÓGICA DE NEGOCIO: REPROGRAMACIÓN / DEVOLUCIÓN
// ============================================================================
async function procesarEntregaFallida(evento, collection) {
  const { shipmentId, eventId, motivo, ocurridoEn } = evento;
  const motivoFallo = motivo || 'NO_ESPECIFICADO';

  console.log(`\n🔍 [EVALUANDO FALLO] ${shipmentId} | Motivo: ${motivoFallo} | EventId: ${eventId}`);

  // 1. Obtener la cantidad de intentos consolidada
  const { intentos } = await obtenerIntentosActualizados(collection, shipmentId, eventId);
  console.log(`📊 [INTENTOS ACUMULADOS] ${shipmentId}: ${intentos} intento(s)`);

  let eventoResultante = null;

  // 2. Regla de negocio de Reprogramación:
  // - Si intentos < 3: Se calcula una nueva fecha (+24h simuladas) y se emite REPROGRAMADO.
  // - Si intentos >= 3 (o rechazo definitivo por el cliente): Se emite DEVOLUCION_ORIGEN.
  const esDevolucion = intentos >= 3 || motivoFallo === 'RECHAZADO';

  if (!esDevolucion) {
    // Calcular nueva fecha de entrega (24 horas simuladas a partir del momento de ocurrencia del fallo)
    const fechaFalloMs = new Date(ocurridoEn).getTime();
    const nuevaFecha = new Date(fechaFalloMs + CONFIG.horasReprogramacionSimuladas * 3600 * 1000).toISOString();

    eventoResultante = {
      eventId: crypto.randomUUID(),
      shipmentId,
      tipo: 'REPROGRAMADO',
      intentoActual: intentos,
      proximoIntento: intentos + 1,
      nuevaFecha,
      motivoFallo,
      ocurridoEn: new Date().toISOString(),
      version: 1,
    };

    console.log(
      `🔄 [REPROGRAMADO] ${shipmentId}: Intento ${intentos}/3 falló por '${motivoFallo}'. Próximo intento: #${
        intentos + 1
      } para ${nuevaFecha}`
    );
  } else {
    // Superó el umbral o fue rechazado por el comprador
    const causaDevolucion = intentos >= 3 ? 'MAXIMO_INTENTOS_ALCANZADO' : 'RECHAZADO_POR_DESTINATARIO';

    eventoResultante = {
      eventId: crypto.randomUUID(),
      shipmentId,
      tipo: 'DEVOLUCION_ORIGEN',
      intentoActual: intentos,
      motivoFallo,
      motivoDevolucion: causaDevolucion,
      ocurridoEn: new Date().toISOString(),
      version: 1,
    };

    console.log(
      `🚨 [DEVOLUCION A ORIGEN] ${shipmentId}: Agotó sus intentos (${intentos}) o fue rechazado. Se cancela entrega y vuelve al comercio.`
    );
  }

  // 3. Publicar el evento resultante en 'envios.reprogramaciones'
  // Usamos SIEMPRE shipmentId como clave de partición para garantizar orden total
  await producer.send({
    topic: CONFIG.topicReprogramaciones,
    compression: CompressionTypes.None,
    messages: [
      {
        key: String(shipmentId),
        value: JSON.stringify(eventoResultante),
        headers: {
          'x-source': 'reprogramacion-service',
          'x-event-type': eventoResultante.tipo,
        },
      },
    ],
  });

  console.log(`📤 [EVENTO EMITIDO] Publicado '${eventoResultante.tipo}' en '${CONFIG.topicReprogramaciones}'`);
}

// ============================================================================
// ARRANQUE DEL CONSUMIDOR KAFKA
// ============================================================================
async function iniciarServicio() {
  console.log('===============================================================');
  console.log('🚀 INICIANDO MICROSERVICIO DE REPROGRAMACIÓN (RutaSur)');
  console.log('===============================================================');

  // 1. Conectar a MongoDB
  console.log(`📡 Conectando a MongoDB: ${CONFIG.mongoUri}...`);
  await mongoClient.connect();
  const db = mongoClient.db(CONFIG.dbName);
  const collection = db.collection(CONFIG.collectionName);
  console.log(`✅ MongoDB conectado. Colección de consulta: '${CONFIG.dbName}.${CONFIG.collectionName}'`);

  // 2. Conectar Productor y Consumidor de Kafka
  console.log(`📡 Conectando a Kafka Brokers: ${CONFIG.kafkaBrokers.join(', ')}...`);
  await producer.connect();
  await consumer.connect();
  console.log(`✅ Productor y Consumidor de Kafka conectados exitosamente.`);

  // 3. Suscripción al topic 'envios.estados'
  await consumer.subscribe({
    topic: CONFIG.topicEstados,
    fromBeginning: false,
  });
  console.log(`📬 Suscrito al topic '${CONFIG.topicEstados}' con GroupId: '${CONFIG.kafkaGroupId}'`);
  console.log(`🎯 Tópico destino de eventos resultantes: '${CONFIG.topicReprogramaciones}'\n`);

  // 4. Bucle de consumo con COMMIT MANUAL
  await consumer.run({
    autoCommit: false, // Confirmación manual obligatoria
    eachMessage: async ({ topic, partition, message }) => {
      const rawString = message.value ? message.value.toString() : '';

      let evento = null;
      try {
        evento = JSON.parse(rawString);
      } catch (err) {
        // Si el evento viene corrupto o malformado, este servicio lo ignora y confirma
        // (la derivación a DLQ es responsabilidad del Proyector en el flujo principal)
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
        return;
      }

      // REGLA 1: Filtrado de eventos
      // Si el tipo no es 'ENTREGA_FALLIDA', se ignora y se confirma el offset de inmediato
      if (evento.tipo !== 'ENTREGA_FALLIDA') {
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
        return;
      }

      try {
        // REGLAS 2 y 3: Evaluar intentos y publicar decisión (REPROGRAMADO o DEVOLUCION_ORIGEN)
        await procesarEntregaFallida(evento, collection);

        // REGLA 4: Confirmación manual del offset SOLO tras publicar exitosamente
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
      } catch (err) {
        console.error(
          `❌ [ERROR PROCESANDO REPROGRAMACIÓN] Fallo con envío ${evento.shipmentId}:`,
          err.message
        );
        // Al arrojar el error, KafkaJS no confirma el offset y aplicará reintento
        throw err;
      }
    },
  });
}

// ============================================================================
// CIERRE GRACEFUL
// ============================================================================
async function cerrarServicio() {
  console.log('\n🛑 Deteniendo servicio de Reprogramación...');
  try {
    await consumer.disconnect();
    await producer.disconnect();
    await mongoClient.close();
    console.log('👋 Conexiones cerradas limpiamente. Proceso terminado.');
    process.exit(0);
  } catch (err) {
    console.error('Error durante el apagado:', err);
    process.exit(1);
  }
}

process.on('SIGINT', cerrarServicio);
process.on('SIGTERM', cerrarServicio);

// Iniciar aplicación
iniciarServicio().catch((err) => {
  console.error('❌ Error fatal al iniciar Reprogramación:', err);
  process.exit(1);
});
