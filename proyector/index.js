/**
 * =================================================================================================
 * PROYECTO: RutaSur - Plataforma de Logística Orientada a Eventos
 * ARCHIVO: proyector/index.js
 * RUTA: C:\Users\lrtr3\OneDrive\Desktop\facultad\programacion avanzada\TP7\proyector\index.js
 * SERVICIO: Proyector (CQRS Read-Model de Envíos en MongoDB)
 * =================================================================================================
 * RESPONSABILIDADES:
 * 1. Consumir eventos del topic 'envios.estados' (groupId: 'proyector').
 * 2. Mantener la proyección del estado consolidado de cada envío en MongoDB ('rutasur.envios').
 * 3. Aplicar regla de validación de esquema: derivar eventos malformados a 'envios.estados.dlq'.
 * 4. Aplicar regla de idempotencia: descartar eventos con eventId ya registrado en el historial.
 * 5. Aplicar regla de orden: descartar eventos con secuencia <= última secuencia aplicada.
 * 6. Confirmación de offset manual (autoCommit: false) SOLO tras escribir con éxito en MongoDB.
 * =================================================================================================
 */

const { Kafka, CompressionTypes } = require('kafkajs');
const { MongoClient } = require('mongodb');

// ============================================================================
// CONFIGURACIÓN DE ENTORNO
// ============================================================================
const CONFIG = {
  kafkaBrokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  kafkaGroupId: process.env.KAFKA_GROUP_ID || 'proyector',
  topicEstados: 'envios.estados',
  topicDlq: 'envios.estados.dlq',

// Conexión a MongoDB Replica Set local (rs0)
  mongoUri: process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/rutasur?replicaSet=rs0&directConnection=true',
  dbName: 'rutasur',
  collectionName: 'envios',

  tiposValidos: [
    'CREADO',
    'INGRESADO_HUB',
    'CLASIFICADO',
    'EN_REPARTO',
    'ENTREGADO',
    'ENTREGA_FALLIDA',
  ],
};

// ============================================================================
// CLIENTES KAFKA Y MONGODB
// ============================================================================
const kafka = new Kafka({
  clientId: 'rutasur-proyector',
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

// Productor dedicado para enviar mensajes corruptos a la DLQ
const producerDlq = kafka.producer();

const mongoClient = new MongoClient(CONFIG.mongoUri);

// ============================================================================
// VALIDACIÓN DE ESQUEMA (DETECCIÓN DE EVENTOS MALFORMADOS)
// ============================================================================
/**
 * Valida el esquema canónico del evento.
 * Devuelve null si es válido o un string con el motivo del fallo.
 */
function validarEsquema(evento) {
  if (!evento || typeof evento !== 'object') {
    return 'El payload del evento no es un objeto JSON válido';
  }
  if (!evento.eventId || typeof evento.eventId !== 'string') {
    return 'Campo obligatorio ausente o inválido: eventId';
  }
  if (!evento.shipmentId || typeof evento.shipmentId !== 'string') {
    return 'Campo obligatorio ausente o inválido: shipmentId';
  }
  if (!evento.tipo || !CONFIG.tiposValidos.includes(evento.tipo)) {
    return `Campo 'tipo' inválido o ausente: '${evento.tipo}'. Tipos válidos: ${CONFIG.tiposValidos.join(', ')}`;
  }
  if (typeof evento.secuencia !== 'number' || evento.secuencia < 1) {
    return `Campo 'secuencia' inválido o no numérico: '${evento.secuencia}'`;
  }
  if (!evento.ocurridoEn) {
    return 'Campo obligatorio ausente: ocurridoEn (timestamp de evento)';
  }
  return null;
}

/**
 * Deriva un mensaje inválido a la Dead Letter Queue (DLQ).
 */
async function enviarADlq(rawMessage, motivoError, shipmentId = 'DESCONOCIDO') {
  try {
    console.warn(`🚨 [DLQ] Desviando mensaje a '${CONFIG.topicDlq}' | Motivo: ${motivoError}`);
    await producerDlq.send({
      topic: CONFIG.topicDlq,
      compression: CompressionTypes.None,
      messages: [
        {
          key: String(shipmentId),
          value: typeof rawMessage === 'string' ? rawMessage : JSON.stringify(rawMessage),
          headers: {
            'x-error-reason': motivoError,
            'x-failed-at': new Date().toISOString(),
            'x-failed-in': 'proyector-service',
            'x-original-topic': CONFIG.topicEstados,
          },
        },
      ],
    });
  } catch (err) {
    console.error('❌ Error crítico enviando mensaje a la DLQ:', err.message);
  }
}

// ============================================================================
// LÓGICA DE NEGOCIO Y PROYECCIÓN CQRS
// ============================================================================
/**
 * Procesa un evento válido contra la colección 'envios' de MongoDB.
 */
async function procesarEvento(evento, collection) {
  const { shipmentId, eventId, secuencia, tipo, ocurridoEn } = evento;

  // 1. Buscar si el documento del envío ya existe en la base de datos
  const docExistente = await collection.findOne({ shipmentId });

  if (docExistente) {
    // 2. Regla de Duplicados (Idempotencia)
    // Si el eventId ya existe en el historial embebido, se descarta
    const yaExiste = docExistente.historial && docExistente.historial.some((h) => h.eventId === eventId);
    if (yaExiste) {
      console.warn(
        `⚠️ [IDEMPOTENCIA] Evento duplicado descartado: eventId=${eventId} ya fue aplicado a ${shipmentId}.`
      );
      return { aplicado: false, razon: 'DUPLICADO' };
    }

    // 3. Regla de Orden (Secuencia creciente)
    // Si la secuencia recibida es menor o igual a la última secuencia guardada, se descarta
    if (secuencia <= docExistente.ultimaSecuencia) {
      console.warn(
        `⚠️ [ORDEN] Evento fuera de orden descartado para ${shipmentId}: Secuencia recibida (${secuencia}) <= Última secuencia aplicada (${docExistente.ultimaSecuencia}).`
      );
      return { aplicado: false, razon: 'FUERA_DE_ORDEN' };
    }
  }

  // 4. Preparación de actualización atómica en MongoDB
  const itemHistorial = {
    eventId,
    tipo,
    secuencia,
    ocurridoEn,
    hubId: evento.hubId || null,
    zonaId: evento.zonaId || null,
    repartidorId: evento.repartidorId || null,
    motivo: evento.motivo || null,
    procesadoEn: new Date(),
  };

const updateOps = {
    $set: {
      shipmentId,
      estado: tipo,
      ultimaSecuencia: secuencia,
      hubId: evento.hubId || (docExistente ? docExistente.hubId : null),
      zonaId: evento.zonaId || (docExistente ? docExistente.zonaId : null),
      repartidorId: evento.repartidorId || (docExistente ? docExistente.repartidorId : null),
      version: evento.version || 1,
      actualizadoEn: new Date(),
    },
    $push: {
      historial: itemHistorial,
    },
    $setOnInsert: {
      creadoEn: new Date(),
      // ELIMINAMOS intentos: 0 de acá para evitar el conflicto con $inc
    },
  };

  // Ajustes condicionales de estado de negocio
  if (tipo === 'ENTREGA_FALLIDA') {
    updateOps.$set.motivoFallo = evento.motivo || 'NO_ESPECIFICADO';
    updateOps.$inc = { intentos: 1 };
  } else {
    // Solo inicializamos intentos en 0 si no es un fallo
    updateOps.$setOnInsert.intentos = 0;
  }

  if (tipo === 'ENTREGADO') {
    updateOps.$set.entregadoEn = ocurridoEn;
    updateOps.$set.motivoFallo = null;
  }

  // 5. Escritura atómica (Upsert por shipmentId)
  await collection.updateOne({ shipmentId }, updateOps, { upsert: true });

  console.log(
    `✅ [PROYECCIÓN OK] ${shipmentId} -> Estado: ${tipo} | Sec: ${secuencia} | EventId: ${eventId}`
  );

  return { aplicado: true };
}

// ============================================================================
// ARRANQUE DEL CONSUMIDOR KAFKA
// ============================================================================
async function iniciarProyector() {
  console.log('===============================================================');
  console.log('🚀 INICIANDO MICROSERVICIO PROYECTOR (RutaSur)');
  console.log('===============================================================');

  // 1. Conexión a MongoDB
  console.log(`📡 Conectando a MongoDB: ${CONFIG.mongoUri}...`);
  await mongoClient.connect();
  const db = mongoClient.db(CONFIG.dbName);
  const collection = db.collection(CONFIG.collectionName);
  console.log(`✅ MongoDB conectado. Colección activa: '${CONFIG.dbName}.${CONFIG.collectionName}'`);

  // Creación de índices según especificación de arquitectura (Página 4 del PDF)
  await collection.createIndex({ shipmentId: 1 }, { unique: true });
  await collection.createIndex({ estado: 1, zonaId: 1 });
  await collection.createIndex({ 'promesa.venceEn': 1 });
  console.log('📑 Índices en MongoDB verificados/creados correctamente.');

  // 2. Conexión a Kafka
  console.log(`📡 Conectando a Kafka Brokers: ${CONFIG.kafkaBrokers.join(', ')}...`);
  await producerDlq.connect();
  await consumer.connect();
  console.log(`✅ Productor DLQ y Consumidor conectados exitosamente.`);

  // Suscripción al topic 'envios.estados'
  await consumer.subscribe({
    topic: CONFIG.topicEstados,
    fromBeginning: false,
  });
  console.log(`📬 Suscrito al topic '${CONFIG.topicEstados}' con GroupId: '${CONFIG.kafkaGroupId}'`);

  // 3. Ejecución del ciclo de consumo con COMMIT MANUAL
  await consumer.run({
    autoCommit: false, // Confirmación manual obligatoria
    eachMessage: async ({ topic, partition, message }) => {
      const rawString = message.value ? message.value.toString() : '';
      const messageKey = message.key ? message.key.toString() : 'UNKNOWN';

      let evento = null;
      let errorEsquema = null;

      // Intentar parsear el JSON
      try {
        evento = JSON.parse(rawString);
        errorEsquema = validarEsquema(evento);
      } catch (parseError) {
        errorEsquema = `Error de sintaxis JSON: ${parseError.message}`;
      }

      // MANEJO DE EVENTO INVÁLIDO (DLQ)
      if (errorEsquema) {
        await enviarADlq(rawString, errorEsquema, evento ? evento.shipmentId : messageKey);
        // Hacemos commit del offset para que el mensaje roto no detenga la partición
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
        return;
      }

      // MANEJO DE EVENTO VÁLIDO
      try {
        // Proyectar el evento en MongoDB aplicando reglas de orden e idempotencia
        await procesarEvento(evento, collection);

        // CONFIRMACIÓN MANUAL: Solo se hace commit tras persistir exitosamente en MongoDB
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
      } catch (dbError) {
        console.error(
          `❌ [ERROR PERSISTENCIA] Fallo al escribir en MongoDB para envío ${evento.shipmentId}:`,
          dbError.message
        );
        // NO se comitea el offset: KafkaJS pausará o reintentará el mensaje ante fallas de infraestructura
        throw dbError;
      }
    },
  });
}

// ============================================================================
// CIERRE GRACEFUL
// ============================================================================
async function cerrarServicio() {
  console.log('\n🛑 Deteniendo Proyector de manera segura...');
  try {
    await consumer.disconnect();
    await producerDlq.disconnect();
    await mongoClient.close();
    console.log('👋 Conexiones cerradas limpiamente. Proceso terminado.');
    process.exit(0);
  } catch (e) {
    console.error('Error durante el apagado:', e);
    process.exit(1);
  }
}

process.on('SIGINT', cerrarServicio);
process.on('SIGTERM', cerrarServicio);

// Iniciar aplicación
iniciarProyector().catch((err) => {
  console.error('❌ Error fatal al iniciar el Proyector:', err);
  process.exit(1);
});
