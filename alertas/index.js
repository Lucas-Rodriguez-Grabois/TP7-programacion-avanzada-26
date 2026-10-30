/**
 * =================================================================================================
 * PROYECTO: RutaSur - Plataforma de Logística Orientada a Eventos
 * ARCHIVO: alertas/index.js
 * RUTA: C:\Users\lrtr3\OneDrive\Desktop\facultad\programacion avanzada\TP7\alertas\index.js
 * SERVICIO: Monitor de SLA (Caso de Uso 3 - Detección de Riesgo de SLA y Emisión de Alertas)
 * =================================================================================================
 * RESPONSABILIDADES:
 * 1. Consumir eventos del topic 'envios.estados' (groupId: 'monitor-sla').
 * 2. Monitorear los paquetes que ingresan a un centro de distribución ('INGRESADO_HUB').
 * 3. Evaluar el paso del tiempo usando el reloj simulado (30 segundos reales = 1 hora simulada).
 * 4. Detectar anomalías de SLA: cuando un paquete permanece en 'INGRESADO_HUB' demasiado tiempo
 *    sin avanzar a 'CLASIFICADO' o 'EN_REPARTO' (Escenario "Demorado en Hub").
 * 5. Publicar eventos en el topic 'alertas.sla' con nivel 'RIESGO_SLA' y key = shipmentId.
 * 6. Evitar duplicación de alertas sobre el mismo envío (política 'sin repetición').
 * 7. Confirmación manual de offset (autoCommit: false) solo tras procesar el mensaje con éxito.
 * =================================================================================================
 */

const { Kafka, CompressionTypes } = require('kafkajs');
const crypto = require('crypto');

// ============================================================================
// CONFIGURACIÓN DE ENTORNO
// ============================================================================
const CONFIG = {
  kafkaBrokers: (process.env.KAFKA_BROKERS || '127.0.0.1:9092').split(','),
  kafkaGroupId: process.env.KAFKA_GROUP_ID || 'monitor-sla',
  topicEstados: 'envios.estados',
  topicAlertasSla: 'alertas.sla',

  // Factor de aceleración temporal del simulador:
  // 30 segundos reales = 3600 segundos simulados (1 hora).
  // Factor = 3600 / 30 = 120 (1 segundo real = 2 minutos simulados).
  timeAccelerationFactor: 3600 / 30,

  // Umbral de SLA para permanencia máxima en Hub sin clasificar:
  // Por defecto: 1.0 hora simulada (equivale a 30 segundos de tiempo real).
  // Puede configurarse mediante variable de entorno UMBRAL_HORAS_SIMULADAS.
  umbralHorasSimuladas: parseFloat(process.env.UMBRAL_HORAS_SIMULADAS || '1.0'),

  // Frecuencia del ciclo de monitoreo activo en milisegundos reales:
  // Evalúa el estado de los paquetes en Hub cada 1 segundo real (2 minutos simulados).
  intervaloEvaluacionMs: 1000,
};

// ============================================================================
// CLIENTES DE KAFKA
// ============================================================================
const kafka = new Kafka({
  clientId: 'rutasur-monitor-sla',
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

// ============================================================================
// TABLA DE ESTADO EN MEMORIA (In-Flight State Store)
// ============================================================================
/**
 * Mantiene los paquetes activos que están actualmente dentro de un Hub esperando clasificación.
 * Estructura del mapa:
 * shipmentId => {
 *   shipmentId: string,
 *   hubId: string,
 *   ingresadoAtReal: number (timestamp ms local),
 *   ocurridoEnSimulado: string (ISO date original del evento),
 *   alertaEmitida: boolean
 * }
 */
const paquetesEnHub = new Map();

// ============================================================================
// PUBLICACIÓN DE ALERTA DE SLA
// ============================================================================
/**
 * Emite un evento en 'alertas.sla' cuando se detecta un incumplimiento de SLA.
 */
async function emitirAlertaSla(paquete, horasDemora, minutosDemora) {
  const tiempoDemoraFormateado = `${horasDemora.toFixed(1)} horas simuladas (${Math.round(minutosDemora)} min)`;

  const eventoAlerta = {
    eventId: crypto.randomUUID(),
    shipmentId: paquete.shipmentId,
    nivelAlerta: 'RIESGO_SLA',
    hubId: paquete.hubId,
    tiempoDemora: tiempoDemoraFormateado,
    emitidoEn: new Date().toISOString(),
  };

  await producer.send({
    topic: CONFIG.topicAlertasSla,
    compression: CompressionTypes.None,
    messages: [
      {
        key: String(paquete.shipmentId), // Particionado por shipmentId
        value: JSON.stringify(eventoAlerta),
        headers: {
          'x-source': 'monitor-sla-service',
          'x-alert-level': eventoAlerta.nivelAlerta,
          'x-hub-id': String(paquete.hubId),
        },
      },
    ],
  });

  console.log('---------------------------------------------------------------');
  console.log(`🚨 [ALERTA DE SLA GENERADA] ${paquete.shipmentId}`);
  console.log(`   Nivel: RIESGO_SLA`);
  console.log(`   Centro Logístico (Hub): ${paquete.hubId}`);
  console.log(`   Tiempo Demorado: ${tiempoDemoraFormateado}`);
  console.log(`   Publicado en tópico: '${CONFIG.topicAlertasSla}'`);
  console.log('---------------------------------------------------------------');
}

// ============================================================================
// EVALUADOR PERIÓDICO DEL PASO DEL TIEMPO (Reloj Acelerado)
// ============================================================================
/**
 * Examina los paquetes en memoria. Si alguno superó el umbral de horas simuladas
 * sin haber pasado a CLASIFICADO, emite la alerta correspondiente.
 */
async function evaluarSlaActivo() {
  if (paquetesEnHub.size === 0) return;

  const ahoraReal = Date.now();

  for (const [shipmentId, paquete] of paquetesEnHub.entries()) {
    // Regla de diseño: Sin repetición (no emitir más de una alerta por envío)
    if (paquete.alertaEmitida) continue;

    // Calcular el tiempo transcurrido en el reloj simulado
    const deltaRealMs = ahoraReal - paquete.ingresadoAtReal;
    const deltaSimuladoMs = deltaRealMs * CONFIG.timeAccelerationFactor;
    const horasDemora = deltaSimuladoMs / (3600 * 1000);
    const minutosDemora = deltaSimuladoMs / (60 * 1000);

    if (horasDemora >= CONFIG.umbralHorasSimuladas) {
      try {
        await emitirAlertaSla(paquete, horasDemora, minutosDemora);
        paquete.alertaEmitida = true; // Marcar para evitar reemisión
      } catch (err) {
        console.error(`❌ Error al emitir alerta de SLA para ${shipmentId}:`, err.message);
      }
    }
  }
}

// ============================================================================
// CICLO PRINCIPAL DE CONSUMO KAFKA
// ============================================================================
async function iniciarServicio() {
  console.log('===============================================================');
  console.log('🚀 INICIANDO MONITOR DE SLA (RutaSur - Caso de Uso 3)');
  console.log('===============================================================');
  console.log(`Brokers Kafka: ${CONFIG.kafkaBrokers.join(', ')}`);
  console.log(`GroupId: '${CONFIG.kafkaGroupId}'`);
  console.log(`Tópico de entrada: '${CONFIG.topicEstados}'`);
  console.log(`Tópico de alertas: '${CONFIG.topicAlertasSla}'`);
  console.log(`Factor de aceleración: ${CONFIG.timeAccelerationFactor}x (30s real = 1h simulada)`);
  console.log(`Umbral SLA Hub: ${CONFIG.umbralHorasSimuladas} hora(s) simulada(s) [=${CONFIG.umbralHorasSimuladas * 30}s reales]`);
  console.log('===============================================================\n');

  // 1. Conectar Productor y Consumidor
  await producer.connect();
  await consumer.connect();
  console.log('✅ Productor y Consumidor de Kafka conectados exitosamente.');

  // 2. Suscribirse a envios.estados
  await consumer.subscribe({
    topic: CONFIG.topicEstados,
    fromBeginning: false,
  });
  console.log(`📬 Suscrito al topic '${CONFIG.topicEstados}'. Monitoreando eventos...\n`);

  // 3. Iniciar el evaluador periódico del reloj simulado
  const timerId = setInterval(async () => {
    await evaluarSlaActivo();
  }, CONFIG.intervaloEvaluacionMs);

  // 4. Bucle de procesamiento de mensajes con COMMIT MANUAL
  await consumer.run({
    autoCommit: false, // Confirmación manual obligatoria
    eachMessage: async ({ topic, partition, message }) => {
      const rawString = message.value ? message.value.toString() : '';

      let evento = null;
      try {
        evento = JSON.parse(rawString);
      } catch (err) {
        // En caso de mensaje malformado, confirmamos offset para no trabar el consumidor
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
        return;
      }

      const { shipmentId, tipo, hubId, ocurridoEn } = evento;

      // ----------------------------------------------------------------------
      // FILTRADO Y LÓGICA DE NEGOCIO SEGÚN ESTADO
      // ----------------------------------------------------------------------
      if (tipo === 'INGRESADO_HUB') {
        // Registramos el paquete en el centro de distribución para monitorear su tiempo
        paquetesEnHub.set(shipmentId, {
          shipmentId,
          hubId: hubId || 'HUB-DESCONOCIDO',
          ingresadoAtReal: Date.now(),
          ocurridoEnSimulado: ocurridoEn,
          alertaEmitida: false,
        });

        console.log(`📥 [HUB ENTRADA] ${shipmentId} ingresó a ${hubId}. Iniciando control de SLA.`);
      } else if (tipo === 'CLASIFICADO') {
        // El paquete avanzó en su proceso: se remueve del monitor de permanencia en Hub
        if (paquetesEnHub.has(shipmentId)) {
          paquetesEnHub.delete(shipmentId);
          console.log(`📦 [HUB SALIDA/CLASIFICADO] ${shipmentId} clasificado a tiempo. Removido de monitor Hub.`);
        }
      } else if (['EN_REPARTO', 'ENTREGADO', 'ENTREGA_FALLIDA'].includes(tipo)) {
        // En cualquier estado posterior, nos aseguramos de que no quede retenido en la tabla de estado
        if (paquetesEnHub.has(shipmentId)) {
          paquetesEnHub.delete(shipmentId);
        }
      }

      // ----------------------------------------------------------------------
      // CONFIRMACIÓN MANUAL DE OFFSET (At-Least-Once)
      // Se realiza una vez procesado el mensaje de estado y actualizado el estado interno
      // ----------------------------------------------------------------------
      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (BigInt(message.offset) + 1n).toString(),
        },
      ]);
    },
  });

  return timerId;
}

// ============================================================================
// CIERRE GRACEFUL
// ============================================================================
let timerReferencia = null;

async function cerrarServicio() {
  console.log('\n🛑 Deteniendo servicio Monitor de SLA...');
  if (timerReferencia) {
    clearInterval(timerReferencia);
  }
  try {
    await consumer.disconnect();
    await producer.disconnect();
    console.log('👋 Conexiones de Kafka cerradas limpiamente. Proceso terminado.');
    process.exit(0);
  } catch (err) {
    console.error('Error durante el apagado:', err);
    process.exit(1);
  }
}

process.on('SIGINT', cerrarServicio);
process.on('SIGTERM', cerrarServicio);

iniciarServicio()
  .then((timer) => {
    timerReferencia = timer;
  })
  .catch((err) => {
    console.error('❌ Error fatal al iniciar Monitor de SLA:', err);
    process.exit(1);
  });
