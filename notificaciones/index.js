/**
 * =================================================================================================
 * PROYECTO: RutaSur - Plataforma de Logística Orientada a Eventos
 * ARCHIVO: notificaciones/index.js
 * RUTA: C:\Users\lrtr3\OneDrive\Desktop\facultad\programacion avanzada\TP7\notificaciones\index.js
 * SERVICIO: Notificaciones al Cliente (Caso de Uso 4)
 * =================================================================================================
 * RESPONSABILIDADES:
 * 1. Consumir eventos desde dos tópicos: 'envios.estados' y 'envios.reprogramaciones'
 *    usando el groupId 'notificaciones'.
 * 2. Filtrar únicamente los eventos de interés para el destinatario:
 *    - Desde 'envios.estados': 'ENTREGADO'.
 *    - Desde 'envios.reprogramaciones': 'REPROGRAMADO' y 'DEVOLUCION_ORIGEN'.
 *    - Todo otro tipo de evento se ignora y se confirma de inmediato el offset.
 * 3. Construir la notificación amigable simulando el envío de correo (Canal: 'EMAIL'):
 *    { idNotificacion (UUID), shipmentId, tipoEvento, mensaje, canal: 'EMAIL', fechaEnvio }.
 * 4. Publicar la notificación en el topic 'notificaciones.enviadas' particionada por shipmentId.
 * 5. Confirmación manual de offset (autoCommit: false) SOLO después de haber publicado la notificación.
 * =================================================================================================
 */

const { Kafka, CompressionTypes } = require('kafkajs');
const crypto = require('crypto');

// ============================================================================
// CONFIGURACIÓN DE ENTORNO (Brokers en 127.0.0.1:9092)
// ============================================================================
const CONFIG = {
  kafkaBrokers: (process.env.KAFKA_BROKERS || '127.0.0.1:9092').split(','),
  kafkaGroupId: process.env.KAFKA_GROUP_ID || 'notificaciones',
  topicEstados: 'envios.estados',
  topicReprogramaciones: 'envios.reprogramaciones',
  topicNotificacionesEnviadas: 'notificaciones.enviadas',
};

// ============================================================================
// CLIENTES DE KAFKA
// ============================================================================
const kafka = new Kafka({
  clientId: 'rutasur-notificaciones',
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
// CONSTRUCTOR DE MENSAJES AMIGABLES AL CLIENTE
// ============================================================================
/**
 * Redacta un texto claro y cercano para el cliente de acuerdo al estado final o reprogramación.
 */
function redactarMensajeNotificacion(tipoEvento, evento) {
  const { shipmentId } = evento;

  switch (tipoEvento) {
    case 'ENTREGADO': {
      const repartidor = evento.repartidorId ? ` por el repartidor ${evento.repartidorId}` : '';
      return `¡Buenas noticias! Tu paquete con código de seguimiento ${shipmentId} ha sido entregado exitosamente${repartidor}. ¡Muchas gracias por elegir RutaSur!`;
    }

    case 'REPROGRAMADO': {
      const proximo = evento.proximoIntento || 2;
      const motivo = evento.motivoFallo || 'Destinatario ausente';
      const fecha = evento.nuevaFecha ? new Date(evento.nuevaFecha).toLocaleString('es-AR') : 'próximamente';
      return `Hola. No pudimos concretar la entrega de tu paquete ${shipmentId} (Motivo: ${motivo}). Hemos coordinado un nuevo intento para el ${fecha} (Intento ${proximo} de 3).`;
    }

    case 'DEVOLUCION_ORIGEN': {
      const motivo = evento.motivoFallo || 'Límite de intentos agotado';
      return `Atención: Tu paquete ${shipmentId} no pudo ser entregado tras múltiples visitas (${motivo}). El envío ha sido cancelado y regresa al depósito de origen.`;
    }

    default:
      return `Actualización de estado sobre tu envío ${shipmentId}: ${tipoEvento}.`;
  }
}

// ============================================================================
// PROCESAMIENTO Y EMISIÓN DE NOTIFICACIÓN
// ============================================================================
async function procesarYNotificar(tipoEvento, evento) {
  const { shipmentId } = evento;
  const mensajeTexto = redactarMensajeNotificacion(tipoEvento, evento);

  const payloadNotificacion = {
    idNotificacion: crypto.randomUUID(),
    shipmentId: String(shipmentId),
    tipoEvento,
    mensaje: mensajeTexto,
    canal: 'EMAIL',
    fechaEnvio: new Date().toISOString(),
  };

  // Publicación en 'notificaciones.enviadas' con shipmentId como clave de partición
  await producer.send({
    topic: CONFIG.topicNotificacionesEnviadas,
    compression: CompressionTypes.None,
    messages: [
      {
        key: String(shipmentId),
        value: JSON.stringify(payloadNotificacion),
        headers: {
          'x-source': 'notificaciones-service',
          'x-event-type': tipoEvento,
          'x-channel': 'EMAIL',
        },
      },
    ],
  });

  console.log('---------------------------------------------------------------');
  console.log(`✉️ [NOTIFICACIÓN ENVIADA - EMAIL] Para: ${shipmentId}`);
  console.log(`   Evento origen: ${tipoEvento}`);
  console.log(`   Mensaje: "${mensajeTexto}"`);
  console.log(`   Publicado en tópico: '${CONFIG.topicNotificacionesEnviadas}'`);
  console.log('---------------------------------------------------------------');
}

// ============================================================================
// ARRANQUE DEL CONSUMIDOR KAFKA
// ============================================================================
async function iniciarServicio() {
  console.log('===============================================================');
  console.log('🚀 INICIANDO MICROSERVICIO DE NOTIFICACIONES (RutaSur)');
  console.log('===============================================================');
  console.log(`Brokers Kafka: ${CONFIG.kafkaBrokers.join(', ')}`);
  console.log(`GroupId: '${CONFIG.kafkaGroupId}'`);
  console.log(`Tópicos suscritos: '${CONFIG.topicEstados}' y '${CONFIG.topicReprogramaciones}'`);
  console.log(`Tópico destino: '${CONFIG.topicNotificacionesEnviadas}'`);
  console.log('===============================================================\n');

  // 1. Conectar Productor y Consumidor
  await producer.connect();
  await consumer.connect();
  console.log('✅ Productor y Consumidor de Kafka conectados exitosamente.');

  // 2. Suscripción multi-tópico mediante expresión regular
  // Captura exactamente 'envios.estados' y 'envios.reprogramaciones'
  await consumer.subscribe({
    topic: /^(envios\.estados|envios\.reprogramaciones)$/,
    fromBeginning: false,
  });
  console.log(`📬 Suscrito exitosamente a los tópicos con GroupId '${CONFIG.kafkaGroupId}'\n`);

  // 3. Bucle de consumo con COMMIT MANUAL
  await consumer.run({
    autoCommit: false, // Confirmación manual obligatoria
    eachMessage: async ({ topic, partition, message }) => {
      const rawString = message.value ? message.value.toString() : '';

      let evento = null;
      try {
        evento = JSON.parse(rawString);
      } catch (err) {
        // En caso de mensaje malformado en Kafka, confirmamos offset para no detener el pipeline
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
        return;
      }

      const tipoEvento = evento.tipo;
      let debeNotificar = false;

      // ----------------------------------------------------------------------
      // REGLA 1: Filtrado específico según el tópico de procedencia
      // ----------------------------------------------------------------------
      if (topic === CONFIG.topicEstados && tipoEvento === 'ENTREGADO') {
        debeNotificar = true;
      } else if (
        topic === CONFIG.topicReprogramaciones &&
        (tipoEvento === 'REPROGRAMADO' || tipoEvento === 'DEVOLUCION_ORIGEN')
      ) {
        debeNotificar = true;
      }

      // Si no es ninguno de los 3 eventos relevantes, se ignora y confirma offset de inmediato
      if (!debeNotificar) {
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
        // ----------------------------------------------------------------------
        // REGLAS 2 y 3: Construir notificación y publicar en 'notificaciones.enviadas'
        // ----------------------------------------------------------------------
        await procesarYNotificar(tipoEvento, evento);

        // ----------------------------------------------------------------------
        // REGLA 4: Confirmación manual de offset SOLO tras publicar con éxito
        // ----------------------------------------------------------------------
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
      } catch (err) {
        console.error(
          `❌ [ERROR NOTIFICACIONES] Fallo al procesar notificación para ${evento.shipmentId}:`,
          err.message
        );
        // Si hay error, KafkaJS no comitea el offset para aplicar reintento
        throw err;
      }
    },
  });
}

// ============================================================================
// CIERRE GRACEFUL
// ============================================================================
async function cerrarServicio() {
  console.log('\n🛑 Deteniendo servicio de Notificaciones...');
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

iniciarServicio().catch((err) => {
  console.error('❌ Error fatal al iniciar Notificaciones:', err);
  process.exit(1);
});
