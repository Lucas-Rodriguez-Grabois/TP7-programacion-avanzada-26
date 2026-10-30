/**
 * =================================================================================================
 * PROYECTO: RutaSur - Plataforma de Logística Orientada a Eventos
 * ARCHIVO: simulador.js
 * RUTA: C:\Users\lrtr3\OneDrive\Desktop\facultad\programacion avanzada\TP7\simulador.js
 * ROL: Productor Kafka que reemplaza a los sistemas de origen (Depósito y App del Repartidor).
 * =================================================================================================
 * REGLAS IMPLEMENTADAS:
 * 1. Tópico destino principal: envios.estados.
 * 2. Clave de partición: shipmentId en todos los eventos (garantía de orden estricto por envío).
 * 3. Payload canónico: { eventId, shipmentId, tipo, ocurridoEn, secuencia, hubId, zonaId, repartidorId, version }.
 * 4. Reloj Acelerado: 30 segundos reales equivalen a 1 hora de simulación (Factor: 120x).
 * 5. Flujo de estados: CREADO -> INGRESADO_HUB -> CLASIFICADO -> EN_REPARTO -> ENTREGADO / ENTREGA_FALLIDA.
 * 6. Motivos de fallo: AUSENTE (60%), DIRECCION_INCORRECTA (20%), RECHAZADO (15%), ZONA_INACCESIBLE (5%).
 * 7. Generador continuo: 20 envíos/minuto, tasa de fallo por zona (5%-30%), 10% demora en hub.
 * 8. Anomalías inyectadas: 2% duplicados, 2% fuera de orden, 1% malformados (para prueba de DLQ).
 * 9. Semilla determinista fija: PRNG Mulberry32 para corridas 100% reproducibles.
 * 10. Métodos bajo demanda (Escenarios 1 al 5): activables por CLI (--escenario X) o menú interactivo.
 * =================================================================================================
 */

const { Kafka, CompressionTypes } = require('kafkajs');
const crypto = require('crypto');
const readline = require('readline');

// ============================================================================
// CONFIGURACIÓN Y CONSTANTES DEL DOMINIO
// ============================================================================
const CONFIG = {
  kafkaBrokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  topicEstados: 'envios.estados',
  topicPromesas: 'envios.promesas',
  topicDlq: 'envios.estados.dlq',

  // Reloj acelerado: 30 segundos reales = 3600 segundos simulados (1 hora).
  // Factor = 3600 / 30 = 120. (1 segundo real = 2 minutos simulados).
  timeAccelerationFactor: 3600 / 30,

  // Volumen continuo: 20 envíos por minuto = 1 envío cada 3000 ms reales.
  intervalMsContinuo: 3000,

  // Semilla fija para reproducibilidad exacta
  seed: 20260429,

  // Probabilidades de inyección de anomalías
  probDuplicado: 0.02,     // 2%
  probFueraDeOrden: 0.02,  // 2%
  probMalformado: 0.01,    // 1% (para probar la DLQ)
  probDemoraHub: 0.10,     // 10% (demora en centro de distribución)

  // Catálogos dimensionales para analítica
  hubs: ['HUB-CENTRAL', 'HUB-NORTE', 'HUB-SUR'],
  zonas: [
    { id: 'ZONA-NORTE', tasaFallo: 0.08 }, // 8%
    { id: 'ZONA-CENTRO', tasaFallo: 0.15 }, // 15%
    { id: 'ZONA-OESTE', tasaFallo: 0.22 },  // 22%
    { id: 'ZONA-SUR', tasaFallo: 0.30 },    // 30%
  ],
  totalRepartidores: 120,
};

// ============================================================================
// GENERADOR DE NÚMEROS PSEUDOALEATORIOS CON SEMILLA FIJA (Mulberry32)
// ============================================================================
let currentSeed = CONFIG.seed;

function random() {
  currentSeed |= 0;
  currentSeed = (currentSeed + 0x6d2b79f5) | 0;
  let t = Math.imul(currentSeed ^ (currentSeed >>> 15), 1 | currentSeed);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}

function randomItem(array) {
  return array[Math.floor(random() * array.length)];
}

// ============================================================================
// RELOJ ACELERADO (30s reales = 1h simulada)
// ============================================================================
const realStartTime = Date.now();
const simulatedStartTime = new Date('2026-10-30T08:00:00.000Z').getTime();

/**
 * Retorna la fecha simulada en formato ISO 8601.
 */
function getSimulatedDate(offsetRealMs = 0) {
  const elapsedRealMs = (Date.now() + offsetRealMs) - realStartTime;
  const elapsedSimulatedMs = elapsedRealMs * CONFIG.timeAccelerationFactor;
  return new Date(simulatedStartTime + elapsedSimulatedMs).toISOString();
}

// ============================================================================
// CLIENTE Y PRODUCTOR KAFKA
// ============================================================================
const kafka = new Kafka({
  clientId: 'rutasur-simulador',
  brokers: CONFIG.kafkaBrokers,
  retry: { retries: 10, initialRetryTime: 500 },
});

const producer = kafka.producer({
  allowAutoTopicCreation: true,
  idempotent: false,
});

/**
 * Publica un evento en Kafka garantizando que la Partition Key sea estrictamente shipmentId.
 */
async function emitirEvento(topic, shipmentIdKey, payload, headers = {}) {
  const value = typeof payload === 'string' ? payload : JSON.stringify(payload);
  await producer.send({
    topic,
    compression: CompressionTypes.None,
    messages: [
      {
        key: String(shipmentIdKey),
        value,
        headers: {
          'x-simulation': 'true',
          'x-source': 'simulador-rutasur',
          ...headers,
        },
      },
    ],
  });
}

// ============================================================================
// GENERACIÓN Y CONSTRUCCIÓN DE EVENTOS
// ============================================================================
let shipmentSequenceId = 1000;

function crearContextoEnvio(overrideZona = null) {
  shipmentSequenceId++;
  const shipmentId = `ENV-${shipmentSequenceId}`;
  const zonaObj = overrideZona || randomItem(CONFIG.zonas);
  const hubId = randomItem(CONFIG.hubs);
  const repartidorNum = Math.floor(random() * CONFIG.totalRepartidores) + 1;
  const repartidorId = `REP-${String(repartidorNum).padStart(3, '0')}`;

  return {
    shipmentId,
    hubId,
    zonaId: zonaObj.id,
    tasaFallo: zonaObj.tasaFallo,
    repartidorId,
    version: 1,
    secuencia: 0,
  };
}

/**
 * Payload Base canónico especificado en la página 4 del caso práctico.
 */
function crearPayloadBase(ctx, tipo, timestampSimulado) {
  ctx.secuencia += 1;
  return {
    eventId: crypto.randomUUID(),
    shipmentId: ctx.shipmentId,
    tipo,
    ocurridoEn: timestampSimulado || getSimulatedDate(),
    secuencia: ctx.secuencia,
    hubId: ctx.hubId,
    zonaId: ctx.zonaId,
    repartidorId: ctx.repartidorId,
    version: ctx.version,
  };
}

function determinarMotivoFallo() {
  const roll = random();
  if (roll < 0.60) return 'AUSENTE';              // 60%
  if (roll < 0.80) return 'DIRECCION_INCORRECTA'; // 20%
  if (roll < 0.95) return 'RECHAZADO';            // 15%
  return 'ZONA_INACCESIBLE';                      // 5%
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ============================================================================
// CICLO DE VIDA CONTINUO DE UN ENVÍO
// ============================================================================
async function procesarCicloDeVida(ctx) {
  try {
    // 1. CREADO
    const eventoCreado = crearPayloadBase(ctx, 'CREADO');
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoCreado);

    // Registro en tópico compactado envios.promesas (SLA: 24 horas simuladas)
    const promesaVenceEn = new Date(new Date(eventoCreado.ocurridoEn).getTime() + 24 * 3600 * 1000).toISOString();
    await emitirEvento(CONFIG.topicPromesas, ctx.shipmentId, {
      shipmentId: ctx.shipmentId,
      promesaEntrega: promesaVenceEn,
      ventanaInicio: '09:00',
      ventanaFin: '18:00',
      version: 1,
    });

    console.log(`[${eventoCreado.ocurridoEn}] 📦 CREADO: ${ctx.shipmentId} | Zona: ${ctx.zonaId} (Sec: ${eventoCreado.secuencia})`);

    // Inyección intencional de evento malformado (1% probabilidad -> DLQ)
    if (random() < CONFIG.probMalformado) {
      console.warn(`⚠️ [ANOMALÍA DLQ] Emitiendo evento con JSON malformado para ${ctx.shipmentId}`);
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, "{ error_sintactico_sin_comillas: true, payload_invalido ");
    }

    // 2. INGRESADO_HUB
    await sleep(1500);
    const eventoIngresado = crearPayloadBase(ctx, 'INGRESADO_HUB');
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoIngresado);

    // Inyección intencional de evento duplicado (2% probabilidad -> Idempotencia)
    if (random() < CONFIG.probDuplicado) {
      console.warn(`⚠️ [ANOMALÍA DUPLICADO] Reemitiendo evento idéntico de ${ctx.shipmentId} (eventId: ${eventoIngresado.eventId})`);
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoIngresado);
    }

    // 10% probabilidad de demora en Hub (para generar riesgo de SLA)
    const demoraEnHub = random() < CONFIG.probDemoraHub;
    if (demoraEnHub) {
      console.warn(`⏳ [DEMORA EN HUB] Paquete ${ctx.shipmentId} retenido en ${ctx.hubId}`);
      await sleep(5000);
    } else {
      await sleep(1500);
    }

    // 3. CLASIFICADO
    const eventoClasificado = crearPayloadBase(ctx, 'CLASIFICADO');

    // Inyección intencional fuera de orden (2% probabilidad -> Regla de secuencia)
    const fueraDeOrden = random() < CONFIG.probFueraDeOrden;
    if (fueraDeOrden) {
      console.warn(`⚠️ [ANOMALÍA SECUENCIA] Emitiendo EN_REPARTO antes que CLASIFICADO en ${ctx.shipmentId}`);
      const eventoRepartoPrematuro = crearPayloadBase(ctx, 'EN_REPARTO');
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoRepartoPrematuro);
      await sleep(400);
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoClasificado); // Llega retrasado
    } else {
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoClasificado);
      await sleep(1500);

      // 4. EN_REPARTO
      const eventoEnReparto = crearPayloadBase(ctx, 'EN_REPARTO');
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoEnReparto);
    }

    await sleep(2000);

    // 5. RESULTADO: ENTREGADO o ENTREGA_FALLIDA (Evaluado según tasa de fallo de la zona)
    const esFallo = random() < ctx.tasaFallo;

    if (!esFallo) {
      const eventoEntregado = crearPayloadBase(ctx, 'ENTREGADO');
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoEntregado);
      console.log(`[${eventoEntregado.ocurridoEn}] ✅ ENTREGADO: ${ctx.shipmentId} por ${ctx.repartidorId}`);
    } else {
      const motivo = determinarMotivoFallo();
      const eventoFallido = {
        ...crearPayloadBase(ctx, 'ENTREGA_FALLIDA'),
        motivo,
      };
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoFallido);
      console.error(`[${eventoFallido.ocurridoEn}] ❌ ENTREGA_FALLIDA: ${ctx.shipmentId} | Motivo: ${motivo}`);
    }
  } catch (err) {
    console.error(`Error durante el ciclo de vida de ${ctx.shipmentId}:`, err.message);
  }
}

// ============================================================================
// ESCENARIOS BAJO DEMANDA (DEMO GUIÓN OFICIAL)
// ============================================================================
const Escenarios = {
  // Escenario 1: Envío feliz (Camino feliz completo sin incidentes)
  async envioFeliz() {
    console.log('\n======================================================');
    console.log('🚀 [ESCENARIO 1] Envío Feliz (Ciclo completo sin fallas)');
    console.log('======================================================');
    const ctx = crearContextoEnvio();

    const ev1 = crearPayloadBase(ctx, 'CREADO');
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, ev1);
    console.log(`1. CREADO -> ${ctx.shipmentId}`);
    await sleep(1200);

    const ev2 = crearPayloadBase(ctx, 'INGRESADO_HUB');
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, ev2);
    console.log(`2. INGRESADO_HUB -> ${ctx.shipmentId} (${ctx.hubId})`);
    await sleep(1200);

    const ev3 = crearPayloadBase(ctx, 'CLASIFICADO');
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, ev3);
    console.log(`3. CLASIFICADO -> ${ctx.shipmentId} (${ctx.zonaId})`);
    await sleep(1200);

    const ev4 = crearPayloadBase(ctx, 'EN_REPARTO');
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, ev4);
    console.log(`4. EN_REPARTO -> ${ctx.shipmentId} (${ctx.repartidorId})`);
    await sleep(1500);

    const ev5 = crearPayloadBase(ctx, 'ENTREGADO');
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, ev5);
    console.log(`5. ✅ ENTREGADO exitosamente -> ${ctx.shipmentId}\n`);
  },

  // Escenario 2: Ausente (Falla y se reprograma)
  async ausente() {
    console.log('\n======================================================');
    console.log('🚀 [ESCENARIO 2] Entrega Fallida por AUSENTE (Reprogramación)');
    console.log('======================================================');
    const ctx = crearContextoEnvio();

    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, crearPayloadBase(ctx, 'CREADO'));
    await sleep(1000);
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, crearPayloadBase(ctx, 'INGRESADO_HUB'));
    await sleep(1000);
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, crearPayloadBase(ctx, 'CLASIFICADO'));
    await sleep(1000);
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, crearPayloadBase(ctx, 'EN_REPARTO'));
    await sleep(1200);

    const eventoFallo = {
      ...crearPayloadBase(ctx, 'ENTREGA_FALLIDA'),
      motivo: 'AUSENTE',
    };
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoFallo);
    console.log(`❌ ENTREGA_FALLIDA emitida para ${ctx.shipmentId} con motivo: AUSENTE.`);
    console.log(`   (Dispara en paralelo: notificación, reprogramación e incremento en analítica)\n`);
  },

  // Escenario 3: Demorado (Queda en el hub hasta generar alerta de SLA)
  async demorado() {
    console.log('\n======================================================');
    console.log('🚀 [ESCENARIO 3] Envío Demorado en Hub (Riesgo de SLA)');
    console.log('======================================================');
    const ctx = crearContextoEnvio();

    const evCreado = crearPayloadBase(ctx, 'CREADO');
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, evCreado);

    // Promesa configurada para vencer en 3 horas simuladas (< 4 horas sin salir a reparto -> Alerta ALTA)
    const vencimientoSimulado = new Date(new Date(evCreado.ocurridoEn).getTime() + 3 * 3600 * 1000).toISOString();
    await emitirEvento(CONFIG.topicPromesas, ctx.shipmentId, {
      shipmentId: ctx.shipmentId,
      promesaEntrega: vencimientoSimulado,
      version: 1,
    });

    await sleep(1000);
    const evHub = crearPayloadBase(ctx, 'INGRESADO_HUB');
    await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, evHub);

    console.log(`⚠️ Envío ${ctx.shipmentId} ingresó a ${ctx.hubId}.`);
    console.log(`   Vencimiento de promesa fijado: ${vencimientoSimulado}`);
    console.log(`   El paquete permanece sin salir a reparto para evaluar la regla 'Sin salida' de SLA.\n`);
  },

  // Escenario 4: Tercer fallo (Agota sus 3 intentos y pasa a devolución al comercio)
  async tercerFallo() {
    console.log('\n======================================================');
    console.log('🚀 [ESCENARIO 4] Tres Fallos Consecutivos (Devolución)');
    console.log('======================================================');
    const ctx = crearContextoEnvio();

    for (let intento = 1; intento <= 3; intento++) {
      console.log(`--- Intento de Entrega #${intento} para ${ctx.shipmentId} ---`);
      if (intento === 1) {
        await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, crearPayloadBase(ctx, 'CREADO'));
        await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, crearPayloadBase(ctx, 'INGRESADO_HUB'));
      }
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, crearPayloadBase(ctx, 'CLASIFICADO'));
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, crearPayloadBase(ctx, 'EN_REPARTO'));
      await sleep(1000);

      const eventoFallo = {
        ...crearPayloadBase(ctx, 'ENTREGA_FALLIDA'),
        motivo: 'AUSENTE',
      };
      await emitirEvento(CONFIG.topicEstados, ctx.shipmentId, eventoFallo);
      console.log(`❌ Intento #${intento} fallido por AUSENTE.`);
      await sleep(1200);
    }
    console.log(`🚨 El envío ${ctx.shipmentId} alcanzó 3 fallos -> Corresponde: 'Devolución al comercio'.\n`);
  },

  // Escenario 5: Ráfaga de 500 envíos en pocos segundos (para ver acumulación y recuperación)
  async rafaga() {
    console.log('\n======================================================');
    console.log('🚀 [ESCENARIO 5] Ráfaga Masiva de 500 Envíos');
    console.log('======================================================');
    const totalRafaga = 500;
    const batchSize = 50;

    for (let i = 0; i < totalRafaga; i += batchSize) {
      const lote = [];
      for (let j = 0; j < batchSize && (i + j) < totalRafaga; j++) {
        const ctx = crearContextoEnvio();
        const ev = crearPayloadBase(ctx, 'CREADO');
        lote.push(emitirEvento(CONFIG.topicEstados, ctx.shipmentId, ev));
      }
      await Promise.all(lote);
      process.stdout.write(`\rProgreso de ráfaga: ${Math.min(i + batchSize, totalRafaga)} / ${totalRafaga} eventos CREADO emitidos.`);
      await sleep(80);
    }
    console.log('\n✅ 500 envíos emitidos a envios.estados exitosamente.\n');
  },
};

// ============================================================================
// BUCLE DE GENERACIÓN CONTINUA (20 envíos/minuto)
// ============================================================================
let runningContinuo = false;

function iniciarGeneracionContinua() {
  if (runningContinuo) return;
  runningContinuo = true;
  console.log(`\n▶️ INICIANDO FLUJO CONTINUO (20 envíos/min = 1 cada 3 segundos)...`);

  const timer = setInterval(() => {
    if (!runningContinuo) {
      clearInterval(timer);
      return;
    }
    const ctx = crearContextoEnvio();
    procesarCicloDeVida(ctx);
  }, CONFIG.intervalMsContinuo);
}

// ============================================================================
// MENÚ CONSOLA INTERACTIVA
// ============================================================================
function mostrarMenu() {
  console.log(`
┌────────────────────────────────────────────────────────┐
│               RutaSur - Simulador de Eventos           │
│   Reloj: 30s real = 1h sim | Topic: envios.estados     │
├────────────────────────────────────────────────────────┤
│  [1] Ejecutar Escenario 1: Envío Feliz                 │
│  [2] Ejecutar Escenario 2: Ausente (Reprogramar)       │
│  [3] Ejecutar Escenario 3: Demorado en Hub (SLA Risk)  │
│  [4] Ejecutar Escenario 4: Tercer Fallo (Devolución)   │
│  [5] Ejecutar Escenario 5: Ráfaga de 500 envíos        │
│  [C] Iniciar/Pausar Flujo Continuo (20 env/min)        │
│  [Q] Salir                                             │
└────────────────────────────────────────────────────────┘
Selecciona una opción: `);
}

async function iniciarConsolaInteractiva() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  mostrarMenu();

  rl.on('line', async (line) => {
    const opcion = line.trim().toUpperCase();
    switch (opcion) {
      case '1':
        await Escenarios.envioFeliz();
        break;
      case '2':
        await Escenarios.ausente();
        break;
      case '3':
        await Escenarios.demorado();
        break;
      case '4':
        await Escenarios.tercerFallo();
        break;
      case '5':
        await Escenarios.rafaga();
        break;
      case 'C':
        runningContinuo = !runningContinuo;
        if (runningContinuo) {
          iniciarGeneracionContinua();
        } else {
          console.log('\n⏸️ Flujo continuo pausado.');
        }
        break;
      case 'Q':
        console.log('\nCerrando simulador...');
        await producer.disconnect();
        process.exit(0);
        break;
      default:
        console.log('Opción no reconocida.');
        break;
    }
    mostrarMenu();
  });
}

// ============================================================================
// ARRANQUE PRINCIPAL
// ============================================================================
async function main() {
  console.log('Conectando productor al broker Kafka...');
  await producer.connect();
  console.log(`✅ Conectado a Kafka en: ${CONFIG.kafkaBrokers.join(', ')}`);

  const args = process.argv.slice(2);
  const scenarioIndex = args.indexOf('--escenario');

  if (scenarioIndex !== -1 && args[scenarioIndex + 1]) {
    const esc = args[scenarioIndex + 1];
    if (esc === '1') await Escenarios.envioFeliz();
    else if (esc === '2') await Escenarios.ausente();
    else if (esc === '3') await Escenarios.demorado();
    else if (esc === '4') await Escenarios.tercerFallo();
    else if (esc === '5') await Escenarios.rafaga();
    else console.error('Escenario inválido. Opciones válidas: 1, 2, 3, 4, 5');

    await producer.disconnect();
    process.exit(0);
  }

  // Sin argumentos CLI: abre el menú interactivo
  iniciarConsolaInteractiva();
}

main().catch((err) => {
  console.error('Error fatal al iniciar simulador:', err);
  process.exit(1);
});
