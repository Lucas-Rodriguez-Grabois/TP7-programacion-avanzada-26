# TP7: RutaSur - Plataforma de Logística Orientada a Eventos

**Asignatura:** Licenciatura en Sistemas de Información / Programación Avanzada - 2026  
**Ubicación:** `C:\Users\lrtr3\OneDrive\Desktop\facultad\programacion avanzada\TP7`

---

## 🏗️ 1. Infraestructura con Docker Compose

El archivo `docker-compose.yml` levanta:
- **Apache Kafka 3.8.0 en modo KRaft:** Broker único en el puerto `9092` (sin ZooKeeper).
- **MongoDB 7.0 en Replica Set (`rs0`):** En el puerto `27017`, necesario para Change Streams.
- **Kafka-Init:** Crea automáticamente los tópicos con **3 particiones** (`envios.estados`, `envios.promesas`, `envios.reprogramaciones`, `alertas.sla`, `notificaciones.enviadas`, `envios.estados.dlq`).
- **Mongo-Init:** Inicializa automáticamente el Replica Set `rs0`.
- **Kafka UI (Kafbat):** Interfaz web en `http://localhost:8080`.

### Iniciar la infraestructura
```bash
docker compose up -d
```

---

## 📦 2. Simulador de Eventos (`simulador.js`)

Actúa como productor en Kafka y reemplaza a los sistemas de origen.
- **Tópico destino:** `envios.estados` (y `envios.promesas`).
- **Clave de partición:** `shipmentId` (garantía de orden estricto por envío).
- **Reloj acelerado:** 30 segundos reales = 1 hora simulada (factor 120x).

### Ejecución del simulador:
```bash
npm start                  # Menú interactivo / Continuo
npm run demo:feliz         # Escenario 1: Envío feliz
npm run demo:ausente       # Escenario 2: Falla por AUSENTE (reprogramación)
npm run demo:demorado      # Escenario 3: Demora en Hub (riesgo SLA)
npm run demo:tercer-fallo  # Escenario 4: 3 fallos consecutivos
npm run demo:rafaga        # Escenario 5: Ráfaga de 500 envíos
```

---

## 📽️ 3. Microservicio Proyector (`proyector/`)

Consume `envios.estados` (groupId: `proyector`) y mantiene la colección de lectura `rutasur.envios` en MongoDB.
- **Idempotencia:** Descarta `eventId` duplicados.
- **Garantía de orden:** Descarta secuencias `<= ultimaSecuencia`.
- **DLQ:** Envía eventos corruptos a `envios.estados.dlq`.
- **Commit manual:** Solo después de persistir en MongoDB.

```bash
cd proyector
npm start
```

---

## 🔄 4. Microservicio Reprogramación (`reprogramacion/`)

Implementa el **Caso de Uso 2: Gestión de Entregas Fallidas**.
- **Consumidor:** Consume `envios.estados` (groupId: `reprogramacion`).
- **Filtrado:** Solo procesa eventos `ENTREGA_FALLIDA`.
- **Resiliencia ante carreras:** Sondea MongoDB verificando que el `eventId` esté presente en el historial del `Proyector`.
- **Reglas de negocio:**
  - `intentos < 3`: Publica `REPROGRAMADO` en `envios.reprogramaciones` con nueva fecha (+24h simuladas).
  - `intentos >= 3` o `motivo == RECHAZADO`: Publica `DEVOLUCION_ORIGEN` en `envios.reprogramaciones`.
- **Commit manual:** Solo después de publicar exitosamente el evento resultante.

```bash
cd reprogramacion
npm start
```

---

## 🚨 5. Microservicio Monitor de SLA (`alertas/`)

Implementa el **Caso de Uso 3: Alerta Temprana de Riesgo de SLA**.
- **Consumidor:** Consume `envios.estados` (groupId: `monitor-sla`).
- **Productor:** Emite a `alertas.sla` con clave de partición `shipmentId`.
- **Reloj acelerado:** Factor 120x (30 segundos reales = 1 hora simulada).
- **Lógica de detección:**
  - Registra paquetes en estado `INGRESADO_HUB`.
  - Evalúa continuamente el tiempo de permanencia mediante un evaluador en memoria.
  - Si un paquete permanece en el hub sin avanzar a `CLASIFICADO` por encima del umbral de SLA (por defecto 1.0 hora simulada = 30s reales), emite un evento `RIESGO_SLA`.
  - Si el paquete pasa a `CLASIFICADO`, se remueve del monitor.
  - Regla **sin repetición**: una misma regla no genera alertas duplicadas sobre el mismo envío.
- **Commit manual:** `autoCommit: false`, confirmado tras procesar cada mensaje.

```bash
cd alertas
npm start
```

---

## ✉️ 6. Microservicio de Notificaciones (`notificaciones/`)

Implementa el **Caso de Uso 4: Notificaciones al Cliente**.
- **Consumidor:** Consume desde `envios.estados` y `envios.reprogramaciones` (groupId: `notificaciones`, broker `127.0.0.1:9092`).
- **Productor:** Emite a `notificaciones.enviadas` con clave de partición `shipmentId`.
- **Filtrado específico:**
  - `ENTREGADO` (desde `envios.estados`).
  - `REPROGRAMADO` y `DEVOLUCION_ORIGEN` (desde `envios.reprogramaciones`).
  - Todo otro evento se descarta y se comitea su offset.
- **Payload canónico:**
  `{ idNotificacion, shipmentId, tipoEvento, mensaje, canal: 'EMAIL', fechaEnvio }`
- **Commit manual:** `autoCommit: false`, confirmado únicamente tras publicar exitosamente en `notificaciones.enviadas`.

```bash
cd notificaciones
npm start
```

---

## 🖥️ 7. Frontend Reactivo en Tiempo Real (`frontend/`)

Implementa la **Torre de Control de Envíos** con Next.js (App Router), React, Tailwind CSS y MongoDB Change Streams sobre SSE.
- **Tecnología:** Next.js 14, React 18, TypeScript, Tailwind CSS, Lucide Icons.
- **Backend SSE (`/api/stream`):** Abre un MongoDB Change Stream (`collection.watch([], { fullDocument: 'updateLookup' })`) conectado al Replica Set local `rs0` y envía eventos en tiempo real al navegador.
- **Endpoint Inicial (`/api/envios`):** Recupera los envíos existentes desde `rutasur.envios` al cargar la página.
- **Interfaz:**
  - Tablero moderno oscuro con contadores en vivo (Total, En Hub, En Reparto, Entregados, Fallidos).
  - Indicadores visuales dinámicos de colores según el estado (`ENTREGADO` verde, `EN_REPARTO` azul, `ENTREGA_FALLIDA` rojo, `REPROGRAMADO` ámbar, etc.).
  - Muestra `shipmentId`, `estado`, `hubId`, `intentos` (ej. 1 / 3), `zonaId` y `repartidorId`.
  - Animación de destaque al recibir actualizaciones en vivo.
  - Modal interactivo para inspeccionar el historial cronológico completo de eventos (*Event Sourcing*).

```bash
cd frontend
npm run dev
```
Acceder en el navegador: [http://localhost:3000](http://localhost:3000)



