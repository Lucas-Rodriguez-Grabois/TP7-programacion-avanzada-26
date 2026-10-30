'use client';

import React, { useEffect, useState, useMemo, useRef } from 'react';
import {
  Package,
  Truck,
  CheckCircle2,
  AlertTriangle,
  RotateCcw,
  Building2,
  ArrowRight,
  Radio,
  Search,
  Filter,
  RefreshCw,
  Clock,
  Layers,
  MapPin,
  User,
  ShieldCheck,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';

interface HistorialItem {
  eventId: string;
  tipo: string;
  secuencia: number;
  ocurridoEn: string;
  hubId?: string;
  zonaId?: string;
  repartidorId?: string;
  motivo?: string;
  procesadoEn?: string;
}

interface Envio {
  _id?: string;
  shipmentId: string;
  estado: string;
  hubId?: string;
  zonaId?: string;
  repartidorId?: string;
  intentos?: number;
  ultimaSecuencia?: number;
  motivoFallo?: string;
  historial?: HistorialItem[];
  creadoEn?: string;
  actualizadoEn?: string;
}

export default function DashboardRutaSur() {
  const [envios, setEnvios] = useState<Envio[]>([]);
  const [loading, setLoading] = useState(true);
  const [sseConectado, setSseConectado] = useState(false);
  const [contadorEventosSse, setContadorEventosSse] = useState(0);
  const [busqueda, setBusqueda] = useState('');
  const [filtroEstado, setFiltroEstado] = useState('TODOS');
  const [filtroHub, setFiltroHub] = useState('TODOS');
  const [envioSeleccionado, setEnvioSeleccionado] = useState<Envio | null>(null);
  const [recientesIds, setRecientesIds] = useState<Set<string>>(new Set());

  // 1. Fetch Inicial de Envíos desde MongoDB
  const cargarEnviosIniciales = async () => {
    try {
      setLoading(true);
      const res = await fetch('/api/envios');
      const json = await res.json();
      if (json.success && Array.isArray(json.data)) {
        setEnvios(json.data);
      }
    } catch (err) {
      console.error('Error al cargar envíos iniciales:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    cargarEnviosIniciales();
  }, []);

  // 2. Conexión Reactiva en Tiempo Real con Server-Sent Events (SSE)
  useEffect(() => {
    let eventSource: EventSource | null = null;
    let reconectarTimeout: NodeJS.Timeout;

    const conectarSSE = () => {
      console.log('🔌 Conectando a /api/stream (SSE)...');
      eventSource = new EventSource('/api/stream');

      eventSource.onopen = () => {
        setSseConectado(true);
        console.log('✅ Canal SSE conectado con éxito.');
      };

      eventSource.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);

          if (payload.envio && payload.envio.shipmentId) {
            const envioActualizado: Envio = payload.envio;
            const id = envioActualizado.shipmentId;

            setContadorEventosSse((prev) => prev + 1);

            // Destacar temporalmente la tarjeta modificada con una animación
            setRecientesIds((prev) => new Set(prev).add(id));
            setTimeout(() => {
              setRecientesIds((prev) => {
                const next = new Set(prev);
                next.delete(id);
                return next;
              });
            }, 2500);

            // Actualización atómica en el estado de React
            setEnvios((prevEnvios) => {
              const index = prevEnvios.findIndex((e) => e.shipmentId === id);
              if (index >= 0) {
                const copia = [...prevEnvios];
                copia[index] = envioActualizado;
                return copia;
              } else {
                // Nuevo envío ingresado
                return [envioActualizado, ...prevEnvios];
              }
            });
          }
        } catch (err) {
          console.warn('Error al procesar mensaje SSE:', err);
        }
      };

      eventSource.onerror = (err) => {
        console.warn('⚠️ Desconexión de SSE. Reintentando en 3s...', err);
        setSseConectado(false);
        if (eventSource) {
          eventSource.close();
        }
        reconectarTimeout = setTimeout(conectarSSE, 3000);
      };
    };

    conectarSSE();

    return () => {
      if (reconectarTimeout) clearTimeout(reconectarTimeout);
      if (eventSource) eventSource.close();
    };
  }, []);

  // 3. Contadores Resumen de Estados
  const metricas = useMemo(() => {
    const total = envios.length;
    let entregados = 0;
    let enReparto = 0;
    let fallidos = 0;
    let enHub = 0;

    envios.forEach((e) => {
      if (e.estado === 'ENTREGADO') entregados++;
      else if (e.estado === 'EN_REPARTO') enReparto++;
      else if (['ENTREGA_FALLIDA', 'DEVOLUCION_ORIGEN'].includes(e.estado)) fallidos++;
      else if (['INGRESADO_HUB', 'CLASIFICADO'].includes(e.estado)) enHub++;
    });

    return { total, entregados, enReparto, fallidos, enHub };
  }, [envios]);

  // 4. Filtrado y Búsqueda
  const enviosFiltrados = useMemo(() => {
    return envios.filter((envio) => {
      const matchBusqueda =
        busqueda === '' ||
        envio.shipmentId.toLowerCase().includes(busqueda.toLowerCase()) ||
        (envio.hubId && envio.hubId.toLowerCase().includes(busqueda.toLowerCase())) ||
        (envio.zonaId && envio.zonaId.toLowerCase().includes(busqueda.toLowerCase())) ||
        (envio.repartidorId && envio.repartidorId.toLowerCase().includes(busqueda.toLowerCase()));

      const matchEstado =
        filtroEstado === 'TODOS' ||
        envio.estado === filtroEstado ||
        (filtroEstado === 'FALLIDOS' && ['ENTREGA_FALLIDA', 'DEVOLUCION_ORIGEN'].includes(envio.estado));

      const matchHub = filtroHub === 'TODOS' || envio.hubId === filtroHub;

      return matchBusqueda && matchEstado && matchHub;
    });
  }, [envios, busqueda, filtroEstado, filtroHub]);

  // 5. Helper de estilos según estado
  const obtenerConfigEstado = (estado: string) => {
    switch (estado) {
      case 'ENTREGADO':
        return {
          badge: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
          border: 'hover:border-emerald-500/50',
          accent: 'bg-emerald-500',
          icon: CheckCircle2,
          label: 'ENTREGADO',
        };
      case 'EN_REPARTO':
        return {
          badge: 'bg-blue-500/15 text-blue-400 border-blue-500/30',
          border: 'hover:border-blue-500/50',
          accent: 'bg-blue-500',
          icon: Truck,
          label: 'EN REPARTO',
        };
      case 'ENTREGA_FALLIDA':
        return {
          badge: 'bg-rose-500/15 text-rose-400 border-rose-500/30',
          border: 'hover:border-rose-500/50',
          accent: 'bg-rose-500',
          icon: AlertTriangle,
          label: 'ENTREGA FALLIDA',
        };
      case 'REPROGRAMADO':
        return {
          badge: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
          border: 'hover:border-amber-500/50',
          accent: 'bg-amber-500',
          icon: RotateCcw,
          label: 'REPROGRAMADO',
        };
      case 'DEVOLUCION_ORIGEN':
        return {
          badge: 'bg-purple-500/15 text-purple-400 border-purple-500/30',
          border: 'hover:border-purple-500/50',
          accent: 'bg-purple-500',
          icon: AlertTriangle,
          label: 'DEVOLUCIÓN ORIGEN',
        };
      case 'INGRESADO_HUB':
        return {
          badge: 'bg-indigo-500/15 text-indigo-400 border-indigo-500/30',
          border: 'hover:border-indigo-500/50',
          accent: 'bg-indigo-500',
          icon: Building2,
          label: 'EN HUB',
        };
      case 'CLASIFICADO':
        return {
          badge: 'bg-cyan-500/15 text-cyan-400 border-cyan-500/30',
          border: 'hover:border-cyan-500/50',
          accent: 'bg-cyan-500',
          icon: Layers,
          label: 'CLASIFICADO',
        };
      default:
        return {
          badge: 'bg-slate-700/30 text-slate-300 border-slate-700',
          border: 'hover:border-slate-500/50',
          accent: 'bg-slate-500',
          icon: Package,
          label: estado || 'CREADO',
        };
    }
  };

  return (
    <div className="min-h-screen bg-[#080d1a] text-slate-100 p-4 md:p-8">
      {/* HEADER DE LA TORRE DE CONTROL */}
      <header className="mb-8 border-b border-slate-800/80 pb-6">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
          <div>
            <div className="flex items-center gap-3">
              <div className="p-2.5 bg-gradient-to-br from-indigo-500 to-blue-600 rounded-xl shadow-lg shadow-indigo-500/20">
                <Truck className="w-7 h-7 text-white" />
              </div>
              <div>
                <h1 className="text-2xl md:text-3xl font-extrabold tracking-tight bg-gradient-to-r from-white via-slate-100 to-slate-400 bg-clip-text text-transparent">
                  RutaSur Logistics
                </h1>
                <p className="text-xs md:text-sm text-slate-400 font-medium">
                  Torre de Control en Vivo • MongoDB Replica Set Change Streams & SSE
                </p>
              </div>
            </div>
          </div>

          {/* INDICADORES DE CONECTIVIDAD EN TIEMPO REAL */}
          <div className="flex items-center gap-3 self-start md:self-auto">
            <button
              onClick={cargarEnviosIniciales}
              className="p-2 bg-slate-900 border border-slate-700/80 hover:bg-slate-800 text-slate-300 rounded-lg text-xs font-semibold flex items-center gap-2 transition"
              title="Refrescar datos manualmente"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
              <span className="hidden sm:inline">Refrescar</span>
            </button>

            <div
              className={`px-3 py-1.5 rounded-full border text-xs font-semibold flex items-center gap-2 ${
                sseConectado
                  ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30'
                  : 'bg-rose-500/10 text-rose-400 border-rose-500/30 animate-pulse'
              }`}
            >
              <span
                className={`w-2 h-2 rounded-full ${
                  sseConectado ? 'bg-emerald-400 animate-ping' : 'bg-rose-500'
                }`}
              />
              <span>{sseConectado ? 'SSE Conectado' : 'SSE Desconectado'}</span>
              {contadorEventosSse > 0 && (
                <span className="ml-1 px-1.5 py-0.2 bg-emerald-500/20 text-emerald-300 rounded text-[10px]">
                  {contadorEventosSse} evts
                </span>
              )}
            </div>
          </div>
        </div>

        {/* MÉTRICAS DE RESUMEN */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3 mt-6">
          <div className="bg-slate-900/70 border border-slate-800 rounded-xl p-4">
            <span className="text-xs font-medium text-slate-400 flex items-center gap-1.5">
              <Package className="w-4 h-4 text-slate-400" /> Total Envíos
            </span>
            <span className="text-2xl font-bold mt-1 block">{metricas.total}</span>
          </div>

          <div className="bg-slate-900/70 border border-slate-800 rounded-xl p-4">
            <span className="text-xs font-medium text-indigo-400 flex items-center gap-1.5">
              <Building2 className="w-4 h-4 text-indigo-400" /> En Hubs
            </span>
            <span className="text-2xl font-bold mt-1 text-indigo-300 block">{metricas.enHub}</span>
          </div>

          <div className="bg-slate-900/70 border border-slate-800 rounded-xl p-4">
            <span className="text-xs font-medium text-blue-400 flex items-center gap-1.5">
              <Truck className="w-4 h-4 text-blue-400" /> En Reparto
            </span>
            <span className="text-2xl font-bold mt-1 text-blue-300 block">{metricas.enReparto}</span>
          </div>

          <div className="bg-slate-900/70 border border-slate-800 rounded-xl p-4">
            <span className="text-xs font-medium text-emerald-400 flex items-center gap-1.5">
              <CheckCircle2 className="w-4 h-4 text-emerald-400" /> Entregados
            </span>
            <span className="text-2xl font-bold mt-1 text-emerald-300 block">{metricas.entregados}</span>
          </div>

          <div className="bg-slate-900/70 border border-slate-800 rounded-xl p-4 col-span-2 sm:col-span-1">
            <span className="text-xs font-medium text-rose-400 flex items-center gap-1.5">
              <AlertTriangle className="w-4 h-4 text-rose-400" /> Fallidos / Devolución
            </span>
            <span className="text-2xl font-bold mt-1 text-rose-300 block">{metricas.fallidos}</span>
          </div>
        </div>
      </header>

      {/* BARRA DE FILTROS Y BÚSQUEDA */}
      <div className="flex flex-col md:flex-row gap-3 mb-6">
        <div className="relative flex-1">
          <Search className="w-4 h-4 absolute left-3 top-3.5 text-slate-400" />
          <input
            type="text"
            placeholder="Buscar por ID de envío, Hub, Zona o Repartidor..."
            value={busqueda}
            onChange={(e) => setBusqueda(e.target.value)}
            className="w-full pl-9 pr-4 py-2.5 bg-slate-900/90 border border-slate-800 focus:border-indigo-500 rounded-xl text-sm text-slate-100 placeholder-slate-500 outline-none transition"
          />
        </div>

        <div className="flex gap-2">
          <select
            value={filtroEstado}
            onChange={(e) => setFiltroEstado(e.target.value)}
            className="bg-slate-900 border border-slate-800 text-slate-300 px-3 py-2.5 rounded-xl text-sm outline-none cursor-pointer focus:border-indigo-500"
          >
            <option value="TODOS">Todos los Estados</option>
            <option value="CREADO">CREADO</option>
            <option value="INGRESADO_HUB">INGRESADO_HUB</option>
            <option value="CLASIFICADO">CLASIFICADO</option>
            <option value="EN_REPARTO">EN_REPARTO</option>
            <option value="ENTREGADO">ENTREGADO</option>
            <option value="FALLIDOS">FALLIDOS / DEVOLUCIÓN</option>
            <option value="REPROGRAMADO">REPROGRAMADO</option>
          </select>

          <select
            value={filtroHub}
            onChange={(e) => setFiltroHub(e.target.value)}
            className="bg-slate-900 border border-slate-800 text-slate-300 px-3 py-2.5 rounded-xl text-sm outline-none cursor-pointer focus:border-indigo-500"
          >
            <option value="TODOS">Todos los Hubs</option>
            <option value="HUB-CENTRAL">HUB-CENTRAL</option>
            <option value="HUB-NORTE">HUB-NORTE</option>
            <option value="HUB-SUR">HUB-SUR</option>
          </select>
        </div>
      </div>

      {/* GRILLA DE TARJETAS DE ENVÍOS */}
      {loading && envios.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-20 text-slate-500">
          <RefreshCw className="w-8 h-8 animate-spin mb-3 text-indigo-400" />
          <p className="text-sm">Conectando a MongoDB y recuperando estado inicial...</p>
        </div>
      ) : enviosFiltrados.length === 0 ? (
        <div className="text-center py-16 bg-slate-900/40 border border-dashed border-slate-800 rounded-2xl">
          <Package className="w-12 h-12 text-slate-600 mx-auto mb-3" />
          <p className="text-base font-semibold text-slate-300">No se encontraron envíos</p>
          <p className="text-xs text-slate-500 mt-1">
            Ejecuta el simulador en una terminal para generar eventos en tiempo real:
          </p>
          <code className="inline-block mt-3 px-3 py-1 bg-slate-950 border border-slate-800 rounded-md text-xs text-indigo-300 font-mono">
            npm run demo:feliz
          </code>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {enviosFiltrados.map((envio) => {
            const config = obtenerConfigEstado(envio.estado);
            const EstadoIcono = config.icon;
            const esReciente = recientesIds.has(envio.shipmentId);

            return (
              <div
                key={envio.shipmentId}
                onClick={() => setEnvioSeleccionado(envio)}
                className={`group relative bg-slate-900/80 border rounded-2xl p-5 cursor-pointer transition-all duration-300 shadow-md ${
                  esReciente
                    ? 'border-indigo-400 bg-indigo-950/20 scale-[1.01] ring-2 ring-indigo-500/30'
                    : 'border-slate-800/90 hover:border-slate-700 hover:bg-slate-900'
                }`}
              >
                {/* LÍNEA DE ACENTO SUPERIOR */}
                <div className={`absolute top-0 left-5 right-5 h-[2px] rounded-t-full ${config.accent}`} />

                {/* HEADER DE LA TARJETA */}
                <div className="flex items-start justify-between gap-3 mb-3">
                  <div>
                    <span className="text-[11px] font-mono font-medium text-slate-400">
                      ID DE ENVÍO
                    </span>
                    <h3 className="text-lg font-bold text-slate-100 tracking-wide font-mono">
                      {envio.shipmentId}
                    </h3>
                  </div>

                  {/* BADGE DE ESTADO CON COLOR DINÁMICO */}
                  <span
                    className={`px-2.5 py-1 rounded-full text-xs font-semibold border flex items-center gap-1.5 ${config.badge}`}
                  >
                    <EstadoIcono className="w-3.5 h-3.5" />
                    {config.label}
                  </span>
                </div>

                {/* DETALLES OPERATIVOS */}
                <div className="space-y-2 text-xs text-slate-300 mt-3 pt-3 border-t border-slate-800/60">
                  <div className="flex justify-between items-center">
                    <span className="text-slate-400 flex items-center gap-1.5">
                      <Building2 className="w-3.5 h-3.5 text-slate-500" /> Centro Logístico:
                    </span>
                    <span className="font-semibold text-slate-200 font-mono">
                      {envio.hubId || 'Pendiente'}
                    </span>
                  </div>

                  <div className="flex justify-between items-center">
                    <span className="text-slate-400 flex items-center gap-1.5">
                      <MapPin className="w-3.5 h-3.5 text-slate-500" /> Zona de Entrega:
                    </span>
                    <span className="font-semibold text-slate-200">
                      {envio.zonaId || 'Sin asignar'}
                    </span>
                  </div>

                  <div className="flex justify-between items-center">
                    <span className="text-slate-400 flex items-center gap-1.5">
                      <User className="w-3.5 h-3.5 text-slate-500" /> Repartidor:
                    </span>
                    <span className="font-mono text-slate-200">
                      {envio.repartidorId || 'No asignado'}
                    </span>
                  </div>

                  <div className="flex justify-between items-center">
                    <span className="text-slate-400 flex items-center gap-1.5">
                      <RotateCcw className="w-3.5 h-3.5 text-slate-500" /> Intentos de Entrega:
                    </span>
                    <span
                      className={`font-bold px-2 py-0.5 rounded text-[11px] ${
                        (envio.intentos || 0) >= 3
                          ? 'bg-rose-500/20 text-rose-300 border border-rose-500/30'
                          : (envio.intentos || 0) > 0
                          ? 'bg-amber-500/20 text-amber-300 border border-amber-500/30'
                          : 'text-slate-300'
                      }`}
                    >
                      {envio.intentos || 0} / 3
                    </span>
                  </div>
                </div>

                {/* ALERTA DE MOTIVO DE FALLO (SI EXISTE) */}
                {envio.motivoFallo && (
                  <div className="mt-3 p-2 bg-rose-500/10 border border-rose-500/30 rounded-lg text-rose-300 text-xs flex items-center gap-2">
                    <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
                    <span className="truncate">Motivo: {envio.motivoFallo}</span>
                  </div>
                )}

                {/* FOOTER DE LA TARJETA */}
                <div className="mt-4 pt-2.5 border-t border-slate-800/40 flex items-center justify-between text-[11px] text-slate-400">
                  <span className="flex items-center gap-1">
                    <Clock className="w-3 h-3 text-slate-400" />
                    {envio.actualizadoEn
                      ? new Date(envio.actualizadoEn).toLocaleTimeString('es-AR')
                      : 'Ahora'}
                  </span>
                  <span className="font-mono text-[10px] text-slate-400">
                    Secuencia #{envio.ultimaSecuencia ?? 0}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* MODAL / DRAWER DETALLE DE HISTORIAL (EVENT SOURCING) */}
      {envioSeleccionado && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm animate-in fade-in">
          <div className="bg-slate-900 border border-slate-800 w-full max-w-xl rounded-2xl p-6 shadow-2xl max-h-[85vh] flex flex-col">
            <div className="flex items-start justify-between border-b border-slate-800 pb-4 mb-4">
              <div>
                <span className="text-xs font-mono text-indigo-400">HISTORIAL DE EVENTOS CQRS</span>
                <h2 className="text-xl font-bold font-mono text-white">
                  {envioSeleccionado.shipmentId}
                </h2>
              </div>
              <button
                onClick={() => setEnvioSeleccionado(null)}
                className="text-slate-400 hover:text-white px-2 py-1 rounded bg-slate-800 text-xs font-semibold"
              >
                Cerrar ✕
              </button>
            </div>

            <div className="overflow-y-auto space-y-3 pr-1 flex-1">
              <p className="text-xs text-slate-400 mb-2">
                Secuencia de eventos inmutables recibidos desde Kafka y consolidados en MongoDB:
              </p>

              {envioSeleccionado.historial && envioSeleccionado.historial.length > 0 ? (
                envioSeleccionado.historial.map((item, idx) => {
                  const cfg = obtenerConfigEstado(item.tipo);
                  const Icon = cfg.icon;

                  return (
                    <div
                      key={item.eventId || idx}
                      className="p-3 bg-slate-950/70 border border-slate-800/90 rounded-xl flex items-start gap-3 text-xs"
                    >
                      <div className={`p-1.5 rounded-lg border ${cfg.badge} shrink-0 mt-0.5`}>
                        <Icon className="w-3.5 h-3.5" />
                      </div>
                      <div className="flex-1">
                        <div className="flex items-center justify-between">
                          <span className="font-semibold text-slate-200">{item.tipo}</span>
                          <span className="text-[10px] font-mono text-slate-500">
                            Sec #{item.secuencia}
                          </span>
                        </div>
                        <p className="text-[11px] text-slate-400 mt-0.5">
                          {new Date(item.ocurridoEn).toLocaleString('es-AR')}
                        </p>
                        {item.motivo && (
                          <p className="text-rose-400 mt-1">Causa: {item.motivo}</p>
                        )}
                        {item.repartidorId && (
                          <p className="text-slate-400 mt-0.5">Repartidor: {item.repartidorId}</p>
                        )}
                      </div>
                    </div>
                  );
                })
              ) : (
                <p className="text-xs text-slate-500">Sin historial registrado.</p>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
