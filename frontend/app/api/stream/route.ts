import { NextRequest } from 'next/server';
import clientPromise, { DB_NAME, COLLECTION_ENVIOS } from '@/lib/mongodb';
import type { ChangeStream } from 'mongodb';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  let changeStream: ChangeStream | null = null;
  let heartbeatInterval: NodeJS.Timeout | null = null;

  const stream = new ReadableStream({
    async start(controller) {
      const encoder = new TextEncoder();

      // Función helper para enviar mensajes formateados en protocolo SSE
      const sendSse = (data: any, eventName?: string) => {
        try {
          let payload = '';
          if (eventName) {
            payload += `event: ${eventName}\n`;
          }
          payload += `data: ${JSON.stringify(data)}\n\n`;
          controller.enqueue(encoder.encode(payload));
        } catch (err) {
          console.warn('⚠️ No se pudo encolar mensaje SSE (posible desconexión):', err);
        }
      };

      try {
        const client = await clientPromise;
        const db = client.db(DB_NAME);
        const collection = db.collection(COLLECTION_ENVIOS);

        // 1. Mensaje de bienvenida al conectarse el cliente
        sendSse(
          {
            tipo: 'CONEXION_ESTABLECIDA',
            mensaje: 'Conexión SSE activa con MongoDB Change Streams (Replica Set rs0)',
            conectadoEn: new Date().toISOString(),
          },
          'conexion'
        );

        // 2. Abrir Change Stream en la colección 'envios'
        // fullDocument: 'updateLookup' asegura recibir el documento completo post-actualización
        changeStream = collection.watch([], {
          fullDocument: 'updateLookup',
        });

        console.log('📡 [SSE / Change Stream] Observador de MongoDB activado en rutasur.envios');

        // Escuchar eventos de cambio (insert, update, replace)
        changeStream.on('change', (change: any) => {
          if (['insert', 'update', 'replace'].includes(change.operationType)) {
            const doc = change.fullDocument;
            if (doc) {
              sendSse({
                operacion: change.operationType,
                envio: doc,
                timestamp: new Date().toISOString(),
              });
            }
          }
        });

        changeStream.on('error', (err: any) => {
          console.error('❌ Error en el Change Stream de MongoDB:', err);
          sendSse({ error: err.message }, 'error');
        });
      } catch (err: any) {
        console.error('❌ Error fatal al inicializar Change Stream:', err);
        sendSse({ error: err.message }, 'error');
        controller.close();
        return;
      }

      // 3. Heartbeat periódico cada 15 segundos para mantener abierto el canal HTTP
      heartbeatInterval = setInterval(() => {
        try {
          controller.enqueue(encoder.encode(': heartbeat keep-alive\n\n'));
        } catch {
          if (heartbeatInterval) clearInterval(heartbeatInterval);
        }
      }, 15000);

      // 4. Limpieza ante desconexión del cliente (cierre de pestaña o recarga)
      request.signal.addEventListener('abort', async () => {
        console.log('🔌 [SSE] Cliente desconectado. Liberando Change Stream.');
        if (heartbeatInterval) clearInterval(heartbeatInterval);
        if (changeStream) {
          try {
            await changeStream.close();
          } catch {}
        }
      });
    },

    async cancel() {
      if (heartbeatInterval) clearInterval(heartbeatInterval);
      if (changeStream) {
        try {
          await changeStream.close();
        } catch {}
      }
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no', // Evita buffering en proxies Nginx
    },
  });
}
