import { NextResponse } from 'next/server';
import clientPromise, { DB_NAME, COLLECTION_ENVIOS } from '@/lib/mongodb';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const client = await clientPromise;
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_ENVIOS);

    // Obtener los envíos existentes ordenados por fecha de actualización descendente
    const envios = await collection
      .find({})
      .sort({ actualizadoEn: -1, creadoEn: -1 })
      .limit(150)
      .toArray();

    return NextResponse.json({
      success: true,
      count: envios.length,
      data: envios,
    });
  } catch (error: any) {
    console.error('❌ Error al consultar envíos en MongoDB:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Error al conectar con la base de datos',
      },
      { status: 500 }
    );
  }
}
