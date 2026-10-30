import { MongoClient } from 'mongodb';

const uri =
  process.env.MONGODB_URI ||
  'mongodb://127.0.0.1:27017/rutasur?replicaSet=rs0&directConnection=true';

export const DB_NAME = process.env.MONGODB_DB || 'rutasur';
export const COLLECTION_ENVIOS = process.env.MONGODB_COLLECTION || 'envios';

let client: MongoClient;
let clientPromise: Promise<MongoClient>;

declare global {
  // eslint-disable-next-line no-var
  var _mongoClientPromise: Promise<MongoClient> | undefined;
}

if (process.env.NODE_ENV === 'development') {
  // En desarrollo usamos una variable global para preservar la conexión entre recargas de HMR
  if (!global._mongoClientPromise) {
    client = new MongoClient(uri);
    global._mongoClientPromise = client.connect();
  }
  clientPromise = global._mongoClientPromise;
} else {
  // En producción se instancia directamente
  client = new MongoClient(uri);
  clientPromise = client.connect();
}

export default clientPromise;
