//preFinalProcess.ts es el quinto que ejecuto, después de cleaner.ts, from.ts, notIndeterminado.ts y ollama.services.ts
import * as fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';

const sourceFile = '../csv/datosOllama.csv'; // El que quieres leer
const targetFile = '../csv/datosDeterminado.csv'; // Donde quieres anexar

const anexarCsv = async () => {
  const records: any[] = [];

  console.log(`Leyendo datos de ${sourceFile}...`);

  // 1. Leemos el archivo origen
  fs.createReadStream(sourceFile)
    .pipe(csv())
    .on('data', (data) => records.push(data))
    .on('end', async () => {
      if (records.length === 0) {
        console.log('No hay datos para anexar.');
        return;
      }

      // 2. Extraer headers dinámicamente
      const headers = Object.keys(records[0]).map((key) => ({
        id: key,
        title: key,
      }));

      // 3. Configurar el Writer con 'append: true'
      // IMPORTANTE: append: true evita que se sobrescriba el archivo
      const csvWriter = createCsvWriter({
        path: targetFile,
        header: headers,
        append: true,
      });

      try {
        await csvWriter.writeRecords(records);
        console.log(`--- Anexado con éxito ---`);
        console.log(`Se agregaron ${records.length} filas a ${targetFile}`);
      } catch (error) {
        console.error('Error al anexar los datos:', error);
      }
    });
};

anexarCsv();
