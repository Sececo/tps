//tps.services.ts es el sexto que ejecuto, después de cleaner.ts, from.ts, notIndeterminado.ts, ollama.services.ts y preFinalProcess.ts
import * as fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';
import dotenv from 'dotenv';

dotenv.config();

const inputFile = '../csv/datosDeterminado.csv';
const outputFile = '../csv/datosFinales.csv';
const baseUrl = process.env.BASE_URL;

const results: any[] = [];

fs.createReadStream(inputFile)
  .pipe(csv())
  .on('data', (row) => {
    const name = row.Adress?.trim() || '';
    const location = row.Zip?.trim() || '';

    row.url = `${baseUrl}?name=${encodeURIComponent(name)}&citystatezip=${encodeURIComponent(location)}`;

    results.push(row);
  })
  .on('end', () => {
    if (results.length === 0) {
      console.log('El archivo está vacío o no tiene el formato correcto.');
      return;
    }

    // 2. Configurar el Writer con las cabeceras dinámicas
    const headers = Object.keys(results[0]).map((key) => ({
      id: key,
      title: key,
    }));

    const csvWriter = createCsvWriter({
      path: outputFile,
      header: headers,
    });

    csvWriter
      .writeRecords(results)
      .then(() =>
        console.log(`Proceso terminado. Archivo guardado en: ${outputFile}`),
      )
      .catch((err) => console.error('Error al escribir el CSV:', err));
  });
