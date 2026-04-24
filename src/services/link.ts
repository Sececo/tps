//tps.services.ts es el sexto que ejecuto, después de cleaner.ts, from.ts, notIndeterminado.ts, ollama.services.ts y preFinalProcess.ts
import * as fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';

export async function agregarLinks(inputFile: string, outputFile: string) {
  const CITY = process.env.CITY || 'default_city';
  const firstUrl = process.env.FIRST_URL;
  const secondUrl = process.env.SECOND_URL;
  const thirdUrl = process.env.THIRD_URL;
  const results: any[] = [];
  fs.createReadStream(inputFile)
    .pipe(csv())
    .on('data', (row) => {
      const address = row.Address?.trim() || '';
      const location = `${CITY},MA`;
      const zip = row.Zip?.trim() || '';
      const district = row.District?.trim() || '';
      const fullAddress = `${address}, ${district}, ${zip}`;
      if (!(row.Tps || '').includes(thirdUrl)) {
        row.Tps = `${firstUrl}${encodeURIComponent(address)}&citystatezip=${encodeURIComponent(location)}`;
      }

      row.Maps_1 = `${secondUrl}${encodeURIComponent(address.replace(/\s+/g, '+'))},${encodeURIComponent(zip)}`;
      row.Maps_2 = `${secondUrl}${encodeURIComponent(fullAddress.replace(/\s+/g, '+'))}`;
      row.Maps_3 = `${secondUrl}${encodeURIComponent(address.replace(/\s+/g, '+'))},${encodeURIComponent(location)}`;

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
}
