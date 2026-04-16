//notIndeterminado.ts es el tercero que ejecuto, después de cleaner.ts y from.ts
import * as fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';

// Configuración de archivos
const inputFile = '../csv/datosFrom.csv';
const outputFile = '../csv/datosDeterminados.csv';

// Valores a filtrar en la columna "From"
const valoresBaneados = ['Indeterminado', 'No clasificar', 'No clasificado'];

interface Row {
  From: string;
  [key: string]: any; // Para aceptar otras columnas dinámicamente
}

const filtrarCsv = async () => {
  const results: Row[] = [];

  console.log('--- Iniciando procesamiento del CSV ---');

  // 1. Leer y filtrar
  fs.createReadStream(inputFile)
    .pipe(csv())
    .on('data', (data: Row) => {
      // Verificamos si el valor de "From" NO está en nuestra lista negra
      if (!valoresBaneados.includes(data.From)) {
        results.push(data);
      }
    })
    .on('end', async () => {
      if (results.length === 0) {
        console.log('No se encontraron filas que cumplan con el criterio.');
        return;
      }

      // 2. Extraer los headers dinámicamente del primer objeto encontrado
      const headers = Object.keys(results[0]).map((key) => ({
        id: key,
        title: key,
      }));

      // 3. Configurar el Writer
      const csvWriter = createCsvWriter({
        path: outputFile,
        header: headers,
      });

      // 4. Escribir el nuevo archivo
      try {
        await csvWriter.writeRecords(results);
        console.log(`¡Éxito! Archivo guardado en: ${outputFile}`);
        console.log(`Filas resultantes: ${results.length}`);
      } catch (error) {
        console.error('Error al escribir el archivo:', error);
      }
    });
};

filtrarCsv();
