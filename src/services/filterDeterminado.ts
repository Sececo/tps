//notIndeterminado.ts es el tercero que ejecuto, después de cleaner.ts y from.ts
import * as fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';

// Configuración de archivos
const inputFile = '../csv/datosFrom.csv';
const outputFileDeterminados = '../csv/datosDeterminados.csv';
const outputFileIndeterminados = '../csv/datosIndeterminados.csv';

// Valores a filtrar en la columna "From"
const valoresAdmitidos = ['Hispano', 'Estadounidense', 'Bussiness'];

interface Row {
  From: string;
  [key: string]: any; // Para aceptar otras columnas dinámicamente
}

const filtrarCsv = async () => {
  const resultsDeterminados: Row[] = [];
  const resultsIndeterminados: Row[] = [];

  console.log('--- Iniciando procesamiento del CSV ---');

  // 1. Leer y filtrar
  fs.createReadStream(inputFile)
    .pipe(csv())
    .on('data', (data: Row) => {
      // Verificamos si el valor de "From" NO está en nuestra lista negra
      if (valoresAdmitidos.includes(data.From)) {
        resultsDeterminados.push(data);
      } else {
        resultsIndeterminados.push(data);
      }
    })
    .on('end', async () => {
      if (resultsDeterminados.length === 0) {
        console.log(
          'No se encontraron filas que cumplan con el criterio.' +
            valoresAdmitidos.join(', '),
        );
        return;
      }

      // 2. Extraer los headers dinámicamente del primer objeto encontrado
      const headers = Object.keys(resultsDeterminados[0]).map((key) => ({
        id: key,
        title: key,
      }));

      // 3. Configurar el Writer
      const csvWriterDeterminados = createCsvWriter({
        path: outputFileDeterminados,
        header: headers,
      });

      const csvWriterIndeterminados = createCsvWriter({
        path: outputFileIndeterminados,
        header: headers,
      });

      // 4. Escribir el nuevo archivo
      try {
        await csvWriterDeterminados.writeRecords(resultsDeterminados);
        console.log('🟢 DETERMINADOS:');
        console.log(`¡Éxito! Archivo guardado en: ${outputFileDeterminados}`);
        console.log(
          `Filas con valores Determinados: ${resultsDeterminados.length}`,
        );
        await csvWriterIndeterminados.writeRecords(resultsIndeterminados);
        console.log('🔴 INDETERMINADOS:');
        console.log(`¡Éxito! Archivo guardado en: ${outputFileIndeterminados}`);
        console.log(
          `Filas con valores Indeterminados: ${resultsIndeterminados.length}`,
        );
      } catch (error) {
        console.error('Error al escribir el archivo:', error);
      }
    });
};

filtrarCsv();
