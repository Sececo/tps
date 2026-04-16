// ollama.services.ts es el cuarto que ejecuto, después de cleaner.ts, from.ts y notIndeterminado.ts
import * as fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';

/**
 * CONFIGURACIÓN DEL SERVICIO
 */
const OLLAMA_API_URL = 'http://localhost:11434/api/generate';
const MODEL_NAME = 'gemma2:2b'; // Ajustado a tu versión estable
const BATCH_SIZE = 25; // Tamaño del bloque para procesamiento masivo
// Permitir retomar desde un lote específico
const START_BATCH = parseInt(process.argv[2] || '0', 10); // 👈 lee argumento de consola
const startIndex = START_BATCH * BATCH_SIZE;

interface FilaCSV {
  [key: string]: string;
}

/**
 * Función para consultar a Ollama en modo Batch
 */
const procesarBloqueOllama = async (
  nombres: string[],
): Promise<Record<string, string>> => {
  // Ajuste de contexto: Se exige explícitamente no usar markdown para no saturar el procesamiento
  // Mejora: Usamos salto de línea para que la IA distinga mejor los nombres compuestos
  const prompt = `Clasifica estos nombres como 'Hispano' o 'Estadounidense'.
  Responde ÚNICAMENTE con un objeto JSON. Mantén el nombre exacto como clave.
  Nombres a procesar:
  ${nombres.join('\n')}
  
  Formato requerido: {"Nombre Completo": "Clasificación"}`;

  try {
    const response = await fetch(OLLAMA_API_URL, {
      method: 'POST',
      body: JSON.stringify({
        model: MODEL_NAME,
        prompt: prompt,
        format: 'json',
        stream: false,
        options: {
          temperature: 0, // Bajamos la temperatura para que sea más determinista
          num_thread: 8,
          num_predict: 500, // Limita la generación para no saturar memoria/tiempo
          num_ctx: 1024, // Limita la ventana de contexto a lo estrictamente necesario
        },
      }),
    });

    const data: any = await response.json();

    // --- PARCHE DE SEGURIDAD ---
    if (!data || !data.response) {
      console.error(' - Ollama devolvió una respuesta vacía.');
      return {};
    }

    // Limpieza de caracteres que Ollama a veces añade por error
    const cleanResponse = data.response
      .trim()
      .replace(/^[^{]*/, '')
      .replace(/[^}]*$/, '');

    return JSON.parse(cleanResponse);
  } catch (error: any) {
    // Aquí capturamos el SyntaxError y evitamos que rompa el script
    console.error(`\n❌ Error en el JSON de Ollama: ${error.message}`);
    return {};
  }
};

/**
 * PROCESO PRINCIPAL
 */
const corregirIndeterminados = async () => {
  const inputPath = '../csv/datosFrom.csv';
  const outputPath = '../csv/datosOllama.csv'; // Archivo para los exitosos
  const indeterminadosPath = '../csv/indeterminadosOllama.csv'; // Archivo para los fallidos
  const logPath = '../logs/ollama_stats.txt';
  const todasLasFilas: FilaCSV[] = [];

  if (!fs.existsSync(inputPath)) {
    console.error(`❌ Error: No se encontró el archivo ${inputPath}`);
    return;
  }

  // --- 📊 TRACKER DE ESTADÍSTICAS ---
  const statsIA = {
    inicio: Date.now(),
    totalIndeterminados: 0,
    corregidosExitosamente: 0,
    fallidos: 0,
    lotesProcesados: 0,
  };

  console.log('🚀 Cargando datos en memoria...');

  const readStream = fs.createReadStream(inputPath).pipe(csv());
  for await (const row of readStream) {
    todasLasFilas.push(row);
  }

  // Identificamos las filas que Ollama debe procesar
  const indicesIndeterminados = todasLasFilas
    .map((row, index) => (row.From === 'Indeterminado' ? index : -1))
    .filter((index) => index !== -1);

  statsIA.totalIndeterminados = indicesIndeterminados.length;

  if (statsIA.totalIndeterminados === 0) {
    console.log("✅ No se encontraron nombres 'Indeterminados' para procesar.");
    return;
  }

  console.log(
    `📦 Procesando ${statsIA.totalIndeterminados} nombres en lotes de ${BATCH_SIZE}...`,
  );

  // --- PROCESAMIENTO POR LOTES ---
  for (let i = startIndex; i < indicesIndeterminados.length; i += BATCH_SIZE) {
    const inicioLote = Date.now(); // Inicio control de tiempo
    const chunkIndices = indicesIndeterminados.slice(i, i + BATCH_SIZE);
    const nombresParaIA = chunkIndices.map((idx) => todasLasFilas[idx].Name);

    statsIA.lotesProcesados++;
    const loteReal = START_BATCH + statsIA.lotesProcesados;
    process.stdout.write(`🧩 Lote ${loteReal}... `);

    const correcciones = await procesarBloqueOllama(nombresParaIA);

    if (Object.keys(correcciones).length === 0) {
      statsIA.fallidos += nombresParaIA.length;
      console.log('❌ Falló lote completo');
    } else {
      // Normalizamos las llaves del JSON para asegurar el emparejamiento
      const correccionesLimpias: Record<string, string> = {};
      Object.keys(correcciones).forEach((k) => {
        correccionesLimpias[k.trim().toUpperCase()] = correcciones[k];
      });

      chunkIndices.forEach((idx) => {
        const nombreOriginal = todasLasFilas[idx].Name.trim().toUpperCase();
        const corregido = correccionesLimpias[nombreOriginal];

        if (corregido) {
          todasLasFilas[idx].From = corregido;
          statsIA.corregidosExitosamente++;
        } else {
          statsIA.fallidos++;
        }
      });

      const finLote = Date.now();
      const tiempoLote = (finLote - inicioLote) / 1000;
      const avisoLento =
        tiempoLote > 25
          ? ` ⚠️ Lento (${tiempoLote.toFixed(1)}s)`
          : ` (${tiempoLote.toFixed(1)}s)`;
      console.log(`✅ Procesado${avisoLento}`);
    }

    // --- 💾 GUARDADO SIEMPRE (TRAS CADA LOTE) ---
    const headers = Object.keys(todasLasFilas[0]).map((key) => ({
      id: key,
      title: key,
    }));

    const datosOllama = chunkIndices
      .map((idx) => todasLasFilas[idx])
      .filter((row) => row.From !== 'Indeterminado');

    const datosIndeterminados = chunkIndices
      .map((idx) => todasLasFilas[idx])
      .filter((row) => row.From === 'Indeterminado');

    if (datosOllama.length > 0) {
      await createCsvWriter({
        path: outputPath,
        header: headers,
        append: true, // Append para no sobrescribir en cada lote
      }).writeRecords(datosOllama);
    }
    if (datosIndeterminados.length > 0) {
      await createCsvWriter({
        path: indeterminadosPath,
        header: headers,
        append: true, // Append para no sobrescribir en cada lote
      }).writeRecords(datosIndeterminados);
    }
  }

  // --- GENERACIÓN DEL REPORTE FINAL ---
  const fin = Date.now();
  const duracionS = ((fin - statsIA.inicio) / 1000).toFixed(2);

  const reporteLog = `
==================================================
🤖 REPORTE DE PROCESAMIENTO OLLAMA (IA)
==================================================
Fecha:                 ${new Date().toLocaleString()}
Modelo usado:          ${MODEL_NAME}
Tiempo total:          ${duracionS} segundos
--------------------------------------------------
Total Indeterminados:  ${statsIA.totalIndeterminados}
Corregidos por IA:     ${statsIA.corregidosExitosamente}
Fallidos/Sin cambios:  ${statsIA.fallidos}
Lotes procesados:      ${statsIA.lotesProcesados}
==================================================
✨ Archivos generados:
 - ${outputPath} (Exitosos)
 - ${indeterminadosPath} (Fallidos)
==================================================`;

  console.log(reporteLog);
  fs.writeFileSync(logPath, reporteLog, 'utf8');
};

// Ejecución
corregirIndeterminados().catch((err) =>
  console.error('🔴 Error crítico:', err),
);
