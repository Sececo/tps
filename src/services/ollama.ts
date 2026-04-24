// ollama.services.ts es el cuarto que ejecuto, después de cleaner.ts, from.ts y notIndeterminado.ts
import * as fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';
import dotenv from 'dotenv';

dotenv.config();

/**
 * CONFIGURACIÓN DEL SERVICIO
 */
const OLLAMA_API_URL = 'http://localhost:11434/api/generate';
const MODEL_NAME = process.env.MODEL_NAME; // Ajustado a tu versión estable
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
  // // Ajuste de contexto: Se exige explícitamente no usar markdown para no saturar el procesamiento
  // // Mejora: Usamos salto de línea para que la IA distinga mejor los nombres compuestos
  // const prompt = `Clasifica estos nombres como 'Hispano' o 'Estadounidense'.
  // Responde ÚNICAMENTE con un objeto JSON. Mantén el nombre exacto como clave.
  // Nombres a procesar:
  // ${nombres.join('\n')}

  const prompt = `${nombres.join('\n')}`;

  // Formato requerido: {"Nombre Completo": "Clasificación"}`;

  try {
    const response = await fetch(OLLAMA_API_URL, {
      method: 'POST',
      body: JSON.stringify({
        model: MODEL_NAME,
        prompt: prompt,
        //format: 'json',
        stream: false,
        // options: {
        //   temperature: 0, // Bajamos la temperatura para que sea más determinista
        //   num_thread: 8,
        //   num_predict: 500, // Limita la generación para no saturar memoria/tiempo
        //   num_ctx: 1024, // Limita la ventana de contexto a lo estrictamente necesario
        // },
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
  const inputPath = 'src/csv/datosIndeterminados.csv';
  const outputPath = 'src/csv/datosOllama.csv'; // Archivo para los exitosos
  const indeterminadosPath = 'src/csv/datosIndeterminadosOllama.csv'; // Archivo para los fallidos
  const logPath = 'src/logs/ollama_stats.txt';
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

  const hr = '━'.repeat(50);
  const dot = '•';

  console.log(`\n${hr}`);
  console.log(`  🚀 SISTEMA DE PROCESAMIENTO IA`);
  console.log(`${hr}`);
  console.log(`${dot} Estatus: Cargando datos en memoria...`);

  const readStream = fs.createReadStream(inputPath).pipe(csv());
  for await (const row of readStream) {
    todasLasFilas.push(row);
  }

  // Identificamos las filas que Ollama debe procesar
  const indicesIndeterminados = todasLasFilas
    .map((row, index) => {
      // row.From será "" en tu ejemplo (porque hay una coma al final pero nada escrito)
      const valor = row.From ? undefined : ''; // Aseguramos que undefined también se trate como indeterminado

      // Seleccionamos si está vacío o si es la palabra literal "Indeterminado"
      return valor === '' || valor === 'Indeterminado' ? index : -1;
    })
    .filter((index) => index !== -1);

  statsIA.totalIndeterminados = indicesIndeterminados.length;

  if (statsIA.totalIndeterminados === 0) {
    console.log(
      `${dot} Resultado: ✅ No se encontraron nombres 'Indeterminados'.`,
    );
    console.log(`${hr}\n`);
    return;
  }

  console.log(
    `${dot} Análisis: Encontrados ${statsIA.totalIndeterminados} elementos.`,
  );
  console.log(`${dot} Config: Lotes de ${BATCH_SIZE} unidades.`);
  console.log(`${hr}`);
  console.log(`  📥 PROCESANDO LOTE DE TRABAJO...`);
  console.log(`${hr}\n`);

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
      .filter((row) => row.From !== '' || undefined); // Consideramos corregidos aquellos que ya no están vacíos

    const datosIndeterminados = chunkIndices
      .map((idx) => todasLasFilas[idx])
      .filter((row) => row.From === '' || undefined);

    statsIA.lotesProcesados += 1;
    statsIA.corregidosExitosamente += datosOllama.length;

    const timestamp = new Date().toLocaleTimeString();
    const reporteLog = `[${timestamp}] Lote #${statsIA.lotesProcesados} | Éxitos: ${datosOllama.length} | Indeterminados: ${datosIndeterminados.length}\n`;
    // statsIA.fallidos se actualizaría en tu bloque `catch` si lo tienes

    // --- 📝 LOG VISUAL DEL LOTE ---
    console.log(`  ${reporteLog.trim()}`);
    fs.appendFileSync(logPath, reporteLog, 'utf8');

    if (datosOllama.length > 0) {
      console.log(
        `  │  ├─ ✅ Guardando ${datosOllama.length} registros exitosos...`,
      );
      await createCsvWriter({
        path: outputPath,
        header: headers,
        append: true, // Append para no sobrescribir en cada lote
      }).writeRecords(datosOllama);
    }
    if (datosIndeterminados.length > 0) {
      console.log(
        `  │  └─ ⚠️  Guardando ${datosIndeterminados.length} indeterminados...`,
      );
      await createCsvWriter({
        path: indeterminadosPath,
        header: headers,
        append: true, // Append para no sobrescribir en cada lote
      }).writeRecords(datosIndeterminados);
    } else {
      console.log(`  │  └─ ⚠️  0 indeterminados en este lote.`);
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
