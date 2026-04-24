//cleaner.ts es el primero que ejecuto
import fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter } from 'csv-writer';

interface RawData {
  Name: string;
  Adress: string;
  Zip: string;
  Year: string;
  District?: string;
}

// const INPUT_FILE = './datos_mock.csv';

// const INPUT_FILE = '../csv/datosIniciales.csv';
const INPUT_FILE = '../csv/revere.csv';
const OUTPUT_FILE = '../csv/datosLimpios.csv';
const OUTPUT_FILE_ENTIDADES = '../csv/datosEntidades.csv';
const OUTPUT_FILE_OTRO_ZIP = '../csv/datosOtroZip.csv';
const BACKUP_DIR = '../backups/'; //si va ha evaluar una ciudad en especifico especificarla aqui, por ejemplo: '../backups/springfield/' y asegurarse de que exista la carpeta antes de ejecutar el script
const backupPath = `${BACKUP_DIR}backup_${Date.now()}_datosIniciales.csv`;
const LOG_FILE = '../logs/cleaner_logs.txt';

// Función para guardar logs con timestamp y mostrar en consola
function saveLog(message: string) {
  const timestamp = new Date().toLocaleString();
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(LOG_FILE, logMessage);
  console.log(message);
}

// funcion para determinar si un nombre es una entidad o ciudad, usando una lista negra de palabras clave
function isEntityOrCity(name: string): boolean {
  const nameLower = name.toLowerCase().trim();
  const blackList = [
    // Corporaciones y empresas
    'association',
    'inc',
    'incorporated',
    'corp',
    'corporation',
    'company',
    'co',
    'llc',
    'llp',
    'pllc',
    'pc',
    'plc',
    'partnership',
    'partners',
    'group',
    'enterprise',
    'ventures',
    'holdings',
    'management',
    'properties',
    'realty',
    'real estate',
    'trust',
    'foundation',

    // Instituciones públicas y gubernamentales
    'city of',
    'town of',
    'county',
    'council',
    'district',
    'school district',
    'commonwealth',
    'state of',
    'province',
    'department',
    'authority',
    'housing authority',
    'public works',
    'utilities',
    'agency',
    'bureau',
    'commission',
    'committee',
    'office',

    // Religiosas y comunitarias
    // 'church',
    // 'parish',
    // 'diocese',
    // 'temple',
    // 'congregation',
    // 'synagogue',
    // 'mosque',
    // 'chapel',
    // 'cathedral',

    // Educación y salud
    'academy',
    'college',
    'university',
    'school',
    'institute',
    'hospital',
    'clinic',
    'medical center',

    // Bancos y finanzas
    'bank',
    'credit union',
    'insurance',
    'financial',
    'mortgage',
    'investment',
    'capital',
    'securities',

    // Variantes internacionales
    'gmbh',
    'srl',
    'sa',
    'ag',
    'nv',
    'oy',
    'pte',
    'pty',
    'limited',
    'ltd',
    'unlimited',
    'bv',
    'aps',
    'ab',
  ];

  if (
    nameLower.includes('springfield') &&
    (nameLower.includes('city') || nameLower.includes('dept'))
  ) {
    return true;
  }

  return blackList.some((keyword) =>
    new RegExp(`\\b${keyword}\\b`, 'i').test(nameLower),
  );
}

// función para obtener la ciudad a partir del código postal, usando un mapeo predefinido
function getCityFromZip(zip: string): string | null {
  const cleanZip = (zip || '').trim().substring(0, 5);

  const mapping: { [key: string]: string } = {
    '01089': 'West Springfield',
    '01090': 'West Springfield (Feeding Hills)',
    '01101': 'Springfield (Main PO)',
    '01102': 'Springfield (Baystate Medical)',
    '01103': 'Springfield (Metro)',
    '01104': 'Springfield (Hungry Hill)',
    '01105': 'Springfield (South End)',
    '01106': 'Longmeadow',
    '01107': 'Springfield (North End)',
    '01108': 'Springfield (Forest Park)',
    '01109': 'Springfield (Pine Point)',
    '01111': 'Springfield (Business District)',
    '01115': 'Springfield (PO Boxes)',
    '01118': 'Springfield (East Forest Park)',
    '01119': 'Springfield (Six Corners)',
    '01128': 'Springfield (Sixteen Acres)',
    '01129': 'Springfield (Sixteen Acres)',
    '01138': 'Springfield (Custom)',
    '01139': 'Springfield (Custom)',
    '01144': 'Springfield (Main)',
    '01151': 'Indian Orchard',
    '01152': 'Springfield (Main)',
    '01199': 'Springfield (Baystate Medical Center)',
  };

  return mapping[cleanZip] || null;
}

function generarBackup() {
  try {
    // Asegurarse de que la carpeta de backups exista
    if (!fs.existsSync(BACKUP_DIR)) {
      fs.mkdirSync(BACKUP_DIR, { recursive: true });
    }

    // Copiar el archivo original al destino de backup
    fs.copyFileSync(INPUT_FILE, backupPath);

    saveLog(`🛡️ Backup creado con éxito: ${backupPath}`);
  } catch (error) {
    saveLog(
      `❌ Error crítico: No se pudo crear el backup. Deteniendo proceso.`,
    );
    process.exit(1); // Detenemos el script si el backup falla por seguridad
  }
}

async function filtrarDatos() {
  generarBackup();
  const resultados: RawData[] = [];
  const resultadosEntidades: RawData[] = []; // Para isEntityOrCity === true
  const resultadosOtroZip: RawData[] = []; // Para getCityFromZip
  const contadorPorLugar: { [key: string]: number } = {};
  const registrosExistentes = new Set<string>();

  let totalOriginal = 0;
  let totalFiltrado = 0;
  let descartadosPorZip = 0;
  let descartadosPorEntidad = 0;
  let duplicadosExactosOmitidos = 0;
  let descartadosPorFaltaName = 0;

  const fileExists = fs.existsSync(OUTPUT_FILE);

  // 1. CARGAR REGISTROS EXISTENTES
  if (fileExists) {
    await new Promise((resolve) => {
      fs.createReadStream(OUTPUT_FILE)
        .pipe(csv())
        .on('data', (row) => {
          const llave = `${row.Name}|${row.Adress}`.toLowerCase().trim();
          registrosExistentes.add(llave);
        })
        .on('end', resolve);
    });
  }

  // 2. PROCESAR ARCHIVO DE ENTRADA
  fs.createReadStream(INPUT_FILE)
    .pipe(csv())
    .on('data', (row: RawData) => {
      totalOriginal++;

      const tieneName = row.Name && row.Name.trim() !== '';
      const esEntidad = isEntityOrCity(row.Name || '');
      const localizacion = getCityFromZip(row.Zip || '');
      const llaveActual = `${row.Name}|${row.Adress}`.toLowerCase().trim();

      // Prioridad de filtrado
      if (!tieneName) {
        descartadosPorFaltaName++;
        return;
      }
      if (registrosExistentes.has(llaveActual)) {
        duplicadosExactosOmitidos++;
        return;
      }

      if (tieneName && esEntidad) {
        descartadosPorEntidad++;
        resultadosEntidades.push(row);
        return;
      }

      if (tieneName && !localizacion) {
        descartadosPorZip++;
        resultadosOtroZip.push(row);
        return;
      }

      if (tieneName && !esEntidad && localizacion) {
        row.District = localizacion;
        resultados.push(row);
        registrosExistentes.add(llaveActual);
        totalFiltrado++;
        contadorPorLugar[localizacion] =
          (contadorPorLugar[localizacion] || 0) + 1;
      }
    })
    .on('end', async () => {
      const header = [
        { id: 'Name', title: 'Name' },
        { id: 'Adress', title: 'Adress' },
        { id: 'District', title: 'District' },
        { id: 'Zip', title: 'Zip' },
        { id: 'Year', title: 'Year' },
      ];

      if (resultados.length > 0) {
        const csvWriter = createObjectCsvWriter({
          path: OUTPUT_FILE,
          header,
          append: fs.existsSync(OUTPUT_FILE),
        });
        await csvWriter.writeRecords(resultadosEntidades);
      }

      if (resultadosEntidades.length > 0) {
        const csvWriterEntidades = createObjectCsvWriter({
          path: OUTPUT_FILE_ENTIDADES,
          header,
          append: fs.existsSync(OUTPUT_FILE_ENTIDADES),
        });
        await csvWriterEntidades.writeRecords(resultadosEntidades);
        console.log(
          `✅ ${resultadosEntidades.length} entidades guardadas en: ${OUTPUT_FILE_ENTIDADES}`,
        );
      }

      // 3. Escritura de Registros Sin ZIP
      if (resultadosOtroZip.length > 0) {
        const csvWriterSinZip = createObjectCsvWriter({
          path: OUTPUT_FILE_OTRO_ZIP,
          header,
          append: fs.existsSync(OUTPUT_FILE_OTRO_ZIP),
        });
        await csvWriterSinZip.writeRecords(resultadosOtroZip);
        console.log(
          `✅ ${resultadosOtroZip.length} registros sin ZIP guardados en: ${OUTPUT_FILE_OTRO_ZIP}`,
        );
      }

      // --- CÁLCULO DE ESTADÍSTICAS ---

      // --- 📊 GENERAR INFORME DE EJECUCIÓN ---
      const porcentajeExito =
        totalOriginal > 0
          ? ((totalFiltrado / totalOriginal) * 100).toFixed(2)
          : '0.00';

      const totalEnBaseFinal = registrosExistentes.size;

      const reporteEjecucion = `
      ┌────────────────────────────────────────────────────────────┐
      │         🔄 PROCESO DE INTEGRACIÓN Y FILTRADO               │
      └────────────────────────────────────────────────────────────┘
        > Hora: ${new Date().toLocaleTimeString()}
        > Estado: ${totalFiltrado > 0 ? '✅ ACTUALIZADO' : 'ℹ️ SIN CAMBIOS'}

        ╔══════════════════════════════════════════════════════════╗
        ║                MÉTRICAS DE FLUJO DE DATOS                ║
        ╚══════════════════════════════════════════════════════════╝

        📦 ENTRADA BRUTA (CSV) : ${totalOriginal.toLocaleString().padEnd(10)}
        ✅ ÉXITO DE FILTRADO   : ${totalFiltrado.toLocaleString().padEnd(10)} [ ${porcentajeExito}% ]

        🚫 REGISTROS DESCARTADOS:
          - Sin dueño/nombre : ${descartadosPorFaltaName.toString().padEnd(6)} (Falta de datos)
          - Entidades/Empresas: ${descartadosPorEntidad.toString().padEnd(6)} (Filtro corporativo)
          - Fuera de zona     : ${descartadosPorZip.toString().padEnd(6)} (ZIP no admitido)
          - Duplicados        : ${duplicadosExactosOmitidos.toString().padEnd(6)} (Ya existentes)

        🗂️ ESTADO DE LA BASE FINAL:
          - Total acumulado   : ${totalEnBaseFinal.toLocaleString()} registros únicos

        ╔══════════════════════════════════════════════════════════╗
        ║               📌 DISTRIBUCIÓN POR DISTRITO               ║
        ╚══════════════════════════════════════════════════════════╝


      `;

      // renderizarTablaDistritos();
      saveLog(reporteEjecucion);

      // --- 🎨 CONFIGURACIÓN DE COLORES PARA CONSOLA ---
      const F = {
        reset: '\x1b[0m',
        bold: '\x1b[1m',
        cyan: '\x1b[36m',
        green: '\x1b[32m',
        yellow: '\x1b[33m',
        gray: '\x1b[90m',
        bgGray: '\x1b[100m',
      };

      const tablaOrdenada = Object.entries(contadorPorLugar)
        .map(([sector, total]) => ({
          Sector: sector,
          Agregados: total,
        }))
        .sort((a, b) => b.Agregados - a.Agregados);

      // --- 🛠️ CONSTRUCCIÓN DE LA TABLA ESTÉTICA ---
      console.log(
        `\n${F.bold}${F.cyan}╔════════════════════════════════════╤══════════╤════════════════════════╗${F.reset}`,
      );
      console.log(
        `${F.bold}${F.cyan}║         DISTRITO / SECTOR          │  TOTAL   │     RANKING VISUAL     ║${F.reset}`,
      );
      console.log(
        `${F.bold}${F.cyan}╟────────────────────────────────────┼──────────┼────────────────────────╢${F.reset}`,
      );

      tablaOrdenada.forEach(({ Sector, Agregados }) => {
        // Cálculo de barra de progreso (máximo 20 caracteres)
        const porcentaje = totalFiltrado > 0 ? Agregados / totalFiltrado : 0;
        const numBloques = Math.round(porcentaje * 100);
        const barra = `${F.green}${'█'.repeat(numBloques)}${F.gray}${'░'.repeat(20 - numBloques)}${F.reset}`;

        // Formateo de la fila
        const nombreSector = Sector.padEnd(34);
        const cantidad = Agregados.toString().padStart(6);

        console.log(
          `${F.bold}${F.cyan}║${F.reset} ${nombreSector} ${F.bold}${F.cyan}│${F.reset} ${F.yellow}${cantidad}${F.reset}   ${F.bold}${F.cyan}│${F.reset}  ${barra}  ${F.bold}${F.cyan}║${F.reset}`,
        );
      });

      console.log(
        `${F.bold}${F.cyan}╚════════════════════════════════════╧══════════╧════════════════════════╝${F.reset}\n`,
      );
    });
}

filtrarDatos();
