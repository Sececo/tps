//cleaner.ts es el primero que ejecuto
import fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter } from 'csv-writer';

interface RawData {
  dueno: string;
  direccion_inmueble: string;
  direccion_dueno: string;
  zip_dueno: string;
  anio_construccion: string;
  ciudad?: string;
}

// const INPUT_FILE = './datos_mock.csv';

const INPUT_FILE = '../csv/datosIniciales.csv';
const OUTPUT_FILE = '../csv/datosLimpios.csv';
const LOG_FILE = '../logs/cleaner_logs.txt';

// Función para guardar logs con timestamp y mostrar en consola
function saveLog(message: string) {
  const timestamp = new Date().toLocaleString();
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(LOG_FILE, logMessage);
  console.log(message);
}

function isEntityOrCity(name: string): boolean {
  const nameLower = name.toLowerCase().trim();
  const blackList = [
    // Corporaciones y empresas
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

async function filtrarDatos() {
  const resultados: RawData[] = [];
  const contadorPorLugar: { [key: string]: number } = {};
  const registrosExistentes = new Set<string>();

  let totalOriginal = 0;
  let totalFiltrado = 0;
  let descartadosPorZip = 0;
  let descartadosPorEntidad = 0;
  let duplicadosExactosOmitidos = 0;
  let descartadosPorFaltaDueno = 0;

  const fileExists = fs.existsSync(OUTPUT_FILE);

  // 1. CARGAR REGISTROS EXISTENTES
  if (fileExists) {
    await new Promise((resolve) => {
      fs.createReadStream(OUTPUT_FILE)
        .pipe(csv())
        .on('data', (row) => {
          const llave = `${row.dueno}|${row.direccion_inmueble}`
            .toLowerCase()
            .trim();
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

      const tieneDueno = row.dueno && row.dueno.trim() !== '';
      const esEntidad = isEntityOrCity(row.dueno || '');
      const localizacion = getCityFromZip(row.zip_dueno || '');
      const llaveActual = `${row.dueno}|${row.direccion_inmueble}`
        .toLowerCase()
        .trim();

      // Prioridad de filtrado
      if (!tieneDueno) {
        descartadosPorFaltaDueno++;
        return;
      } else {
        if (registrosExistentes.has(llaveActual)) {
          duplicadosExactosOmitidos++;
          return;
        }

        if (tieneDueno && esEntidad) {
          descartadosPorEntidad++;
          return;
        }

        if (tieneDueno && !localizacion) {
          descartadosPorZip++;
          return;
        }

        if (tieneDueno && !esEntidad && localizacion) {
          row.ciudad = localizacion;
          resultados.push(row);
          registrosExistentes.add(llaveActual);
          totalFiltrado++;
          contadorPorLugar[localizacion] =
            (contadorPorLugar[localizacion] || 0) + 1;
        }
      }
    })
    .on('end', async () => {
      const csvWriter = createObjectCsvWriter({
        path: OUTPUT_FILE,
        header: [
          { id: 'dueno', title: 'Name' },
          { id: 'direccion_inmueble', title: 'Adress' },
          { id: 'ciudad', title: 'District' },
          { id: 'zip_dueno', title: 'Zip' },
          { id: 'anio_construccion', title: 'Build_year' },
        ],
        append: fileExists,
      });

      if (resultados.length > 0) {
        await csvWriter.writeRecords(resultados);
      }

      // --- CÁLCULO DE ESTADÍSTICAS ---
      const porcentajeExito =
        totalOriginal > 0
          ? ((totalFiltrado / totalOriginal) * 100).toFixed(2)
          : 0;
      const totalEnBaseFinal = registrosExistentes.size;

      const infoResumen = [
        `\n--- 📊 INFORME DE EJECUCIÓN (${new Date().toLocaleTimeString()}) ---`,
        `📦 Total registros leídos:      ${totalOriginal}`,
        `✅ Nuevos agregados:           ${totalFiltrado} (${porcentajeExito}%)`,
        `⚠️ Descartados por falta de dueño: ${descartadosPorFaltaDueno}`,
        `👯 Duplicados exactos:         ${duplicadosExactosOmitidos}`,
        `🏢 Entidades/Empresas:         ${descartadosPorEntidad}`,
        `📍 Fuera de zona (ZIP):        ${descartadosPorZip}`,
        `🗂️ Total acumulado en base:    ${totalEnBaseFinal}`,
        `--------------------------------------------------`,
      ].join('\n');

      // IMPRESIÓN DUAL
      console.log(infoResumen);
      fs.appendFileSync(LOG_FILE, infoResumen + '\n');

      if (totalFiltrado > 0) {
        const tablaOrdenada = Object.entries(contadorPorLugar)
          .map(([sector, total]) => ({
            Sector: sector,
            Agregados: total,
          }))
          .sort((a, b) => b.Agregados - a.Agregados);

        console.table(tablaOrdenada);
      } else {
        console.log('⚠️ No se añadieron registros nuevos en esta vuelta.');
      }
    });
}

filtrarDatos();
