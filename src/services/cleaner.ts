//cleaner.ts es el primero que ejecuto
import fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter } from 'csv-writer';
import { procesarArchivo } from './from';
import { agregarLinks } from './link';

interface RawData {
  shape_perimeter: string;
  shape_area: string; //
  last_modified_date: string;
  building_value: string;
  land_value: string;
  other_improvements_value: string;
  total_assessed_value: string;
  fiscal_year: string; //
  lot_size: string; //
  last_sale_date: string; //
  last_sale_price: string; //
  land_use_code: string;
  property_site_address: string; //
  address_number: string;
  full_street_name: string;
  location_coordinates: string;
  city: string; //
  zip_code: string; //
  primary_owner_name: string; //
  owner_mailing_address: string;
  owner_city: string;
  owner_state: string;
  owner_zip_code: string;
  owner_country_company: string;
  zoning_district: string; //
  year_built: string; //
  gross_building_area: string; //
  total_units: string;
  residential_living_area: string; //
  architectural_style: string; //
  number_of_stories: string;
  total_number_of_rooms: string; //
  lot_size_units: string; //
  District?: string; //
}

// const INPUT_FILE = './datos_mock.csv';
// const INPUT_FILE = '../csv/datosIniciales.csv';

// Función para guardar logs con timestamp y mostrar en consola
function saveLog(message: string, LOG_FILE: string) {
  const timestamp = new Date().toLocaleString();
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(LOG_FILE, logMessage);
  console.log(message);
}

// funcion para determinar si un nombre es una entidad o ciudad, usando una lista negra de palabras clave
function isEntityOrCity(name: string): boolean {
  if (!name) return false;

  const CITY = (process.env.CITY || 'REVERE').toLowerCase();
  const nameLower = name.toLowerCase().trim();

  // 1. Detección rápida por reglas dinámicas (Ciudad/Dept)
  if (
    nameLower.includes(CITY) &&
    (nameLower.includes('city') || nameLower.includes('dept'))
  ) {
    return true;
  }

  // 2. Lista Negra Optimizada
  const blackList = [
    // Corporaciones e Inversión (Sufijos legales)
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
    'ltd',
    'limited',
    'unlimited',
    'gmbh',
    'srl',
    'sa',
    'ag',
    'nv',
    'oy',
    'pte',
    'pty',
    'bv',
    'aps',
    'ab',

    // Estructuras de Negocio y Real Estate
    'association',
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
    'trustee',
    'trustees',
    'foundation',
    'syndicate',
    'capital',
    'investment',
    'securities',
    'equity',
    'assets',

    // Instituciones Públicas y Gobierno (Muy importante para Massachusetts)
    'city of',
    'town of',
    'county',
    'council',
    'district',
    'school district',
    'commonwealth',
    'state of',
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
    'municipal',
    'federal',
    'board of',

    // Educación y Salud
    'academy',
    'college',
    'university',
    'school',
    'institute',
    'hospital',
    'clinic',
    'medical center',

    // Religiosas (Descomentadas si buscas solo dueños privados)
    // 'church',
    // 'parish',
    // 'diocese',
    // 'temple',
    // 'congregation',
    // 'synagogue',
    // 'mosque',
    // 'chapel',

    // Bancos y Finanzas
    'bank',
    'credit union',
    'insurance',
    'financial',
    'mortgage',
    'lending',
  ];

  // 3. Ejecución por Regex con Word Boundaries (\b)
  // Usamos una única Regex unificada para máxima velocidad en procesos masivos
  const pattern = new RegExp(`\\b(${blackList.join('|')})\\b`, 'i');

  return pattern.test(nameLower);
}

// función para obtener la ciudad a partir del código postal, usando un mapeo predefinido
function getCityFromZip(zip: string): string | null {
  const cleanZip = (zip || '').trim().substring(0, 5);

  const mapping: { [key: string]: string } = {
    // '01089': 'West Springfield',
    // '01090': 'West Springfield (Feeding Hills)',
    // '01101': 'Springfield (Main PO)',
    // '01102': 'Springfield (Baystate Medical)',
    // '01103': 'Springfield (Metro)',
    // '01104': 'Springfield (Hungry Hill)',
    // '01105': 'Springfield (South End)',
    // '01106': 'Longmeadow',
    // '01107': 'Springfield (North End)',
    // '01108': 'Springfield (Forest Park)',
    // '01109': 'Springfield (Pine Point)',
    // '01111': 'Springfield (Business District)',
    // '01115': 'Springfield (PO Boxes)',
    // '01118': 'Springfield (East Forest Park)',
    // '01119': 'Springfield (Six Corners)',
    // '01128': 'Springfield (Sixteen Acres)',
    // '01129': 'Springfield (Sixteen Acres)',
    // '01138': 'Springfield (Custom)',
    // '01139': 'Springfield (Custom)',
    // '01144': 'Springfield (Main)',
    // '01151': 'Indian Orchard',
    // '01152': 'Springfield (Main)',
    // '01199': 'Springfield (Baystate Medical Center)',
    '02151': 'Revere',
  };

  const city = mapping[cleanZip] || null;

  if (!city) {
    console.warn(`⚠️ ZIP no reconocido: "${cleanZip}" (Original: "${zip}")`);
  }

  return city || null;
}

export function generarBackup(
  INPUT_FILE: string,
  BACKUP_DIR: string,
  backupPath: string,
  LOG_FILE: string,
) {
  try {
    // Asegurarse de que la carpeta de backups exista
    if (!fs.existsSync(BACKUP_DIR)) {
      fs.mkdirSync(BACKUP_DIR, { recursive: true });
    }

    // Copiar el archivo original al destino de backup
    fs.copyFileSync(INPUT_FILE, backupPath);

    saveLog(`🛡️ Backup creado con éxito: ${backupPath}`, LOG_FILE);
  } catch (error) {
    saveLog(
      `❌ Error crítico: No se pudo crear el backup. Deteniendo proceso.${error}`,
      LOG_FILE,
    );
    process.exit(1); // Detenemos el script si el backup falla por seguridad
  }
}

export async function filtrarDatos(
  INPUT_FILE: string,
  // OUTPUT_FILE: string,
  OUTPUT_FILE_ENTIDADES: string,
  OUTPUT_FILE_OTRO_ZIP: string,
  OUTPUT_FILE_BALDIOS: string,
  OUTPUT_FILE_TRIPLEX: string,
  OUTPUT_FILE_DUPLEX: string,
  OUTPUT_FILE_SINGLE: string,
  LOG_FILE: string,
) {
  const CITY = process.env.CITY || 'default_city';

  const OUTPUT_FROM_ENTIDADES = `./csv/${CITY}/datosEntidades_From.csv`;
  const OUTPUT_FROM_OTRO_ZIP = `./csv/${CITY}/datosOtroZip_From.csv`;
  const OUTPUT_FROM_BALDIOS = `./csv/${CITY}/datosBaldios_From.csv`;
  const OUTPUT_FROM_TRIPLEX = `./csv/${CITY}/datosTriplex_From.csv`;
  const OUTPUT_FROM_DUPLEX = `./csv/${CITY}/datosDuplex_From.csv`;
  const OUTPUT_FROM_SINGLE = `./csv/${CITY}/datosSingle_From.csv`;
  const LOG_FILE_FROM = `./logs/${CITY}/cleaner_logs_From.txt`;

  const OUTPUT_LINK_ENTIDADES = `./csv/${CITY}/datosEntidades_From_Link.csv`;
  const OUTPUT_LINK_OTRO_ZIP = `./csv/${CITY}/datosOtroZip_From_Link.csv`;
  const OUTPUT_LINK_BALDIOS = `./csv/${CITY}/datosBaldios_From_Link.csv`;
  const OUTPUT_LINK_TRIPLEX = `./csv/${CITY}/datosTriplex_From_Link.csv`;
  const OUTPUT_LINK_DUPLEX = `./csv/${CITY}/datosDuplex_From_Link.csv`;
  const OUTPUT_LINK_SINGLE = `./csv/${CITY}/datosSingle_From_Link.csv`;

  const resultadosEntidades: RawData[] = []; // Para isEntityOrCity === true
  const resultadosOtroZip: RawData[] = []; // Para getCityFromZip
  const resultadoBaldios: RawData[] = [];
  const resultadoTriplex: RawData[] = [];
  const resultadoDuplex: RawData[] = [];
  const resultadoSingle: RawData[] = [];
  const contadorPorLugar: { [key: string]: number } = {};
  const registrosExistentes = new Set<string>();

  let totalOriginal = 0;
  let totalFiltrado = 0;
  let descartadosPorZip = 0;
  let descartadosPorEntidad = 0;
  let descartadosPorBaldio = 0;
  let duplicadosExactosOmitidos = 0;
  let descartadosPorFaltaName = 0;
  let triplexCount = 0;
  let duplexCount = 0;
  let singleCount = 0;
  // const fileExists = fs.existsSync(OUTPUT_FILE);

  // // 1. CARGAR REGISTROS EXISTENTES
  // if (fileExists) {
  //   await new Promise((resolve) => {
  //     fs.createReadStream(OUTPUT_FILE)
  //       .pipe(csv())
  //       .on('data', (row) => {
  //         const llave = `${row.Name}|${row.Adress}`.toLowerCase().trim();
  //         registrosExistentes.add(llave);
  //       })
  //       .on('end', resolve);
  //   });
  // }

  // 2. PROCESAR ARCHIVO DE ENTRADA
  fs.createReadStream(INPUT_FILE)
    .pipe(csv())
    .on('data', (row: RawData) => {
      totalOriginal++;

      const tieneDueno = row.primary_owner_name.trim();
      // const tieneName = row.Name && row.Name.trim() !== '';

      // --- 🏛️ FILTRO DE ENTIDADES (EXENTOS) ---

      const localizacion = getCityFromZip(row.zip_code || '');
      const llaveActual =
        `${row.primary_owner_name}|${row.property_site_address}`
          .toLowerCase()
          .trim();
      // --- 🚜 FILTROS DE EXCLUSIÓN (BALDÍOS Y ENTIDADES) ---

      // 1. BALDÍOS (Vacant Land)
      // Unificamos la lógica: Es baldío si el código está en el rango 130-132,
      // o si el valor de la edificación es 0, o si el código de uso es 106 (común para Land).
      const landCode = parseInt(row.land_use_code, 10);
      const buildingVal = parseInt(row.building_value, 10);

      const baldios =
        (landCode >= 130 && landCode <= 132) || // Códigos específicos de lotes vacíos
        landCode === 106 || // Código 106: Vacant Residential Land en MA
        row.lot_size_units === 'L' || // 'L' suele indicar Land-only
        buildingVal === 0 || // Si la construcción vale 0, es un lote
        row.architectural_style?.toUpperCase().includes('VACANT');

      // 2. ENTIDADES (City / State / Tax Exempt)
      // Generalmente, códigos > 900 son propiedades exentas (iglesias, escuelas, parques, edificios municipales).
      const esEntidad =
        landCode >= 900 || // Códigos 900+ son legalmente EXEMPT en MA
        isEntityOrCity(row.primary_owner_name || '') || // Validación por nombre del dueño
        row.zoning_district === 'OS' || // OS = Open Space (Parques)
        row.zoning_district === 'PS'; // PS = Public Service

      // --- 📐 FILTROS DE TIPOLOGÍA CON JERARQUÍA ---

      // Definimos flags de prioridad legal
      const esCodigoTriplex =
        row.land_use_code == '105' || row.total_units == '3';
      const esCodigoDuplex =
        row.land_use_code == '104' || row.total_units == '2';
      const esCodigoSingle =
        row.land_use_code == '101' || row.total_units == '1';

      // 1. TRIPLEX: Es triplex si el código lo dice, O si tiene 3 pisos PERO no dice ser duplex/single.
      const triplex =
        esCodigoTriplex ||
        (row.number_of_stories == '3' && !esCodigoDuplex && !esCodigoSingle) ||
        row.architectural_style?.toUpperCase().includes('3 FAMILY');

      // 2. DUPLEX: Es duplex si el código lo dice, O si el estilo arquitectónico lo confirma.
      const duplex =
        (esCodigoDuplex ||
          row.architectural_style?.toUpperCase().includes('2 FAMILY')) &&
        !triplex; // Evitamos doble clasificación

      // 3. SINGLE: Es single si el código lo dice, O si es un estilo clásico de una familia.
      const single =
        (esCodigoSingle ||
          ['RANCH', 'CONVETL', 'CAPE', 'COLONIAL'].includes(
            row.architectural_style?.toUpperCase(),
          )) &&
        !duplex &&
        !triplex;

      // Prioridad de filtrado
      if (!tieneDueno) {
        descartadosPorFaltaName++;
        return;
      }
      if (registrosExistentes.has(llaveActual)) {
        duplicadosExactosOmitidos++;
        return;
      }
      if (tieneDueno) {
        if (esEntidad) {
          descartadosPorEntidad++;
          resultadosEntidades.push(row);
          return;
        }
        if (!localizacion) {
          descartadosPorZip++;
          resultadosOtroZip.push(row);
          return;
        }
        // if (localizacion) {
        //   row.District = localizacion;
        //   resultados.push(row);
        //   registrosExistentes.add(llaveActual);
        //   totalFiltrado++;
        //   contadorPorLugar[localizacion] =
        //     (contadorPorLugar[localizacion] || 0) + 1;
        // }
        row.District = localizacion;
        registrosExistentes.add(llaveActual);
        contadorPorLugar[localizacion] =
          (contadorPorLugar[localizacion] || 0) + 1;

        if (baldios) {
          descartadosPorBaldio++;
          resultadoBaldios.push(row);
          return;
        }
        if (triplex) {
          triplexCount++;
          totalFiltrado++;
          resultadoTriplex.push(row);
          return;
        }
        if (duplex) {
          duplexCount++;
          totalFiltrado++;
          resultadoDuplex.push(row);
          return;
        }
        if (single) {
          singleCount++;
          totalFiltrado++;
          resultadoSingle.push(row);
          return;
        }
      }
      if (row.zip_code == row.owner_zip_code && row.owner_city == row.city) {
        const ubi = row.property_site_address;
      }
    })
    .on('end', async () => {
      const header = [
        { id: 'primary_owner_name', title: 'Owner' },
        { id: 'property_site_address', title: 'Address' },
        { id: 'District', title: 'District' },
        { id: 'zip_code', title: 'Zip' },
        { id: 'lot_size', title: 'Lot_Size' },
        { id: 'residential_living_area', title: 'Living_Area' },
        { id: 'year_built', title: 'Year Built' },
        { id: 'last_sale_price', title: 'Last_Sale_Price' },
        { id: 'last_sale_date', title: 'Last_Sale_Date' },
        { id: 'fiscal_year', title: 'Fiscal_Year' },
        { id: 'zoning_district', title: 'Zoning_District' },
        { id: 'architectural_style', title: 'Architectural_Style' },
        { id: 'total_units', title: 'Total_Units' },
        { id: 'land_use_code', title: 'Use_Code' },
        { id: 'number_of_stories', title: 'Number_of_Stories' },
      ];

      /**
       * FUNCIÓN INTERNA DE PROCESAMIENTO
       * Encapsula la escritura, clasificación y generación de links por categoría
       */
      const ejecutarFase = async (
        nombre: string,
        datos: any[],
        pathBase: string,
        pathFrom: string,
        pathLink: string,
      ) => {
        if (datos.length === 0) {
          console.log(`ℹ️ Saltando ${nombre}: Sin registros.`);
          return;
        }

        try {
          // 1. Escribir el archivo base (Sobrescribir para evitar mezclas de ejecuciones previas)
          const csvWriter = createObjectCsvWriter({
            path: pathBase,
            header,
            append: false,
          });

          await csvWriter.writeRecords(datos);
          console.log(
            `\n✅ [${nombre.toUpperCase()}] Base guardada: ${datos.length} registros.`,
          );

          // 2. Ejecutar clasificación (Hispano/USA/Indeterminado)
          // Pasamos un log específico por fase para evitar conflictos de escritura
          const logEspecifico = `./logs/revere/log_cleaner_${nombre.toLowerCase()}.txt`;
          await procesarArchivo(pathBase, pathFrom, logEspecifico);

          // 3. Agregar enlaces de búsqueda
          await agregarLinks(pathFrom, pathLink);

          console.log(
            `✨ [${nombre.toUpperCase()}] Fase completada exitosamente.`,
          );
        } catch (error) {
          console.error(`❌ Error procesando la categoría ${nombre}:`, error);
        }
      };

      // --- EJECUCIÓN SECUENCIAL ---
      // El uso de await aquí garantiza que NO se pisen los procesos en memoria

      await ejecutarFase(
        'Entidades',
        resultadosEntidades,
        OUTPUT_FILE_ENTIDADES,
        OUTPUT_FROM_ENTIDADES,
        OUTPUT_LINK_ENTIDADES,
      );

      await ejecutarFase(
        'Sin_Zip',
        resultadosOtroZip,
        OUTPUT_FILE_OTRO_ZIP,
        OUTPUT_FROM_OTRO_ZIP,
        OUTPUT_LINK_OTRO_ZIP,
      );

      await ejecutarFase(
        'Baldios',
        resultadoBaldios,
        OUTPUT_FILE_BALDIOS,
        OUTPUT_FROM_BALDIOS,
        OUTPUT_LINK_BALDIOS,
      );

      await ejecutarFase(
        'Triplex',
        resultadoTriplex,
        OUTPUT_FILE_TRIPLEX,
        OUTPUT_FROM_TRIPLEX,
        OUTPUT_LINK_TRIPLEX,
      );

      await ejecutarFase(
        'Duplex',
        resultadoDuplex,
        OUTPUT_FILE_DUPLEX,
        OUTPUT_FROM_DUPLEX,
        OUTPUT_LINK_DUPLEX,
      );

      await ejecutarFase(
        'Single',
        resultadoSingle,
        OUTPUT_FILE_SINGLE,
        OUTPUT_FROM_SINGLE,
        OUTPUT_LINK_SINGLE,
      );

      console.log('\n🏁 --- PROCESO COMPLETO FINALIZADO --- 🏁');
    });
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

        🟢 REGISTROS GUARDADOS:
          - Triplex           : ${triplexCount.toString().padEnd(6)}
          - Duplex            : ${duplexCount.toString().padEnd(6)}
          - Single           : ${singleCount.toString().padEnd(6)}
        🗂️ ESTADO DE LA BASE FINAL:
          - Total acumulado   : ${totalEnBaseFinal.toLocaleString()} registros únicos

        ╔══════════════════════════════════════════════════════════╗
        ║               📌 DISTRIBUCIÓN POR DISTRITO               ║
        ╚══════════════════════════════════════════════════════════╝


      `;

  // renderizarTablaDistritos();
  saveLog(reporteEjecucion, LOG_FILE);

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
}
