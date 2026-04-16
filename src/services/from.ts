//from.ts es el segundo que ejecuto, después de cleaner.ts
import * as fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';

// --- CONFIGURACIÓN DE PATRONES (INTACTO) ---
const DATA_HISPANA = new Set([
  // Tus originales (Top Global + Colombia)
  'GARCIA',
  'RODRIGUEZ',
  'GONZALEZ',
  'FERNANDEZ',
  'LOPEZ',
  'MARTINEZ',
  'SANCHEZ',
  'PEREZ',
  'GOMEZ',
  'HERNANDEZ',
  'JIMENEZ',
  'DIAZ',
  'MORENO',
  'MUÑOZ',
  'ALVAREZ',
  'ROMERO',
  'ALONSO',
  'GUTIERREZ',
  'NAVARRO',
  'TORRES',
  'DOMINGUEZ',
  'RAMOS',
  'VAZQUEZ',
  'RAMIREZ',
  'GIL',
  'SERRANO',
  'BLANCO',
  'MOLINA',
  'MORALES',
  'SUAREZ',
  'CEBALLOS',
  'CORREA',
  'CAICEDO',
  'OSORIO',
  'CASTAÑO',
  'HOLGUIN',
  'OROZCO',
  'RESTREPO',
  'POSADA',
  'GIRALDO',
  'BEDOYA',
  'MONTOYA',
  'ZULUAGA',
  'QUINTERO',
  'PATIÑO',
  'MURILLO',
  'MOSQUERA',
  'VALENCIA',
  'HURTADO',
  'PORTOCARRERO',
  // Expansión: Más comunes en Latinoamérica
  'VELASQUEZ',
  'RIVERA',
  'ORTEGA',
  'CASTILLO',
  'MENDOZA',
  'ROBLES',
  'PACHECO',
  'CANO',
  'VALDEZ',
  'GUZMAN',
  'BARRIOS',
  'VARGAS',
  'VEGA',
  'DELGADO',
  'PEÑA',
  'AGUILAR',
  'SOTO',
  'FIGUEROA',
  'REYES',
  'HINOJOSA',
  'IBARRA',
  'VILLALOBOS',
  'ARIAS',
  'MONTERO',
  'CARDONA',
  'DUQUE',
  'ESPINOSA',
  'MENDEZ',
  'PAEZ',
  'SOLER',
  'ZAMBRANO',
  'MERCADO',
  'CABALLERO',
  'SALAZAR',
  'CABRERA',
  'VICTORIA',
  'PINTO',
  'PARRA',
  'MENDEZ',
  'CALDERON',
  'GALLARDO',
  'ESCOBAR',
  'SANTANA',
  'AGUIRRE',
  'FUENTES',
  'MEDINA',
  'CORTES',
  'CABRERA',
  'CASTELLANOS',
  'ESTRADA',
  // Expansión: Refuerzo Valle del Cauca / Colombia
  'ARBOLEDA',
  'BOLAÑOS',
  'CABAL',
  'CAMPAZ',
  'CISTERNA',
  'CUERO',
  'ESTUPIÑAN',
  'GARCÉS',
  'LUCUMÍ',
  'MINA',
  'OBREGON',
  'PALACIOS',
  'RIASCOS',
  'SINESTERRA',
  'TENORIO',
  'VIVEROS',
  'ZAPATA',
  'ANGULO',
  'URRUTIA',
  'CARABALI',
  'ECHEVERRY',
  'LONDOÑO',
  'CASTAÑEDA',
  'HINCAPIE',
  'VANEGAS',
  'BORJA',
  'JARAMILLO',
  'PABON',
  'TABARES',
  'URIBE',
]);
const DATA_US = new Set([
  // Tus originales (Nombres y Apellidos)
  'HENRY',
  'JOHNSON',
  'JILL',
  'RAYMOND',
  'PLUMMER',
  'DEVERON',
  'STASIAK',
  'PAUL',
  'DARRELL',
  'LUZHANSKIY',
  'VICTOR',
  'SERGEY',
  'MENDES',
  'ELIZABETH',
  'GEORGE',
  'TIMOTHY',
  'SMITH',
  'WILLIAMS',
  'BROWN',
  'JONES',
  'MILLER',
  'DAVIS',
  'WILSON',
  'ANDERSON',
  'TAYLOR',
  'THOMAS',
  'MOORE',
  'JACKSON',
  'MARTIN',
  'LEE',
  'THOMPSON',
  'WHITE',
  'HARRIS',
  'CLARK',
  'LEWIS',
  'ROBINSON',
  'WALKER',
  'YOUNG',
  'ALLEN',
  'KING',
  'WRIGHT',
  'SCOTT',
  'HILL',
  'GREEN',
  'ADAMS',
  'NELSON',
  'BAKER',
  'HALL',
  'CAMPBELL',
  'MITCHELL',
  'CARTER',
  'ROBERTS',
  'KIRBY',
  'BRADLEY',
  'VOORHIS',
  'STEVENS',
  'MURPHY',
  'COOK',
  'ROGERS',
  'MORGAN',
  'COOPER',
  'PETERSON',
  'REED',
  'BAILEY',
  'BELL',
  'KELLY',
  'HOWARD',
  'WARD',
  'COX',
  'RICHARDSON',
  'WATSON',
  'BROOKS',
  'CHAVEZ',
  'WOOD',
  'JAMES',
  'BENNETT',
  'GRAY',
  'MEYER',
  'HAMILTON',
  'FISHER',
  'SULLIVAN',
  'WASHINGTON',
  'COLEMAN',
  'BUTLER',
  'SIMMONS',
  'FOSTER',
  'BRYANT',
  'ALEXANDER',
  'RUSSELL',
  'GRIFFIN',
  // Expansión: Apellidos Anglo/Euro-Americanos
  'STEWART',
  'MORRIS',
  'NGUYEN',
  'MURPHY',
  'RIVERA',
  'COOK',
  'ROGERS',
  'PETERS',
  'PERRY',
  'POWELL',
  'LONG',
  'PATTERSON',
  'HUGHES',
  'FLORES',
  'WASHINGTON',
  'BUTLER',
  'SIMMONS',
  'FOSTER',
  'GONZALES',
  'BRYANT',
  'ALEXANDER',
  'RUSSELL',
  'GRIFFIN',
  'DIAZ',
  'HAYES',
  'MYERS',
  'FORD',
  'HAMILTON',
  'GRAHAM',
  'SULLIVAN',
  'WALLACE',
  'COLE',
  'WEST',
  'JORDAN',
  'OWENS',
  'REYNOLDS',
  'FISHER',
  'ELLIS',
  'HARRISON',
  'GIBSON',
  'MCDONALD',
  'CRUZ',
  'MARSHALL',
  'ORTIZ',
  'GOMEZ',
  'MURRAY',
  'FREEMAN',
  'WELLS',
  'WEBB',
  'SIMPSON',
  'STEVENS',
  'TUCKER',
  'PORTER',
  'HUNTER',
  'HICKS',
  'CRAWFORD',
  'BOYD',
  'MASON',
  'MORRISON',
  'KENNEDY',
]);

const SUFIJOS_HISPANOS = [
  // Fuertes (Casi siempre hispanos)
  'EZ',
  'IZ',
  'AZ',
  'OZ',
  'ILLA',
  'ILLO',
  'EÑO',
  'EÑA',
  'ERO',
  'ERA',
  'ARRI',
  'OYA',
  'GUI',
  // Débiles (Necesitan validación)
  'AS',
  'OS',
  'ON',
  'ES',
  'NO',
  'IA',
  'DO',
  'RO',
  'LO',
  'RA',
  'NA',
  'SA',
  'TA',
  'ZA',
  'CA',
  'MA',
];
const SUFIJOS_US = [
  // Fuertes (Anglo/Germánicos)
  'SON',
  'FORD',
  'TON',
  'STER',
  'WOOD',
  'BURY',
  'FIELD',
  'HAM',
  'SHIRE',
  'WORTH',
  'LAND',
  'THORPE',
  // Otros
  'CK',
  'LY',
  'MAN',
  'TH',
  'EY',
  'LEY',
  'HIS',
  'RDS',
  'IY',
  'OV',
  'SKIY',
  'AK',
  'IK',
  'OFF',
  'ITZ',
  'BERG',
  'STEIN',
];

/**
 * Lógica de clasificación por pesos (INTACTO)
 */
const clasificarNombre = (nombreCompleto: string | undefined): string => {
  if (!nombreCompleto) return 'Indeterminado';

  const normalizado = nombreCompleto.toUpperCase().trim();
  let scoreHispano = 0;
  let scoreUS = 0;

  // 1. Detección de caracteres latinos
  if (/[ÑÁÉÍÓÚ]/.test(normalizado)) scoreHispano += 15;

  const partes = normalizado.split(/\s+/);

  partes.forEach((parte) => {
    if (DATA_HISPANA.has(parte)) scoreHispano += 10;
    if (DATA_US.has(parte)) scoreUS += 10;

    if (parte.length > 4) {
      if (SUFIJOS_HISPANOS.some((suf) => parte.endsWith(suf)))
        scoreHispano += 4;
      if (SUFIJOS_US.some((suf) => parte.endsWith(suf))) scoreUS += 4;
    }
  });

  if (scoreHispano > scoreUS) return 'Hispano';
  if (scoreUS > scoreHispano) return 'Estadounidense';
  return 'Indeterminado';
};

// --- PROCESAMIENTO CON ESTADÍSTICAS AÑADIDAS ---

const procesarArchivo = async () => {
  const inputPath = '../csv/datosLimpios.csv';
  const outputPath = '../csv/datosFrom.csv';
  const logPath = '../logs/stats_From.txt'; // <-- Archivo donde guardaremos el log
  const filasProcesadas: any[] = [];

  if (!fs.existsSync(inputPath)) {
    console.error(`Archivo ${inputPath} no encontrado.`);
    return;
  }

  // --- 📊 TRACKER DE ESTADÍSTICAS ---
  const stats = {
    inicioTimer: Date.now(),
    totalProcesados: 0,
    nulosOVacios: 0,
    conTildesONies: 0,
    clasificacion: {
      hispano: 0,
      estadounidense: 0,
      indeterminado: 0,
    },
  };

  // Leemos el archivo para capturar los headers dinámicamente
  fs.createReadStream(inputPath)
    .pipe(csv())
    .on('data', (row) => {
      // TRACKING: Contar totales
      stats.totalProcesados++;

      // TRACKING: Contar vacíos
      if (!row.Name || row.Name.trim() === '') {
        stats.nulosOVacios++;
      } else if (/[ÑÁÉÍÓÚ]/i.test(row.Name)) {
        // TRACKING: Contar caracteres latinos
        stats.conTildesONies++;
      }

      // Clasificamos usando la columna "Name"
      const origen = clasificarNombre(row.Name);

      // TRACKING: Contar resultados
      if (origen === 'Hispano') stats.clasificacion.hispano++;
      else if (origen === 'Estadounidense')
        stats.clasificacion.estadounidense++;
      else stats.clasificacion.indeterminado++;

      // Retornamos todo el objeto original + la nueva columna
      filasProcesadas.push({
        ...row,
        From: origen,
      });
    })
    .on('end', async () => {
      if (filasProcesadas.length === 0) {
        console.log('El CSV está vacío.');
        return;
      }

      // Configuramos el Writer con los headers dinámicos
      const headers = Object.keys(filasProcesadas[0]).map((key) => ({
        id: key,
        title: key,
      }));

      const csvWriter = createCsvWriter({
        path: outputPath,
        header: headers,
      });

      await csvWriter.writeRecords(filasProcesadas);

      // --- 📈 GENERAR REPORTE DE ESTADÍSTICAS ---
      const finTimer = Date.now();
      const duracionSegundos = ((finTimer - stats.inicioTimer) / 1000).toFixed(
        2,
      );

      const calcularPorcentaje = (cantidad: number) =>
        stats.totalProcesados > 0
          ? ((cantidad / stats.totalProcesados) * 100).toFixed(2)
          : '0.00';

      const reporteLog = `
==================================================
📊 REPORTE DE CLASIFICACIÓN DE NOMBRES
==================================================
Fecha de ejecución: ${new Date().toLocaleString()}
Tiempo de procesamiento: ${duracionSegundos} segundos

--- 1. DATOS GENERALES ---
Total de filas escaneadas:  ${stats.totalProcesados}
Nombres vacíos o nulos:     ${stats.nulosOVacios} (${calcularPorcentaje(stats.nulosOVacios)}%)
Nombres con tildes o Ñ:     ${stats.conTildesONies} (${calcularPorcentaje(stats.conTildesONies)}%)

--- 2. DISTRIBUCIÓN DE CLASIFICACIÓN ---
Hispano:          ${stats.clasificacion.hispano} (${calcularPorcentaje(stats.clasificacion.hispano)}%)
Estadounidense:   ${stats.clasificacion.estadounidense} (${calcularPorcentaje(stats.clasificacion.estadounidense)}%)
Indeterminado:    ${stats.clasificacion.indeterminado} (${calcularPorcentaje(stats.clasificacion.indeterminado)}%)

==================================================
✅ Archivo CSV generado en: ${outputPath}
==================================================`;

      // 1. Imprimimos en consola (console log)
      console.log(reporteLog);

      // 2. Guardamos en el archivo (save log)
      fs.writeFileSync(logPath, reporteLog, 'utf8');
      console.log(`📝 Log de estadísticas guardado en: ${logPath}`);
    });
};

procesarArchivo();
