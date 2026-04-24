//from.ts es el segundo que ejecuto, después de cleaner.ts
import * as fs from 'fs';
import csv from 'csv-parser';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';

// --- CONFIGURACIÓN DE PATRONES (INTACTO) ---
const DATA_HISPANA = new Set([
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
  'SEBASTIÁN',
  'ALEJANDRO',
  'MIGUEL',
  'ÁNGEL',
  'JUAN',
  'JOSÉ',
  'BARTOLOMÉ',
  'ÍÑIGO',
  'JERÓNIMO',
  'JOAQUÍN',
  'AGUSTÍN',
  'FROILÁN',
  'ESTEBAN',
  'GONZALO',
  'SANTIAGO',
  'MATÍAS',
  'NICOLÁS',
  'VALENTÍN',
  'DAMIÁN',
  'ADRIÁN',
  'ÁLVARO',
  'ÍKER',
  'RAÚL',
  'SAÚL',
  'RENÉ',
  'IGNACIO',
  'FRANCISCO',
  'FERNANDO',
  'RODRIGO',
  'VICENTE',
  'JAVIER',
  'MAURICIO',
  'GABRIEL',
  'ANDRÉS',
  'RAMÓN',
  'FELIPE',
  'MARÍA',
  'LUCÍA',
  'XIMENA',
  'BEATRIZ',
  'VERÓNICA',
  'SOFÍA',
  'PIEDAD',
  'AMPARO',
  'ROCÍO',
  'CONCEPCIÓN',
  'ENCARNACIÓN',
  'JIMENA',
  'YOLANDA',
  'LETICIA',
  'GUADALUPE',
  'ROSARIO',
  'MERCEDES',
  'DOLORES',
  'CARMEN',
  'TERESA',
  'MARTHA',
  'REBECA',
  'ADRIANA',
  'ESPERANZA',
  'ASCENSIÓN',
  'ASUNCIÓN',
  'MILAGROS',
  'ESTEFANÍA',
  'ALMUDENA',
  'ARACELI',
  'CECILIA',
  'GRACIELA',
  'INÉS',
]);
const DATA_US = new Set([
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
  'STEWART',
  'MORRIS',
  'NGUYEN',
  'MURPHY',
  'PETERS',
  'PERRY',
  'POWELL',
  'LONG',
  'PATTERSON',
  'HUGHES',
  'HAYES',
  'MYERS',
  'FORD',
  'GRAHAM',
  'WALLACE',
  'COLE',
  'WEST',
  'JORDAN',
  'OWENS',
  'REYNOLDS',
  'ELLIS',
  'HARRISON',
  'GIBSON',
  'MCDONALD',
  'MARSHALL',
  'MURRAY',
  'FREEMAN',
  'WELLS',
  'WEBB',
  'SIMPSON',
  'TUCKER',
  'PORTER',
  'HUNTER',
  'HICKS',
  'CRAWFORD',
  'BOYD',
  'MASON',
  'MORRISON',
  'KENNEDY',
  'WILLIAM',
  'WYATT',
  'WESTON',
  'WALKER',
  'WAYNE',
  'WARREN',
  'WALTER',
  'KEVIN',
  'KYLE',
  'KENT',
  'KEITH',
  'KIRK',
  'KALEB',
  'KENNETH',
  'TYLER',
  'ZACHARY',
  'GREGORY',
  'BRADLEY',
  'ANTHONY',
  'TIMOTHY',
  'DOROTHY',
  'BETHANY',
  'HEATHER',
  'SHIRLEY',
  'SHEILA',
  'SHARON',
  'SHELDON',
  'SHANE',
  'CHADWICK',
  'BECKETT',
  'BROCK',
  'CHANDLER',
  'HUNTER',
  'PARKER',
  'COOPER',
  'SAWYER',
  'FLETCHER',
  'JONATHAN',
  'NATHAN',
  'ETHAN',
  'MATTHEW',
  'CHHENG',
  'DWIGHT',
  'BRITTANY',
  'CHELSEA',
  'ASHLEY',
  'COURTNEY',
  'WHITNEY',
  'BEVERLY',
  'TIFFANY',
  'KIMBERLY',
  'MACKENZIE',
  'MADISON',
  'ADDISON',
  'BROOKLYN',
  'SKYLAR',
  'HARPER',
  'PAXTON',
  'QUINTON',
  'MAXWELL',
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

const PESOS_TRIGRAMAS: Record<string, { h: number; u: number }> = {
  // --- PATRONES HISPANOS (Estructuras de Sílabas) ---
  ' AR': { h: 3, u: 0 },
  ' AN': { h: 2, u: 0 }, // Inicios comunes (Arturo, Andres)
  GUE: { h: 8, u: 0 },
  GUI: { h: 8, u: 0 }, // Rodriguez, Guillen
  LLA: { h: 6, u: 0 },
  LLO: { h: 6, u: 0 }, // Jaramillo, Padilla
  RRA: { h: 5, u: 0 },
  RRE: { h: 5, u: 0 }, // Gutierrez, Aguirre
  QUI: { h: 7, u: 0 },
  QUE: { h: 4, u: 1 }, // Quintero, Enrique
  CIO: { h: 5, u: 0 },
  CIA: { h: 4, u: 1 }, // Mauricio, Garcia
  DRE: { h: 4, u: 0 },
  NDA: { h: 3, u: 0 }, // Alejandro, Miranda
  'EZ ': { h: 10, u: 0 },
  'AS ': { h: 4, u: 2 }, // Finales fuertes (Perez, Rojas)
  INO: { h: 4, u: 0 },
  ITO: { h: 4, u: 0 }, // Benitez, Esposito (influencia italo/hispana)

  // --- PATRONES ESTADOUNIDENSES / ANGLO (Estructuras Fonéticas) ---
  GHT: { h: 0, u: 10 },
  'GH ': { h: 0, u: 6 }, // Wright, Hugh
  SON: { h: 1, u: 9 },
  SEN: { h: 1, u: 7 }, // Johnson, Jensen
  'CK ': { h: 0, u: 8 },
  CKA: { h: 0, u: 5 }, // Black, Packard
  'TH ': { h: 0, u: 9 },
  THA: { h: 0, u: 6 }, // Smith, Jonathan
  'SH ': { h: 0, u: 7 },
  SHA: { h: 0, u: 5 }, // Bush, Marshall
  WRI: { h: 0, u: 8 },
  WRE: { h: 0, u: 6 }, // Wright, Wren
  SCH: { h: 0, u: 9 },
  DTH: { h: 0, u: 8 }, // Schmidt, Meredith
  'OO ': { h: 0, u: 6 },
  OOD: { h: 0, u: 7 }, // Wood, Good
  RLY: { h: 0, u: 7 },
  LEY: { h: 1, u: 6 }, // Early, Stanley
  'ST ': { h: 1, u: 5 },
  RDS: { h: 0, u: 7 }, // West, Edwards
};
// para ver en donde se clasifico cada nombre, se pueden imprimir las variables regex, diccionary y ngrama al final del proceso para tener una idea de cuántos nombres fueron clasificados por cada capa. Esto puede ayudar a entender la efectividad de cada método y ajustar los pesos o patrones si es necesario.
let regex = 0;
let diccionary = 0;
let ngrama = 0;
let tildesONies = 0; // Contador de nombres con tildes o eñes (solo para estadísticas, no afecta la clasificación)
let nulosOVacios = 0; // Contador de nombres nulos o vacíos (solo para estadísticas, no afecta la clasificación)
/**
 * Lógica de clasificación por pesos (INTACTO)
 */
const clasificarNombre = (nombreCompleto: string | undefined): string => {
  if (!nombreCompleto) {
    nulosOVacios++;
    return 'Indeterminado';
  }

  const normalizado = nombreCompleto.toUpperCase().trim();
  const conEspacios = ` ${normalizado} `;
  let scoreHispano = 0;
  let scoreUS = 0;

  // --- CAPA 1: REGEX (Certezas Lingüísticas) ---
  // A. Caracteres irrefutables
  if (/[ÑÁÉÍÓÚÜ]/.test(normalizado)) {
    tildesONies++;
    scoreHispano += 25;
  }

  // B. Partículas y Prefijos de origen (Certeza alta)
  if (/\b(DE|DEL|LA|Y)\b/.test(normalizado)) scoreHispano += 15;
  if (/\b(MC|O'|FITZ|VON)\b/.test(normalizado)) scoreUS += 20;

  // C. Patrones estructurales (Sufijos/Dígrafos Regex)
  if (/EZ\b|NDRO\b|ICIO\b/i.test(normalizado)) scoreHispano += 10;
  if (/(SON|SEN|TH|SH|CK|GH)\b/i.test(normalizado)) scoreUS += 10;
  if (/[KW]/.test(normalizado)) scoreUS += 5; // Letras raras en español

  // Decisión intermedia: Si la diferencia ya es masiva, ahorramos N-Gramas
  let diferencia = Math.abs(scoreHispano - scoreUS);
  if (diferencia >= 20) {
    regex++;
    return scoreHispano > scoreUS ? 'Hispano' : 'Estadounidense';
  }

  // --- CAPA 2: DICCIONARIO Y SUFIJOS (Memoria de Palabras) ---
  const partes = normalizado.split(/\s+/);

  partes.forEach((parte) => {
    // Búsqueda en Sets (O(1))
    if (DATA_HISPANA.has(parte)) scoreHispano += 15;
    if (DATA_US.has(parte)) scoreUS += 15;

    // Sufijos dinámicos (Protección > 3 letras)
    if (parte.length > 3) {
      if (SUFIJOS_HISPANOS.some((suf) => parte.endsWith(suf)))
        scoreHispano += 6;
      if (SUFIJOS_US.some((suf) => parte.endsWith(suf))) scoreUS += 6;
    }
  });

  // Decisión intermedia: Si la diferencia ya es masiva, ahorramos N-Gramas
  diferencia = Math.abs(scoreHispano - scoreUS);
  if (diferencia >= 20) {
    diccionary++;
    return scoreHispano > scoreUS ? 'Hispano' : 'Estadounidense';
  }

  // --- CAPA 3: N-GRAMAS (Análisis Fonético / Desempate) ---
  // Solo se activa si las capas anteriores no dieron una certeza absoluta
  for (let i = 0; i < conEspacios.length - 2; i++) {
    const trigrama = conEspacios.substring(i, i + 3);
    if (PESOS_TRIGRAMAS[trigrama]) {
      scoreHispano += PESOS_TRIGRAMAS[trigrama].h;
      scoreUS += PESOS_TRIGRAMAS[trigrama].u;
    }
  }

  // --- CLASIFICACIÓN FINAL ---
  diferencia = Math.abs(scoreHispano - scoreUS);
  const margenMinimo = 7; // Subimos un poco el margen por la acumulación de puntos

  if (diferencia < margenMinimo) return 'Indeterminado';
  ngrama++;
  return scoreHispano > scoreUS ? 'Hispano' : 'Estadounidense';
};

// --- PROCESAMIENTO CON ESTADÍSTICAS AÑADIDAS ---

export async function procesarArchivo(
  inputPath: string,
  outputPath: string,
  logPath: string,
) {
  // const inputPath = '../pruebas/datosparafrom.csv';
  // const outputPath = '../pruebas/datosdespuesdefrom2.csv';
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
    capas: {
      regex: 0, // Resuelto por Capa 1
      diccionario: 0, // Resuelto por Capa 2
      nGram: 0, // Resuelto por Capa 3
    },
    clasificacion: {
      hispano: 0,
      estadounidense: 0,
      indeterminado: 0,
    },
  };

  // Leemos el archivo para capturar los headers dinámicamente
  return new Promise((resolve, reject) => {
    fs.createReadStream(inputPath)
      .pipe(csv())
      .on('data', (row) => {
        // TRACKING: Contar totales
        stats.totalProcesados++;

        // Clasificamos usando la columna "Owner"
        const origen = clasificarNombre(row.Owner);

        // TRACKING: Contar resultados
        if (origen === 'Hispano') stats.clasificacion.hispano++;
        else if (origen === 'Estadounidense')
          stats.clasificacion.estadounidense++;
        else stats.clasificacion.indeterminado++;

        // Retornamos todo el objeto original + la nueva columna
        filasProcesadas.push({
          ...row,
          Label: origen,
        });
      })
      .on('end', async () => {
        if (filasProcesadas.length === 0) {
          console.log('El CSV está vacío.');
          return;
        }
        // Guardamos las estadísticas de capas y calidad de datos
        stats.capas.regex = regex;
        stats.capas.diccionario = diccionary;
        stats.capas.nGram = ngrama;
        stats.nulosOVacios = nulosOVacios;
        stats.conTildesONies = tildesONies;
        // Configuramos el Writer con los headers dinámicos
        const headers = Object.keys(filasProcesadas[0]).map((key) => ({
          id: key,
          title: key,
        }));

        const csvWriter = createCsvWriter({
          path: outputPath,
          header: headers,
        });

        // --- 📈 GENERAR REPORTE DE ESTADÍSTICAS ---
        const finTimer = Date.now();
        const duracionSegundos = (
          (finTimer - stats.inicioTimer) /
          1000
        ).toFixed(2);

        const calcularPorcentaje = (cantidad: number) =>
          stats.totalProcesados > 0
            ? ((cantidad / stats.totalProcesados) * 100).toFixed(2)
            : '0.00';

        const barraProgreso = (cantidad: number) => {
          const totalSlots = 20;
          const llenos = Math.round(
            (cantidad / stats.totalProcesados) * totalSlots,
          );
          return (
            '┃' + '█'.repeat(llenos) + '░'.repeat(totalSlots - llenos) + '┃'
          );
        };

        const reporteLog = `
      ┌────────────────────────────────────────────────────────────┐
      │          📊 REPORTE FINAL DE CLASIFICACIÓN                 │
      └────────────────────────────────────────────────────────────┘
        > Fecha: ${new Date().toLocaleString()} es-CO
        > Duración: ${duracionSegundos}s
        > Rendimiento: ${(stats.totalProcesados / parseFloat(duracionSegundos)).toFixed(0)} reg/s
        > Archivo Procesado: ${inputPath.split('/').pop()}
      
        ╔══════════════════════════════════════════════════════════╗
        ║                 RESUMEN DE PROCESAMIENTO                 ║
        ╚══════════════════════════════════════════════════════════╝
        
        [ TOTAL PROCESADOS ] : ${stats.totalProcesados.toLocaleString().padEnd(10)}
      
        ● CALIDAD DE DATOS:
          - Nulos/Vacíos   : ${stats.nulosOVacios.toString().padEnd(6)} [ ${calcularPorcentaje(stats.nulosOVacios)}% ]
          - Con Tildes/Ñ   : ${stats.conTildesONies.toString().padEnd(6)} [ ${calcularPorcentaje(stats.conTildesONies)}% ]

        ● EFECTIVIDAD POR CAPA:
          - REGEX     : ${stats.capas.regex.toString().padEnd(6)} [ ${calcularPorcentaje(stats.capas.regex)}% ]
          - DICCIONARY: ${stats.capas.diccionario.toString().padEnd(6)} [ ${calcularPorcentaje(stats.capas.diccionario)}% ]
          - N-GRAMA   : ${stats.capas.nGram.toString().padEnd(6)} [ ${calcularPorcentaje(stats.capas.nGram)}% ]
      
        ● DISTRIBUCIÓN POR ORIGEN:
          (H) HISPANO      : ${stats.clasificacion.hispano.toString().padEnd(6)} [ ${calcularPorcentaje(stats.clasificacion.hispano)}% ]
          ${barraProgreso(stats.clasificacion.hispano)}
      
          (U) EE.UU.       : ${stats.clasificacion.estadounidense.toString().padEnd(6)} [ ${calcularPorcentaje(stats.clasificacion.estadounidense)}% ]
          ${barraProgreso(stats.clasificacion.estadounidense)}
      
          (I) INDETERMINADO: ${stats.clasificacion.indeterminado.toString().padEnd(6)} [ ${calcularPorcentaje(stats.clasificacion.indeterminado)}% ]
          ${barraProgreso(stats.clasificacion.indeterminado)}
      
        ____________________________________________________________
        📁 OUTPUT  : ${outputPath}
        📝 LOG     : ${logPath}
        ────────────────────────────────────────────────────────────
      `;

        try {
          await csvWriter.writeRecords(filasProcesadas);
          // 1. Imprimimos en consola
          console.log(reporteLog);
          // 2. Guardamos en el archivo
          fs.writeFileSync(logPath, reporteLog, 'utf8');
          console.log(
            `✨ Proceso finalizado. Estadísticas exportadas correctamente.`,
          );

          // IMPORTANTE: Solo resolvemos la promesa aquí,
          // cuando el archivo está REALMENTE escrito en disco.
          resolve(true);
        } catch (error) {
          reject(error);
        }
      });
  });
}
