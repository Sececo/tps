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

/**
 * Lógica de clasificación por pesos (INTACTO)
 */
const clasificarNombre = (nombreCompleto: string | undefined): string => {
  if (!nombreCompleto) return '';

  const normalizado = nombreCompleto.toUpperCase().trim();
  let scoreHispano = 0;
  let scoreUS = 0;

  // 1. Detección de caracteres latinos
  // A. Tildes y Ñ (Prueba irrefutable)
  if (/[ñáéíóúü]/i.test(normalizado)) scoreHispano += 20;

  // B. Dígrafos iniciales y terminaciones comunes
  // 'Ll' al inicio o apellidos que terminan en 'ez' (Rodríguez, Pérez)
  if (/^Ll|ez\b|ndro\b|ocio\b/i.test(normalizado)) scoreHispano += 15;

  // C. Partículas de unión (Muy típicas en apellidos compuestos)
  // Ej: "De la Cruz", "Del Bosque", "Y" (en nombres antiguos)
  if (/\b(de\s|del\s|la\s|y\s)/i.test(normalizado)) scoreHispano += 10;

  // 2. Detección de caracteres Estadounidenses
  // A. Terminaciones de apellidos patronímicos (Hijo de...)
  // -son (Jackson), -sen (Jensen)
  if (/\b\w+(son|sen)\b/i.test(normalizado)) scoreUS += 15;

  // B. Fonemas de fricativas y oclusivas (th, sh, ck, gh)
  // Smith, Marshall, Beckett, Vaughan
  if (/(th|sh|ck|gh|ee|oo|tt|pp)\b/i.test(normalizado)) scoreUS += 15;

  // C. Prefijos de origen gaélico/escocés/irlandés
  // Mc- (McCarthy), O'- (O'Connor), Fitz- (Fitzgerald)
  if (/\b(Mc|O'|Fitz)/i.test(normalizado)) scoreUS += 20;

  // D. Letras de alta frecuencia en inglés, bajas en español
  // La 'k' y 'w' en nombres propios son indicadores fuertes
  if (/[kw]/i.test(normalizado)) scoreUS += 10;

  // 2. Detección de palabras clave

  const partes = normalizado.split(/\s+/);
  const diferencia = Math.abs(scoreHispano - scoreUS);
  const margenMinimo = 10; // El grado de "certeza" que exiges

  partes.forEach((parte) => {
    if (DATA_HISPANA.has(parte)) scoreHispano += 10;
    if (DATA_US.has(parte)) scoreUS += 10;

    if (parte.length > 4) {
      if (SUFIJOS_HISPANOS.some((suf) => parte.endsWith(suf)))
        scoreHispano += 4;
      if (SUFIJOS_US.some((suf) => parte.endsWith(suf))) scoreUS += 4;
    }
  });

  if (scoreHispano > scoreUS) {
    return 'Hispano';
  }
  if (scoreUS > scoreHispano) {
    return 'Estadounidense';
  }
  return 'Indeterminado';
  // if (diferencia < margenMinimo) {
  //   return 'Indeterminado';
  // } else if (scoreHispano > scoreUS) {
  //   return 'Hispano';
  // } else {
  //   return 'Estadounidense';
  // }
};

// --- PROCESAMIENTO CON ESTADÍSTICAS AÑADIDAS ---

const procesarArchivo = async () => {
  // const inputPath = '../pruebas/datosparafrom.csv';
  // const outputPath = '../pruebas/datosdespuesdefrom2.csv';
  const inputPath = '../csv/datosLimpios.csv';
  const outputPath = '../csv/datosFrom2.csv';
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

      const barraProgreso = (cantidad: number) => {
        const totalSlots = 20;
        const llenos = Math.round(
          (cantidad / stats.totalProcesados) * totalSlots,
        );
        return '┃' + '█'.repeat(llenos) + '░'.repeat(totalSlots - llenos) + '┃';
      };

      const reporteLog = `
      ┌────────────────────────────────────────────────────────────┐
      │          📊 REPORTE FINAL DE CLASIFICACIÓN                 │
      └────────────────────────────────────────────────────────────┘
        > Fecha: ${new Date().toLocaleString()}
        > Duración: ${duracionSegundos}s
        > Rendimiento: ${(stats.totalProcesados / parseFloat(duracionSegundos)).toFixed(0)} reg/s
      
        ╔══════════════════════════════════════════════════════════╗
        ║                 RESUMEN DE PROCESAMIENTO                 ║
        ╚══════════════════════════════════════════════════════════╝
        
        [ TOTAL PROCESADOS ] : ${stats.totalProcesados.toLocaleString().padEnd(10)}
      
        ● CALIDAD DE DATOS:
          - Nulos/Vacíos   : ${stats.nulosOVacios.toString().padEnd(6)} [ ${calcularPorcentaje(stats.nulosOVacios)}% ]
          - Con Tildes/Ñ   : ${stats.conTildesONies.toString().padEnd(6)} [ ${calcularPorcentaje(stats.conTildesONies)}% ]
      
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

      // 1. Imprimimos en consola
      console.log(reporteLog);

      // 2. Guardamos en el archivo
      fs.writeFileSync(logPath, reporteLog, 'utf8');
      console.log(
        `✨ Proceso finalizado. Estadísticas exportadas correctamente.`,
      );
    });
};
procesarArchivo();
