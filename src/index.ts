import { filtrarDatos, generarBackup } from './services/cleaner';
import dotenv from 'dotenv';
dotenv.config();
const CITY = process.env.CITY || 'default_city';
// //From
// import { procesarArchivo } from './services/from';
// const INPUT_FROM = `../csv/${CITY}.csv`;
// const OUTPUT_FROM = `../csv/${CITY}/outputLabel.csv`;
// const logPath = `../logs/${CITY}/stats_From.txt`; // <-- Archivo donde guardaremos el log
// await procesarArchivo(INPUT_FROM, OUTPUT_FROM, logPath);

/**
 * NOTA: Asegúrate de que las carpetas existan:
 * ../csv/${CITY}/
 * ../logs/${CITY}/
 * ../backups/${CITY}/
 */

//cleaner
// Rutas de Archivos Dinámicas
// const INPUT_FILE = OUTPUT_FROM; // El output del from es el input del cleaner
// const OUTPUT_FILE = `../csv/${CITY}/outputcleaner.csv`;
const INPUT_FILE = `./csv/${CITY}/${CITY}.csv`;
const OUTPUT_FILE_ENTIDADES = `./csv/${CITY}/datosEntidades.csv`;
const OUTPUT_FILE_OTRO_ZIP = `./csv/${CITY}/datosOtroZip.csv`;
const OUTPUT_FILE_BALDIOS = `./csv/${CITY}/datosBaldios.csv`;
const OUTPUT_FILE_TRIPLEX = `./csv/${CITY}/datosTriplex.csv`;
const OUTPUT_FILE_DUPLEX = `./csv/${CITY}/datosDuplex.csv`;
const OUTPUT_FILE_SINGLE = `./csv/${CITY}/datosSingle.csv`;

// Backups y Logs
const BACKUP_DIR = `../backups/${CITY}/`;
const backupPath = `${BACKUP_DIR}backup_${CITY}_${Date.now()}_datosIniciales.csv`;
const LOG_FILE = `./logs/${CITY}/cleaner_logs.txt`;
await generarBackup(INPUT_FILE, BACKUP_DIR, backupPath, LOG_FILE);
filtrarDatos(
  INPUT_FILE,
  // OUTPUT_FILE,
  OUTPUT_FILE_ENTIDADES,
  OUTPUT_FILE_OTRO_ZIP,
  OUTPUT_FILE_BALDIOS,
  OUTPUT_FILE_TRIPLEX,
  OUTPUT_FILE_DUPLEX,
  OUTPUT_FILE_SINGLE,
  LOG_FILE,
);
