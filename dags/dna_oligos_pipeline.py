"""
Pipeline Airflow pour transformer des fichiers en oligos ADN avec utils.dna_storage
Inspiré du pipeline DNA existant
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import hashlib
import os
import json
import shutil
import time
import random
import logging
from enum import Enum
from dataclasses import dataclass
import sys

# Ajouter utils au path
# Le dossier utils est monté dans /opt/airflow/utils selon docker-compose.yml
sys.path.insert(0, '/opt/airflow')

# Imports utils
from utils.dna_storage import DNAStorage

# Configuration par défaut
default_args = {
    'owner': 'dna_pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration du pipeline
DEFAULT_INPUT_DIR = '/opt/airflow/data/input'
OUTPUT_DIR = '/opt/airflow/data/output'
PROCESSED_DIR = '/opt/airflow/data/processed'
DEAD_LETTER_DIR = '/opt/airflow/data/dead_letter'
TEMP_DIR = '/opt/airflow/temp'

# Configuration de la gestion d'erreurs
BASE_DELAY = 1
MAX_DELAY = 60
JITTER_FACTOR = 0.1
CIRCUIT_BREAKER_TIMEOUT = 300

# Valeurs par défaut des variables Airflow
DEFAULT_CHUNK_SIZE = 100
DEFAULT_MAX_RETRIES = 3
DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
DEFAULT_ERROR_CORRECTION_SYMBOLS = 10

# Configuration logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProcessingStatus(Enum):
    """Statuts de traitement"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class CircuitBreakerState(Enum):
    """États du circuit breaker"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryConfig:
    """Configuration retry"""
    max_retries: int = 3
    base_delay: float = BASE_DELAY
    max_delay: float = MAX_DELAY
    jitter_factor: float = JITTER_FACTOR
    exponential_base: float = 2.0


@dataclass
class CircuitBreakerConfig:
    """Configuration circuit breaker"""
    failure_threshold: int = 5
    timeout: int = CIRCUIT_BREAKER_TIMEOUT
    success_threshold: int = 2


class CircuitBreaker:
    """Circuit breaker"""
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
    
    def can_execute(self) -> bool:
        """Vérifie si exécution autorisée"""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
                return True
            return False
        elif self.state == CircuitBreakerState.HALF_OPEN:
            return True
        return False
    
    def record_success(self):
        """Enregistre succès"""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
    
    def record_failure(self):
        """Enregistre échec"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitBreakerState.CLOSED:
            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN
    
    def _should_attempt_reset(self) -> bool:
        """Vérifie si réinitialisation"""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.config.timeout


class RetryManager:
    """Gestionnaire de retry"""
    
    def __init__(self, config: RetryConfig):
        self.config = config
    
    def calculate_delay(self, attempt: int) -> float:
        """Calcule délai"""
        delay = self.config.base_delay * (self.config.exponential_base ** attempt)
        delay = min(delay, self.config.max_delay)
        jitter = delay * self.config.jitter_factor * (2 * random.random() - 1)
        return max(0, delay + jitter)
    
    def execute_with_retry(self, func, *args, **kwargs):
        """Exécute avec retry"""
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                logger.warning(f"Tentative {attempt + 1}/{self.config.max_retries + 1} échouée")
                
                non_retryable = (FileNotFoundError, PermissionError, ValueError, TypeError)
                if isinstance(e, non_retryable):
                    break
                
                if attempt < self.config.max_retries:
                    delay = self.calculate_delay(attempt)
                    time.sleep(delay)
        
        raise last_exception


# Instances globales
circuit_breaker = None
retry_manager = None


def initialize_components():
    """Initialise composants"""
    global circuit_breaker, retry_manager
    
    config = get_config_variables()
    
    retry_config = RetryConfig(max_retries=config['max_retries'])
    circuit_config = CircuitBreakerConfig(
        failure_threshold=config['circuit_breaker_threshold'],
        timeout=300
    )
    
    circuit_breaker = CircuitBreaker(circuit_config)
    retry_manager = RetryManager(retry_config)
    
    logger.info("Composants initialisés")


def get_input_dir():
    """Récupère répertoire d'entrée"""
    try:
        return Variable.get("input_directory", default_var=DEFAULT_INPUT_DIR)
    except Exception:
        return DEFAULT_INPUT_DIR


def get_config_variables():
    """Récupère toutes les variables de configuration depuis Airflow avec valeurs par défaut"""
    config = {}
    
    # input_directory - valeur par défaut si variable n'existe pas
    try:
        config['input_directory'] = Variable.get("input_directory", default_var=DEFAULT_INPUT_DIR)
    except Exception:
        config['input_directory'] = DEFAULT_INPUT_DIR
    
    # chunk_size - valeur par défaut si variable n'existe pas
    try:
        value = Variable.get("chunk_size", default_var=str(DEFAULT_CHUNK_SIZE))
        config['chunk_size'] = int(value)
    except (ValueError, TypeError, Exception):
        config['chunk_size'] = DEFAULT_CHUNK_SIZE
    
    # max_retries - valeur par défaut si variable n'existe pas
    try:
        value = Variable.get("max_retries", default_var=str(DEFAULT_MAX_RETRIES))
        config['max_retries'] = int(value)
    except (ValueError, TypeError, Exception):
        config['max_retries'] = DEFAULT_MAX_RETRIES
    
    # circuit_breaker_threshold - valeur par défaut si variable n'existe pas
    try:
        value = Variable.get("circuit_breaker_threshold", default_var=str(DEFAULT_CIRCUIT_BREAKER_THRESHOLD))
        config['circuit_breaker_threshold'] = int(value)
    except (ValueError, TypeError, Exception):
        config['circuit_breaker_threshold'] = DEFAULT_CIRCUIT_BREAKER_THRESHOLD
    
    # error_correction_symbols - valeur par défaut si variable n'existe pas
    try:
        value = Variable.get("error_correction_symbols", default_var=str(DEFAULT_ERROR_CORRECTION_SYMBOLS))
        config['error_correction_symbols'] = int(value)
    except (ValueError, TypeError, Exception):
        config['error_correction_symbols'] = DEFAULT_ERROR_CORRECTION_SYMBOLS
    
    return config


def update_file_status(file_hash: str, status: str, file_path: str = None, file_size: int = None, **context):
    """Met à jour ou crée le statut du fichier"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Vérifier si le fichier existe
        check_sql = "SELECT id, file_path, file_size FROM processed_files WHERE file_hash = %s"
        existing = hook.get_first(check_sql, parameters=[file_hash])
        
        if existing:
            # Mettre à jour
            update_sql = """
                UPDATE processed_files 
                SET status = %s, processed_at = CURRENT_TIMESTAMP 
                WHERE file_hash = %s
            """
            hook.run(update_sql, parameters=[status, file_hash])
            logger.info(f"Statut mis à jour: {file_hash[:8]}... → {status}")
        else:
            # Créer nouvelle entrée
            if not file_path or file_size is None:
                logger.warning(f"Données manquantes pour créer l'entrée: file_path={file_path}, file_size={file_size}")
                return
            
            insert_sql = """
                INSERT INTO processed_files (file_hash, file_path, file_size, status, processed_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            hook.run(insert_sql, parameters=[file_hash, file_path, file_size, status])
            logger.info(f"Nouvelle entrée créée: {file_hash[:8]}... → {status}")
            
    except Exception as e:
        logger.error(f"Erreur mise à jour statut: {e}")


def move_to_dead_letter(file_path: str, error_msg: str, context: dict) -> str:
    """Déplace fichier vers dead letter"""
    os.makedirs(DEAD_LETTER_DIR, exist_ok=True)
    
    filename = os.path.basename(file_path)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dead_letter_path = os.path.join(DEAD_LETTER_DIR, f"failed_{timestamp}_{filename}")
    
    if os.path.exists(file_path):
        shutil.move(file_path, dead_letter_path)
    
    logger.error(f"Fichier envoyé au dead letter: {dead_letter_path}")
    return dead_letter_path


def check_database_connection(**context):
    """Vérifie connexion DB"""
    initialize_components()
    
    try:
        if not circuit_breaker.can_execute():
            raise Exception("Circuit breaker ouvert")
        
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        check_table_sql = """
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = 'processed_files' AND table_schema = 'public'
        """
        
        result = hook.get_first(check_table_sql)
        circuit_breaker.record_success()
        
        if result[0] > 0:
            logger.info("Table processed_files trouvée")
            return {'status': 'ready'}
        else:
            logger.warning("Table processed_files non trouvée")
            return {'status': 'table_missing'}
            
    except Exception as e:
        circuit_breaker.record_failure()
        logger.error(f"Erreur connexion DB: {e}")
        raise


def check_files_and_trigger(**context):
    """Vérifie présence de fichiers (tous types)"""
    input_dir = get_input_dir()
    os.makedirs(input_dir, exist_ok=True)
    
    # Lister tous les fichiers (pas seulement .txt)
    all_files = [f for f in os.listdir(input_dir) 
                 if os.path.isfile(os.path.join(input_dir, f))]
    
    if not all_files:
        # Créer fichier de test
        test_file = os.path.join(input_dir, 'sample.txt')
        with open(test_file, 'w') as f:
            f.write("Hello DNA Storage!\nThis is a test file.")
        logger.info(f"Fichier de test créé: {test_file}")
    else:
        logger.info(f"{len(all_files)} fichiers trouvés")
    
    return {'files_found': len(all_files) > 0}


def get_unprocessed_files(**context):
    """Récupère fichiers non traités (tous types)"""
    input_dir = get_input_dir()
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    all_files = []
    for file in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file)
        
        # Ignorer les répertoires
        if not os.path.isfile(file_path):
            continue
        
        # Calculer hash et taille (lecture binaire pour tous types)
        with open(file_path, 'rb') as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
            file_size = os.path.getsize(file_path)
        
        # Vérifier si déjà traité
        check_sql = "SELECT COUNT(*) FROM processed_files WHERE file_hash = %s"
        if hook.get_first(check_sql, parameters=[file_hash])[0] == 0:
            all_files.append({
                'file_path': file_path,
                'file_hash': file_hash,
                'file_size': file_size,
                'filename': file
            })
            logger.info(f"Nouveau: {file} (hash: {file_hash[:8]}...)")
    
    context['task_instance'].xcom_push(key='unprocessed_files', value=all_files)
    return {'count': len(all_files)}


def process_single_file(file_info, **context):
    """Traite un fichier en oligos ADN"""
    initialize_components()
    
    file_path = file_info['file_path']
    file_hash = file_info['file_hash']
    filename = file_info['filename']
    file_size = file_info['file_size']
    
    logger.info(f"Traitement: {filename}")
    
    try:
        if not circuit_breaker.can_execute():
            raise Exception("Circuit breaker ouvert")
        
        update_file_status(file_hash, 'processing', file_path=file_path, file_size=file_size, **context)
        
        def _process_with_retry():
            # Récupérer configuration
            config = get_config_variables()
            
            # Créer encodeur
            dna_storage = DNAStorage(
                chunk_size=config['chunk_size'],
                redundancy=3,
                error_correction=config['error_correction_symbols'],
                segment_nt=120,
                reseed_attempts=4
            )
            
            # Encoder en oligos
            logger.info("Encodage en oligos ADN...")
            oligos = dna_storage.encode_file(file_path)
            
            logger.info(f"{len(oligos)} oligos générés")
            
            # Sauvegarder en DB
            hook = PostgresHook(postgres_conn_id='postgres_default')
            
            # Récupérer l'ID du fichier depuis processed_files
            file_id_sql = "SELECT id FROM processed_files WHERE file_hash = %s"
            file_id_result = hook.get_first(file_id_sql, parameters=[file_hash])
            
            if not file_id_result:
                raise ValueError(f"Fichier non trouvé dans processed_files: {file_hash}")
            
            file_id = file_id_result[0]
            
            # Insérer oligos en batch (tous en une fois)
            # Utiliser ON CONFLICT pour éviter les doublons
            if oligos:
                values = [(file_id, idx, oligo) for idx, oligo in enumerate(oligos)]
                hook.insert_rows(
                    table='dna_oligos',
                    rows=values,
                    target_fields=['file_id', 'sequence_index', 'sequence'],
                    replace=False
                )
            
            # Sauvegarder fichier FASTA
            output_file = os.path.join(OUTPUT_DIR, f"oligos_{filename}.fasta")
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            
            with open(output_file, 'w') as f:
                for idx, oligo in enumerate(oligos):
                    f.write(f">oligo_{idx}\n{oligo}\n")
            
            logger.info(f"{len(oligos)} oligos sauvegardés en DB")
            
            update_file_status(file_hash, 'completed', **context)
            
            return {
                'filename': filename,
                'file_hash': file_hash,
                'num_oligos': len(oligos),
                'output_file': output_file,
                'status': 'completed'
            }
        
        result = retry_manager.execute_with_retry(_process_with_retry)
        circuit_breaker.record_success()
        
        logger.info(f"{filename} traité avec succès")
        return result
        
    except Exception as e:
        circuit_breaker.record_failure()
        error_msg = f"Erreur: {filename}: {str(e)}"
        logger.error(error_msg)
        
        update_file_status(file_hash, 'failed', **context)
        move_to_dead_letter(file_path, error_msg, context)
        
        return {'filename': filename, 'status': 'failed', 'error': error_msg}


def process_all_files(**context):
    """Traite tous les fichiers"""
    initialize_components()
    
    unprocessed = context['task_instance'].xcom_pull(
        task_ids='get_unprocessed_files',
        key='unprocessed_files'
    )
    
    if not unprocessed:
        return {'processed_count': 0}
    
    results = []
    for file_info in unprocessed:
        result = process_single_file(file_info, **context)
        results.append(result)
    
    logger.info(f"Traitement terminé: {len(results)} fichiers")
    
    # Stocker les résultats pour la vérification
    context['task_instance'].xcom_push(key='processed_results', value=results)
    
    return {'processed_count': len(results), 'results': results}


def verify_reconstruction(**context):
    """Vérifie la reconstruction des fichiers depuis les oligos ADN"""
    initialize_components()
    
    # Récupérer les résultats du traitement précédent
    process_result = context['task_instance'].xcom_pull(
        task_ids='process_all_files'
    )
    
    # Vérifier si des fichiers ont été traités
    if not process_result or process_result.get('processed_count', 0) == 0:
        logger.info("Aucun fichier traité dans cette exécution - vérification ignorée")
        return {'verified_count': 0, 'skipped': True}
    
    processed_results = context['task_instance'].xcom_pull(
        task_ids='process_all_files',
        key='processed_results'
    )
    
    if not processed_results:
        logger.info("Aucun résultat de traitement trouvé - vérification ignorée")
        return {'verified_count': 0, 'skipped': True}
    
    # Utiliser les fichiers traités dans cette exécution
    file_hashes_to_check = [r.get('file_hash') for r in processed_results if r.get('status') == 'completed']
    
    if not file_hashes_to_check:
        logger.info("Aucun fichier complété à vérifier dans cette exécution")
        return {'verified_count': 0, 'skipped': False}
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Récupérer configuration
    config = get_config_variables()
    
    dna_storage = DNAStorage(
        chunk_size=config['chunk_size'],
        redundancy=3,
        error_correction=config['error_correction_symbols'],
        segment_nt=120,
        reseed_attempts=4
    )
    
    # Récupérer les infos complètes depuis la DB
    placeholders = ','.join(['%s'] * len(file_hashes_to_check))
    completed_files_sql = f"""
        SELECT id, file_hash, file_path, file_size 
        FROM processed_files 
        WHERE file_hash IN ({placeholders})
    """
    completed_files = hook.get_records(completed_files_sql, parameters=file_hashes_to_check)
    
    if not completed_files:
        logger.info("Aucun fichier complété à vérifier")
        return {'verified_count': 0, 'skipped': False}
    
    logger.info(f"Vérification de {len(completed_files)} fichiers...")
    
    verification_results = []
    
    for file_id, file_hash, file_path, file_size in completed_files:
        try:
            # Récupérer tous les oligos pour ce fichier
            oligos_sql = """
                SELECT sequence_index, sequence 
                FROM dna_oligos 
                WHERE file_id = %s 
                ORDER BY sequence_index
            """
            oligos_records = hook.get_records(oligos_sql, parameters=[file_id])
            
            if not oligos_records:
                logger.warning(f"Aucun oligo trouvé pour file_id={file_id}")
                continue
            
            # Extraire les séquences ADN
            sequences = [seq for _, seq in oligos_records]
            logger.info(f"{len(sequences)} oligos récupérés pour {file_path}")
            
            # Reconstruire le fichier
            logger.info(f"Reconstruction du fichier...")
            success, reconstructed_bytes = dna_storage.decode_sequences(sequences)
            
            if not success:
                logger.error(f"Échec de reconstruction pour {file_path}")
                verification_results.append({
                    'file_id': file_id,
                    'file_path': file_path,
                    'file_hash': file_hash,
                    'status': 'reconstruction_failed',
                    'error': 'decode_sequences returned False'
                })
                continue
            
            # Calculer le hash du fichier reconstruit
            reconstructed_hash = hashlib.md5(reconstructed_bytes).hexdigest()
            
            # Comparer avec le hash original
            if reconstructed_hash == file_hash:
                logger.info(f"Vérification OK: {file_path} (hash: {file_hash[:8]}...)")
                verification_results.append({
                    'file_id': file_id,
                    'file_path': file_path,
                    'file_hash': file_hash,
                    'reconstructed_hash': reconstructed_hash,
                    'status': 'verified',
                    'file_size': file_size,
                    'reconstructed_size': len(reconstructed_bytes),
                    'num_oligos': len(sequences)
                })
                
                # Mettre à jour le statut (optionnel: ajouter un champ verified)
                update_sql = """
                    UPDATE processed_files 
                    SET status = 'verified'
                    WHERE id = %s
                """
                hook.run(update_sql, parameters=[file_id])
            else:
                logger.error(f"Hash mismatch pour {file_path}")
                logger.error(f"   Original: {file_hash}")
                logger.error(f"   Reconstruit: {reconstructed_hash}")
                verification_results.append({
                    'file_id': file_id,
                    'file_path': file_path,
                    'file_hash': file_hash,
                    'reconstructed_hash': reconstructed_hash,
                    'status': 'hash_mismatch',
                    'file_size': file_size,
                    'reconstructed_size': len(reconstructed_bytes),
                    'num_oligos': len(sequences)
                })
                
        except Exception as e:
            logger.error(f"Erreur lors de la vérification de {file_path}: {e}")
            verification_results.append({
                'file_id': file_id,
                'file_path': file_path,
                'file_hash': file_hash,
                'status': 'verification_error',
                'error': str(e)
            })
    
    verified_count = sum(1 for r in verification_results if r['status'] == 'verified')
    logger.info(f"Vérification terminée: {verified_count}/{len(verification_results)} fichiers vérifiés avec succès")
    
    return {
        'verified_count': verified_count,
        'total_checked': len(verification_results),
        'results': verification_results
    }


# Création du DAG
dag = DAG(
    'dna_oligos_pipeline',
    default_args=default_args,
    description='Transforme fichiers en oligos ADN avec utils.dna_storage',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['dna', 'oligos'],
)

# Tâches
task_setup_vars = PythonOperator(
    task_id='setup_airflow_variables',
    python_callable=lambda: Variable.set('input_directory', DEFAULT_INPUT_DIR),
    dag=dag,
)

task_check_db = PythonOperator(
    task_id='check_database_connection',
    python_callable=check_database_connection,
    dag=dag,
)

task_check_files = PythonOperator(
    task_id='check_files_and_trigger',
    python_callable=check_files_and_trigger,
    dag=dag,
)

task_get_unprocessed = PythonOperator(
    task_id='get_unprocessed_files',
    python_callable=get_unprocessed_files,
    dag=dag,
)

task_process_all = PythonOperator(
    task_id='process_all_files',
    python_callable=process_all_files,
    dag=dag,
)

task_verify_reconstruction = PythonOperator(
    task_id='verify_reconstruction',
    python_callable=verify_reconstruction,
    dag=dag,
)

# Dépendances
task_setup_vars >> task_check_db >> task_check_files >> task_get_unprocessed >> task_process_all >> task_verify_reconstruction

