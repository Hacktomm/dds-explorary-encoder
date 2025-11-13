# Pipeline ADN - Stockage de fichiers en oligos ADN

Pipeline Airflow pour transformer des fichiers en oligos ADN et les stocker dans une base de données PostgreSQL.

## Lien entre Biologie, Art et Data

Ce projet explore le lien profond entre la biologie, l'art et la donnée.

### Biologie et Data

Le lien entre biologie et data est immédiat. La biologie repose sur des séquences d'information transformées en matière. Là où un ordinateur code en 0 et 1, la vie le fait en A, C, G et T. L'ADN est le support naturel du stockage d'information, avec une densité et une pérennité inégalées.

### Art et Data

Le lien entre art et data est plus discret. Paul Klee écrivait : « L'art ne reproduit pas le visible, il rend visible. » Ainsi, l'art révèle les structures cachées du monde et les transforme en une expérience sensible. Là où la donnée mesure, l'art rend sensible.

### La convergence

En liant biologie, art et data, il devient possible de créer des formes porteuses de sens cachés. Par exemple, en encodant un message dans une séquence d'ADN synthétique, puis en transformant cette séquence d'ADN en image, on obtient une nouvelle représentation du message. Le spectateur n'observe plus une image, mais une nouvelle interprétation issue du vivant, un message enfoui dans la matière même de la vie.

Ce pipeline technique est donc un outil au service de cette exploration : transformer des données numériques en séquences ADN, qui peuvent ensuite devenir matière artistique, révélant ainsi les structures cachées qui relient le numérique au vivant.

## Structure du projet

```
Pipeline/
├── dags/
│   └── dna_oligos_pipeline.py    # DAG principal Airflow
│
├── utils/                         # Bibliothèque de fonctions utilitaires
│   ├── __init__.py               # Exports du package
│   ├── dna_storage.py            # Classe principale DNAStorage
│   └── functions/                # Modules spécialisés
│       ├── converts.py           # Conversions bytes ↔ trits ↔ ADN (Goldman)
│       ├── crc.py                # CRC-8 et CRC-16-CCITT
│       ├── constraints.py       # Contraintes GC content et run length
│       ├── consensus.py          # Consensus sur réplicats
│       └── encode.py             # Fonctions d'encodage
│
├── data/                          # Données du pipeline
│   ├── input/                    # Fichiers à traiter
│   ├── output/                   # Fichiers FASTA générés
│   └── dead_letter/              # Fichiers en échec
│
├── docker-compose.yml            # Configuration Airflow + PostgreSQL
├── init-scripts.sql              # Schéma de base de données
├── requirements.txt              # Dépendances Python
└── airflow_variables.json        # Variables Airflow (exemple)
```

## Architecture

### Composants principaux

#### 1. **utils/dna_storage.py** - Classe principale
- `DNAStorage`: Encodeur/décodeur principal
- Utilise l'encodage Goldman (sans homopolymères)
- Correction d'erreurs Reed-Solomon
- Préfixe robuste de 80 bases avec CRC-8
- Support jusqu'à 16M chunks avec 1K séquences par chunk

#### 2. **utils/functions/** - Modules utilitaires

**converts.py**
- `bytes_to_trits()` / `trits_to_bytes()`: Conversion base 3
- `trits_to_dna()` / `dna_to_trits()`: Mapping Goldman
- `bytes_to_dna_goldman()` / `dna_to_bytes_goldman()`: Conversion directe
- `bytes_to_bits()` / `bits_to_bytes()`: Manipulation binaire

**crc.py**
- `crc8()`: Checksum 8 bits
- `crc16_ccitt()`: Checksum 16 bits CCITT

**constraints.py**
- `gc_content()`: Calcul du pourcentage GC
- `max_run_length()`: Longueur maximale de répétition
- `passes_constraints()`: Validation GC/run
- `encode_with_constraints()`: Encodage avec re-seed

**consensus.py**
- `consensus()`: Vote majoritaire sur réplicats

#### 3. **dags/dna_oligos_pipeline.py** - Pipeline Airflow

**Tâches du DAG:**
1. `setup_airflow_variables`: Configuration des variables
2. `check_database_connection`: Vérification PostgreSQL
3. `check_files_and_trigger`: Détection de fichiers
4. `get_unprocessed_files`: Filtrage des doublons (hash MD5)
5. `process_all_files`: Traitement et encodage
6. `verify_reconstruction`: Vérification par hash

**Fonctionnalités:**
- Circuit Breaker (protection contre cascades d'erreurs)
- Retry avec backoff exponentiel
- Gestion des doublons via hash MD5
- Insertion batch des oligos en DB
- Vérification automatique de reconstruction

## Base de données

### Tables

#### `processed_files`
```sql
- id: SERIAL PRIMARY KEY
- file_hash: VARCHAR(32) UNIQUE  -- Hash MD5 pour déduplication
- file_path: VARCHAR(500)
- file_size: BIGINT
- status: VARCHAR(20)             -- pending, processing, completed, verified, failed
- processed_at: TIMESTAMP
- created_at: TIMESTAMP
```

#### `dna_oligos`
```sql
- id: SERIAL PRIMARY KEY
- file_id: INTEGER REFERENCES processed_files(id)
- sequence_index: INTEGER         -- Ordre de la séquence
- sequence: TEXT                 -- Séquence ADN complète
- chunk_idx: INTEGER             -- Index du chunk
- seq_type: VARCHAR(10)         -- H (Header), D (Data), P (Parity)
- created_at: TIMESTAMP
- UNIQUE(file_id, sequence_index)
```

## Utilisation

### 1. Démarrer les services

```bash
docker-compose up -d
```

Services démarrés:
- **PostgreSQL**: Base de données
- **Redis**: Broker Celery
- **Airflow Webserver**: Interface web (http://localhost:8081)
- **Airflow Scheduler**: Planificateur
- **Airflow Worker**: Exécuteur de tâches

### 2. Accéder à l'interface

http://localhost:8081  
**Credentials**: `airflow` / `airflow`

### 3. Configurer Airflow

#### 3.1. Créer la connexion PostgreSQL

1. Dans l'interface Airflow, aller dans **Admin → Connections**
2. Cliquer sur **+** (ajouter)
3. Configurer:
   - **Connection Id**: `postgres_default`
   - **Connection Type**: `Postgres`
   - **Host**: `postgres`
   - **Schema**: `airflow`
   - **Login**: `airflow`
   - **Password**: `airflow`
   - **Port**: `5432`
4. Cliquer sur **Save**

#### 3.2. Créer les variables de configuration

1. Aller dans **Admin → Variables**
2. Pour chaque variable, cliquer sur **+** (ajouter) et configurer:

**Variable obligatoire:**
- **Key**: `input_directory`
- **Value**: `/opt/airflow/data/input`

**Variables optionnelles (valeurs par défaut utilisées si non définies):**
- **Key**: `chunk_size`, **Value**: `1000`
- **Key**: `max_retries`, **Value**: `3`
- **Key**: `circuit_breaker_threshold`, **Value**: `5`
- **Key**: `error_correction_symbols`, **Value**: `10`

3. Cliquer sur **Save** pour chaque variable

**Note**: 
- Le chemin `/opt/airflow/data/input` correspond au montage Docker. Les fichiers doivent être placés dans `data/input/` sur votre machine locale.
- Les variables optionnelles peuvent être omises si vous souhaitez utiliser les valeurs par défaut.

### 4. Ajouter un fichier à traiter

```bash
# Créer le répertoire si nécessaire
mkdir -p data/input

# Ajouter un fichier (tous types supportés)
cp mon_fichier.pdf data/input/
```

### 5. Exécuter le pipeline

Dans l'interface Airflow:
1. Aller dans **DAGs**
2. Trouver `dna_oligos_pipeline`
3. Activer le DAG (toggle)
4. Déclencher manuellement (Play) ou attendre le schedule

### 6. Vérifier les résultats

```bash
# Voir les fichiers traités
docker-compose exec postgres psql -U airflow -d airflow -c "
  SELECT file_path, status, processed_at 
  FROM processed_files 
  ORDER BY processed_at DESC LIMIT 10;
"

# Compter les oligos par fichier
docker-compose exec postgres psql -U airflow -d airflow -c "
  SELECT pf.file_path, COUNT(do.id) as num_oligos
  FROM processed_files pf
  LEFT JOIN dna_oligos do ON pf.id = do.file_id
  GROUP BY pf.id, pf.file_path;
"
```

## Flux de traitement

```
Fichier dans data/input/
    ↓
get_unprocessed_files()
    ├─ Calcul hash MD5
    └─ Vérifie doublons dans processed_files
    ↓
process_all_files()
    ├─ UPDATE processed_files SET status='processing'
    ├─ DNAStorage.encode_file()
    │   ├─ Découpe en chunks
    │   ├─ CRC-32 par chunk
    │   ├─ Reed-Solomon encoding
    │   ├─ Encodage Goldman
    │   └─ Génération préfixes (80 bases)
    ├─ INSERT INTO dna_oligos (batch)
    ├─ Sauvegarde FASTA dans data/output/
    └─ UPDATE processed_files SET status='completed'
    ↓
verify_reconstruction()
    ├─ Récupère oligos depuis DB
    ├─ DNAStorage.decode_sequences()
    ├─ Calcule hash MD5 du fichier reconstruit
    └─ Compare avec hash original
        ├─ Si match: UPDATE status='verified'
        └─ Si mismatch: status='hash_mismatch'
```

## Configuration

### Variables Airflow

Toutes les variables de configuration peuvent être définies dans l'interface Airflow: **Admin → Variables**

| Variable | Valeur par défaut | Description |
|----------|-------------------|-------------|
| `input_directory` | `/opt/airflow/data/input` | Répertoire d'entrée des fichiers à traiter |
| `chunk_size` | `1000` | Taille des segments en bytes (affecte la granularité de segmentation) |
| `max_retries` | `3` | Nombre maximum de tentatives en cas d'erreur |
| `circuit_breaker_threshold` | `5` | Seuil du circuit breaker (protège contre les cascades d'échecs) |
| `error_correction_symbols` | `10` | Nombre de symboles Reed-Solomon par chunk (robustesse à l'erreur) |

**Note**: Si une variable n'est pas définie, la valeur par défaut sera utilisée. Toutes ces variables sont récupérées dynamiquement à chaque exécution.

### Paramètres DNAStorage fixes


```python
dna_storage = DNAStorage(
    chunk_size=<variable>,           # Configurable via variable chunk_size
    redundancy=3,                   # Fixe: 3 réplicats par séquence
    error_correction=<variable>,    # Configurable via variable error_correction_symbols
    segment_nt=120,                 # Fixe: 120 bases par segment
    reseed_attempts=4               # Fixe: 4 essais pour respecter contraintes
)
```

## Format des oligos

Chaque oligo contient:

### Header (80 bases)
```
AG                          # SYNC (2 bases)
CC                          # TYPE: AA=Header, CC=Data, GG=Parity (2 bases)
{68 bases}                 # Champs: chunk_idx(24) + total_chunks(24) + 
                            #         seq_idx(10) + total_seqs(10)
{8 bases}                   # CRC-8 des 68 bits précédents
```

### Payload
```
{120 bases typiquement}     # Données encodées en Goldman
```

**Total**: ~200 bases par oligo

## Statuts des fichiers

| Statut | Description |
|--------|-------------|
| `pending` | En attente de traitement |
| `processing` | En cours d'encodage |
| `completed` | Encodage terminé, oligos en DB |
| `verified` | Reconstruction vérifiée (hash OK) |
| `failed` | Échec du traitement |
| `hash_mismatch` | Reconstruction échouée (hash différent) |

## Dépendances

### Python
- `reedsolo==1.7.0`: Correction d'erreurs Reed-Solomon
- `bitstring==4.1.0`: Manipulation de bits
- `psycopg2-binary==2.9.9`: Connecteur PostgreSQL
- `sqlalchemy==1.4.53`: ORM

### Docker
- `apache/airflow:2.9.0-python3.11`
- `postgres:13`
- `redis:latest`

## Dépannage

### Le DAG ne détecte pas les fichiers

```bash
# Vérifier que le volume est monté
docker-compose exec airflow-webserver ls /opt/airflow/data/input/

# Vérifier les permissions
chmod -R 755 data/
```

### Erreur "Module not found: utils"

```bash
# Redémarrer les conteneurs après modification docker-compose.yml
docker-compose down
docker-compose up -d
```

### Vider la base de données

```bash
docker-compose exec postgres psql -U airflow -d airflow -c "
  TRUNCATE TABLE dna_oligos, processed_files CASCADE;
"
```

### Voir les logs

```bash
# Logs scheduler
docker-compose logs -f airflow-scheduler

# Logs worker
docker-compose logs -f airflow-worker

# Logs d'une tâche spécifique
# Dans Airflow UI: Graph View → Tâche → View Log
```

## Performance

### Exemple de traitement

**Fichier image JPG (1.5 MB)**:
- **Oligos générés**: ~322,000
- **Temps d'encodage**: ~4 secondes
- **Temps d'insertion DB**: ~28 secondes (batch)
- **Temps de vérification**: ~15 secondes

### Optimisations

- **Insertion batch**: Tous les oligos en une requête
- **Circuit Breaker**: Protection contre surcharge
- **Retry intelligent**: Backoff exponentiel avec jitter
- **Déduplication**: Hash MD5 pour éviter retraitement

## Sécurité

- Les fichiers originaux restent dans `data/input/` (non supprimés)
- Les fichiers en échec sont déplacés vers `data/dead_letter/`
- Hash MD5 pour détecter les doublons
- CRC-8 dans les préfixes pour validation
- CRC-32 par chunk pour détecter corruption

## Notes techniques

### Encodage Goldman

- Évite les homopolymères (pas de AAA, CCC, etc.)
- Mapping trits → ADN selon dernière base
- Re-seed si contraintes GC/run non respectées

### Reed-Solomon

- GF(256): Longueur code ≤ 255 bytes
- Contrainte: `chunk_size + 4 (CRC32) + nsym ≤ 255`
- Correction jusqu'à `nsym/2` erreurs

### Préfixe robuste

- SYNC: `AG` (détection début)
- TYPE: `AA`/`CC`/`GG` (Header/Data/Parity)
- CRC-8: Validation des champs
- Support: 16M chunks × 1K séquences

## Ressources

- [Documentation Airflow](https://airflow.apache.org/docs/)
- [Reed-Solomon](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction)
- [Goldman Encoding](https://en.wikipedia.org/wiki/DNA_digital_data_storage)

## Licence

Projet privé - DNA Data Symphonia

