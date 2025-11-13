-- Initialisation de la base de données Airflow
-- (Ces commandes sont déjà exécutées par les variables d'environnement Docker)

-- Création de la table pour tracker les fichiers traités
CREATE TABLE IF NOT EXISTS processed_files (
    id SERIAL PRIMARY KEY,
    file_hash VARCHAR(32) UNIQUE NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    file_size BIGINT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'completed',
    output_file VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Création des index pour optimiser les performances
CREATE INDEX IF NOT EXISTS idx_processed_files_hash ON processed_files(file_hash);
CREATE INDEX IF NOT EXISTS idx_processed_files_path ON processed_files(file_path);
CREATE INDEX IF NOT EXISTS idx_processed_files_status ON processed_files(status);
CREATE INDEX IF NOT EXISTS idx_processed_files_processed_at ON processed_files(processed_at);

-- Commentaire sur la table
COMMENT ON TABLE processed_files IS 'Table de tracking des fichiers traités par le pipeline';
COMMENT ON COLUMN processed_files.file_hash IS 'Hash MD5 unique du fichier pour éviter les doublons';
COMMENT ON COLUMN processed_files.file_path IS 'Chemin original du fichier traité';
COMMENT ON COLUMN processed_files.status IS 'Statut du traitement: pending, processing, completed, verified, failed';

-- Création de la table pour stocker les oligos ADN
CREATE TABLE IF NOT EXISTS dna_oligos (
    id SERIAL PRIMARY KEY,
    file_id INTEGER REFERENCES processed_files(id) ON DELETE CASCADE,
    sequence_index INTEGER NOT NULL,
    sequence TEXT NOT NULL,
    chunk_idx INTEGER,
    seq_type VARCHAR(10),
    total_chunks INTEGER,
    seq_idx INTEGER,
    total_seqs INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(file_id, sequence_index)
);

-- Index pour optimiser les requêtes
CREATE INDEX IF NOT EXISTS idx_dna_oligos_file_id ON dna_oligos(file_id);
CREATE INDEX IF NOT EXISTS idx_dna_oligos_sequence_index ON dna_oligos(sequence_index);
CREATE INDEX IF NOT EXISTS idx_dna_oligos_chunk_idx ON dna_oligos(chunk_idx);
CREATE INDEX IF NOT EXISTS idx_dna_oligos_seq_type ON dna_oligos(seq_type);

-- Commentaires sur la table
COMMENT ON TABLE dna_oligos IS 'Table de stockage des oligos ADN générés depuis les fichiers';
COMMENT ON COLUMN dna_oligos.file_id IS 'ID du fichier source dans processed_files';
COMMENT ON COLUMN dna_oligos.sequence_index IS 'Index de la séquence dans le fichier';
COMMENT ON COLUMN dna_oligos.sequence IS 'Séquence ADN de l''oligo';
COMMENT ON COLUMN dna_oligos.chunk_idx IS 'Index du chunk dans le fichier';
COMMENT ON COLUMN dna_oligos.seq_type IS 'Type de séquence: H (Header), D (Data), P (Parity)';
