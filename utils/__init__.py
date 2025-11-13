"""
Utils package for DNA Storage.
"""

# Importer la classe principale
from .dna_storage import DNAStorage

# Importer les fonctions utilitaires de functions/
from .functions.converts import (
    bytes_to_bits,
    bits_to_bytes,
    bytes_to_trits,
    trits_to_bytes,
    trits_to_dna,
    dna_to_trits,
    bytes_to_dna_goldman,
    dna_to_bytes_goldman,
    GOLDMAN_ENCODE,
    GOLDMAN_DECODE,
)

from .functions.crc import crc8, crc16_ccitt
from .functions.constraints import gc_content, max_run_length, passes_constraints
from .functions.consensus import consensus

__all__ = [
    'DNAStorage',
    # Fonctions de conversion
    'bytes_to_bits',
    'bits_to_bytes',
    'bytes_to_trits',
    'trits_to_bytes',
    'trits_to_dna',
    'dna_to_trits',
    'bytes_to_dna_goldman',
    'dna_to_bytes_goldman',
    'GOLDMAN_ENCODE',
    'GOLDMAN_DECODE',
    # CRC
    'crc8',
    'crc16_ccitt',
    # Contraintes
    'gc_content',
    'max_run_length',
    'passes_constraints',
    # Consensus
    'consensus',
]
