"""
Système de stockage sur ADN auto-suffisant.
- Préfixe robuste (80 bases) avec CRC-8
- Support jusqu'à 16M chunks avec 1K séquences par chunk
- CRC-32 par chunk (avant RS) pour détecter les faux positifs de décodage
- Respect des limites Reed-Solomon GF(256) : longueur code ≤ 255
- Encodage Goldman (sans homopolymères), re-seeding simple pour contraintes GC/run
- Consensus sur réplicats
"""

from __future__ import annotations
import hashlib
import zlib
from typing import List, Tuple, Optional
from collections import Counter
try:
    # Essayer import absolu (depuis utils package)
    from .functions.converts import bytes_to_bits, bits_to_bytes
    from .functions.crc import crc8
    from .functions.constraints import passes_constraints
    from .functions.converts import bytes_to_dna_goldman, dna_to_bytes_goldman
except ImportError:
    # Fallback: essayer import depuis airf low
    try:
        from utils.functions.converts import bytes_to_bits, bits_to_bytes
        from utils.functions.crc import crc8
        from utils.functions.constraints import passes_constraints
        from utils.functions.converts import bytes_to_dna_goldman, dna_to_bytes_goldman
    except ImportError:
        # Dernier fallback: import direct si dans le même dir
        import sys
        import os
        # Ajouter le répertoire parent
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from utils.functions.converts import bytes_to_bits, bits_to_bytes
        from utils.functions.crc import crc8
        from utils.functions.constraints import passes_constraints
        from utils.functions.converts import bytes_to_dna_goldman, dna_to_bytes_goldman

try:
    from reedsolo import RSCodec
except ImportError:
    raise ImportError("Installation requise: pip install reedsolo")




# =========================
# Classe principale
# =========================

class DNAStorage:
    """
    Stockage ADN auto-suffisant (tout dans les séquences).
    Champs encodés dans un préfixe robuste de 80 bases avec CRC-8.
    Support jusqu'à 16M chunks avec 1K séquences par chunk.
    """
    # Codes de type (2 bases après sync)
    TYPE_CODE = {'H': 'AA', 'D': 'CC', 'P': 'GG'}
    TYPE_DECODE = {'AA': 'H', 'CC': 'D', 'GG': 'P'}

    def __init__(self,
                 chunk_size: int = 100,          # bytes de données brutes par chunk (avant CRC)
                 redundancy: int = 3,            # réplicats par séquence
                 error_correction: int = 10,     # nsym RS (symboles de parité)
                 segment_nt: int = 120,          # longueur d'un segment ADN (payload nt par séquence)
                 prefix_len: int = 80,           # long. préfixe (fixe: 80)
                 reseed_attempts: int = 4):      # essais de re-seed (bases de départ)
        # RS constraint: codeword_len = (chunk + CRC32 4B) + nsym ≤ 255
        if chunk_size + 4 + error_correction > 255:
            raise ValueError(
                f"chunk_size({chunk_size}) + 4 (CRC32) + nsym({error_correction}) doit être ≤ 255."
            )
        self.chunk_size = chunk_size
        self.redundancy = redundancy
        self.error_correction = error_correction
        self.segment_nt = segment_nt
        self.prefix_len = prefix_len
        self.reseed_attempts = reseed_attempts

    # ---------- Préfixe (80 bases) ----------
    # Layout:
    #  SYNC(2) = 'AG'
    #  TYPE(2) = 'AA' (H) | 'CC' (D) | 'GG' (P)
    #  CHUNK_IDX (24 bits, C/T)
    #  TOTAL_CHUNKS (24 bits, C/T)
    #  SEQ_IDX (10 bits, C/T)
    #  TOTAL_SEQS (10 bits, C/T)
    #  CRC8 des 68 bits précédents (8 bits, C/T)
    # Mapping binaire→base: 0→'T', 1→'C'
    # Longueur totale: 2 + 2 + 68 + 8 = 80 bases

    @staticmethod
    def _bin_to_ct(bitstr: str) -> str:
        return ''.join('C' if b == '1' else 'T' for b in bitstr)

    @staticmethod
    def _ct_to_bin(ct: str) -> str:
        return ''.join('1' if b == 'C' else '0' for b in ct)

    def _create_prefix(self,
                       chunk_idx: int,
                       total_chunks: int,
                       seq_type: str,
                       seq_idx: int,
                       total_seqs: int) -> str:
        if seq_type not in self.TYPE_CODE:
            raise ValueError("seq_type doit être 'H', 'D' ou 'P'.")

        # bornes
        if not (0 <= chunk_idx < 2**24) or not (0 <= total_chunks < 2**24):
            raise ValueError("chunk_idx/total_chunks doivent tenir sur 24 bits (0..16777215).")
        if not (0 <= seq_idx < 2**10) or not (0 <= total_seqs < 2**10):
            raise ValueError("seq_idx/total_seqs doivent tenir sur 10 bits (0..1023).")

        fields_bits = (
            bytes_to_bits(chunk_idx, 24) +      # 24 bits = 16M chunks max
            bytes_to_bits(total_chunks, 24) +    # 24 bits = 16M chunks max
            bytes_to_bits(seq_idx, 10) +        # 10 bits = 1024 seqs/chunk max
            bytes_to_bits(total_seqs, 10)        # 10 bits = 1024 seqs/chunk max
        )
        crc = crc8(bits_to_bytes(fields_bits))  # CRC-8 des 36 bits packés
        crc_bits = bytes_to_bits(crc, 8)

        prefix = []
        prefix.append('AG')                                 # SYNC
        prefix.append(self.TYPE_CODE[seq_type])             # TYPE
        prefix.append(self._bin_to_ct(fields_bits))         # CHAMPS
        prefix.append(self._bin_to_ct(crc_bits))            # CRC8

        out = ''.join(prefix)
        assert len(out) == 80
        return out

    def _parse_prefix(self, sequence: str) -> Optional[dict]:
        if len(sequence) < 80:
            return None
        sync = sequence[:2]
        if sync != 'AG':
            return None
        type_code = sequence[2:4]
        seq_type = self.TYPE_DECODE.get(type_code)
        if not seq_type:
            return None

        fields_ct = sequence[4:4+68]
        crc_ct = sequence[72:80]
        fields_bits = self._ct_to_bin(fields_ct)
        crc_bits = self._ct_to_bin(crc_ct)

        calc_crc = crc8(bits_to_bytes(fields_bits))
        if bytes_to_bits(calc_crc, 8) != crc_bits:
            return None  # CRC préfixe invalide

        chunk_idx = int(fields_bits[0:24], 2)
        total_chunks = int(fields_bits[24:48], 2)
        seq_idx = int(fields_bits[48:58], 2)
        total_seqs = int(fields_bits[58:68], 2)

        return {
            'seq_type': seq_type,
            'chunk_idx': chunk_idx,
            'total_chunks': total_chunks,
            'seq_idx': seq_idx,
            'total_seqs': total_seqs,
            'payload': sequence[80:],  # après préfixe (80 bases)
        }

    # ---------- Header fichier (auto-contenu) ----------
    # bytes:
    #  - file_size: 8B
    #  - chunk_size: 2B
    #  - nsym (RS): 1B
    #  - reserved: 1B
    #  - checksum SHA256 (tronc.) : 8B
    #  - CRC16 sur les 20 premiers: 2B (CCITT)
    # total 22B

    @staticmethod
    def _crc16_ccitt(data: bytes, poly=0x1021, init=0xFFFF) -> int:
        crc = init
        for b in data:
            crc ^= (b << 8) & 0xFFFF
            for _ in range(8):
                if crc & 0x8000:
                    crc = ((crc << 1) ^ poly) & 0xFFFF
                else:
                    crc = (crc << 1) & 0xFFFF
        return crc & 0xFFFF

    def _create_header(self, file_size: int, checksum8: bytes) -> bytes:
        hdr = bytearray(22)
        hdr[0:8] = file_size.to_bytes(8, 'little')
        hdr[8:10] = self.chunk_size.to_bytes(2, 'little')
        hdr[10] = self.error_correction
        hdr[11] = 0  # reserved
        hdr[12:20] = checksum8
        crc16 = self._crc16_ccitt(bytes(hdr[:20]))
        hdr[20:22] = crc16.to_bytes(2, 'little')
        return bytes(hdr)

    def _parse_header(self, hdr: bytes) -> dict:
        if len(hdr) < 22:
            raise ValueError("Header trop court")
        crc_calc = self._crc16_ccitt(hdr[:20])
        crc_read = int.from_bytes(hdr[20:22], 'little')
        if crc_calc != crc_read:
            raise ValueError("CRC16 du header invalide")

        file_size = int.from_bytes(hdr[0:8], 'little')
        chunk_size = int.from_bytes(hdr[8:10], 'little')
        nsym = hdr[10]
        checksum8 = hdr[12:20]
        num_chunks = (file_size + chunk_size - 1) // chunk_size
        # revalide contrainte RS
        if chunk_size + 4 + nsym > 255:
            raise ValueError("Paramètres RS invalides dans le header (dépasse 255).")

        return {
            'file_size': file_size,
            'chunk_size': chunk_size,
            'nsym': nsym,
            'checksum8': checksum8,
            'num_chunks': num_chunks,
        }

    # ---------- Encodage / Décodage haut niveau ----------

    def encode_file(self, file_path: str) -> List[str]:
        with open(file_path, 'rb') as f:
            data = f.read()

        file_size = len(data)
        checksum8 = hashlib.sha256(data).digest()[:8]
        header_bytes = self._create_header(file_size, checksum8)

        # Header → ADN (une séquence, fortement répliquée)
        header_dna = bytes_to_dna_goldman(header_bytes, start='A')
        header_prefix = self._create_prefix(
            chunk_idx=0, total_chunks=(file_size + self.chunk_size - 1) // self.chunk_size,
            seq_type='H', seq_idx=0, total_seqs=1
        )
        header_seq = header_prefix + header_dna

        sequences: List[str] = []
        for _ in range(self.redundancy * 2):
            sequences.append(header_seq)

        # Split en chunks (bytes)
        chunks = [data[i:i+self.chunk_size] for i in range(0, file_size, self.chunk_size)]
        rs = RSCodec(self.error_correction)

        for chunk_idx, chunk in enumerate(chunks, start=1):
            # CRC-32 sur le chunk (avant RS)
            crc32 = zlib.crc32(chunk) & 0xFFFFFFFF
            payload = chunk + crc32.to_bytes(4, 'little')  # longueur data_RS = chunk+4

            # RS encode → codeword de longueur data_RS + nsym (≤255)
            codeword = rs.encode(payload)
            data_rs = codeword[:len(payload)]      # partie données (incl. CRC32)
            parity_rs = codeword[len(payload):]    # parité

            # bytes → ADN (avec re-seed simple pour contraintes)
            data_dna = self._encode_with_constraints(data_rs)
            parity_dna = self._encode_with_constraints(parity_rs)

            # Découper en segments ADN
            data_segments = [data_dna[i:i+self.segment_nt] for i in range(0, len(data_dna), self.segment_nt)]
            parity_segments = [parity_dna[i:i+self.segment_nt] for i in range(0, len(parity_dna), self.segment_nt)]
            total_seqs = len(data_segments) + len(parity_segments)
            if total_seqs >= 256:
                raise ValueError("Trop de segments par chunk (>255). Augmenter segment_nt ou réduire chunk_size/nsym.")

            # Créer séquences + préfixes, avec réplication
            # Data
            for seq_i, seg in enumerate(data_segments):
                prefix = self._create_prefix(
                    chunk_idx=chunk_idx,
                    total_chunks=len(chunks),
                    seq_type='D',
                    seq_idx=seq_i,
                    total_seqs=total_seqs
                )
                seq = prefix + seg
                for _ in range(self.redundancy):
                    sequences.append(seq)

            # Parity
            for p_i, seg in enumerate(parity_segments):
                prefix = self._create_prefix(
                    chunk_idx=chunk_idx,
                    total_chunks=len(chunks),
                    seq_type='P',
                    seq_idx=len(data_segments) + p_i,
                    total_seqs=total_seqs
                )
                seq = prefix + seg
                for _ in range(self.redundancy):
                    sequences.append(seq)

        return sequences

    def decode_sequences(self, sequences: List[str]) -> Tuple[bool, bytes]:
        # Parser tous les préfixes valides
        parsed = []
        for s in sequences:
            info = self._parse_prefix(s)
            if info:
                parsed.append(info)
        if not parsed:
            return False, b""

        # Header
        header_reads = [p['payload'] for p in parsed if p['seq_type'] == 'H' and p['chunk_idx'] == 0]
        if not header_reads:
            return False, b""
        header_dna_cons = self._consensus(header_reads)
        try:
            header_bytes = dna_to_bytes_goldman(header_dna_cons, start='A')
            header = self._parse_header(header_bytes)
        except Exception:
            return False, b""

        nsym = header['nsym']
        rs = RSCodec(nsym)

        # Grouper par chunk
        chunks_map = {}  # chunk_idx -> {'total_seqs': int, 'data': {seq_idx:[reads]}, 'parity':{seq_idx:[reads]}}
        for p in parsed:
            if p['seq_type'] == 'H':
                continue
            ci = p['chunk_idx']
            ts = p['total_seqs']
            si = p['seq_idx']
            kind = 'data' if p['seq_type'] == 'D' else 'parity'
            entry = chunks_map.setdefault(ci, {'total_seqs': ts, 'data': {}, 'parity': {}})
            entry['total_seqs'] = max(entry['total_seqs'], ts)
            entry[kind].setdefault(si, []).append(p['payload'])

        reconstructed = []
        for ci in range(1, header['num_chunks'] + 1):
            entry = chunks_map.get(ci)
            if not entry:
                # chunk manquant
                continue

            # reconstruire le flux data/parity par ordre de seq_idx
            data_seq = ''.join(self._consensus(entry['data'][i]) for i in sorted(entry['data'].keys()))
            parity_seq = ''.join(self._consensus(entry['parity'][i]) for i in sorted(entry['parity'].keys()))

            try:
                data_bytes_rs = dna_to_bytes_goldman(data_seq, start='A') if data_seq else b""
                parity_bytes_rs = dna_to_bytes_goldman(parity_seq, start='A') if parity_seq else b""
            except Exception:
                # échec de mapping (transitions invalides)
                continue

            # RS decode
            try:
                decoded = rs.decode(data_bytes_rs + parity_bytes_rs)[0]
            except Exception:
                # fallback: données brutes sans RS
                decoded = data_bytes_rs

            if len(decoded) < 4:
                continue
            payload = decoded[:-4]
            crc_read = int.from_bytes(decoded[-4:], 'little')
            if (zlib.crc32(payload) & 0xFFFFFFFF) != crc_read:
                # CRC chunk invalide → on jette ce chunk
                continue

            reconstructed.append(payload)

        file_bytes = b''.join(reconstructed)
        # Tronquer à la taille exacte
        file_bytes = file_bytes[:header['file_size']]

        # Vérifier checksum global
        ok = hashlib.sha256(file_bytes).digest()[:8] == header['checksum8']
        return (ok, file_bytes if ok else b"")

    # ---------- Aides encodage/consensus/contraintes ----------

    def _encode_with_constraints(self, b: bytes) -> str:
        """Encode bytes→ADN avec Goldman et essaie différents 'start' pour satisfaire GC/run."""
        for start in ('A', 'C', 'G', 'T'):
            dna = bytes_to_dna_goldman(b, start=start)
            if passes_constraints(dna):
                return dna
        # Si rien ne passe, retourner la meilleure (la moins mauvaise) — ici la première
        return bytes_to_dna_goldman(b, start='A')

    @staticmethod
    def _consensus(reads: List[str]) -> str:
        if not reads:
            return ''
        if len(reads) == 1:
            return reads[0]
        L = max(len(s) for s in reads)
        out = []
        for i in range(L):
            col = [s[i] for s in reads if i < len(s)]
            if not col:
                continue
            out.append(Counter(col).most_common(1)[0][0])
        return ''.join(out)

    # ---------- I/O pratiques ----------

    @staticmethod
    def save_sequences(sequences: List[str], output_file: str):
        with open(output_file, 'w') as f:
            for s in sequences:
                f.write(s + "\n")

    @staticmethod
    def load_sequences(input_file: str) -> List[str]:
        out = []
        with open(input_file, 'r') as f:
            for line in f:
                s = line.strip().upper()
                if s and all(c in "ACGT" for c in s):
                    out.append(s)
        return out

