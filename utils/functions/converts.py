from typing import List, Optional



def bits_to_bytes(bits: str) -> bytes:
    """Pack une chaîne de bits '0101...' en bytes."""
    if not bits:
        return b""
    pad = (-len(bits)) % 8
    bits_padded = bits + ("0" * pad)
    out = bytearray()
    for i in range(0, len(bits_padded), 8):
        out.append(int(bits_padded[i:i+8], 2))
    return bytes(out)

def bytes_to_bits(x: int, nbits: int) -> str:
    return format(x, f'0{nbits}b')

GOLDMAN_ENCODE = {
    'A': {0: 'C', 1: 'G', 2: 'T'},
    'C': {0: 'G', 1: 'T', 2: 'A'},
    'G': {0: 'T', 1: 'A', 2: 'C'},
    'T': {0: 'A', 1: 'C', 2: 'G'},
}
GOLDMAN_DECODE = {last: {nuc: val for val, nuc in mapping.items()}
                  for last, mapping in GOLDMAN_ENCODE.items()}

def bytes_to_trits(data: bytes) -> List[int]:
    """Chaque octet 0..255 → 6 trits (LSB → MSB en base 3)."""
    trits: List[int] = []
    for byte in data:
        v = int(byte)
        for _ in range(6):
            v, r = divmod(v, 3)
            trits.append(r)
    return trits

def trits_to_bytes(trits: List[int], byte_length: Optional[int] = None) -> bytes:
    """Inverse strict de bytes_to_trits (ignore les trits incomplets)."""
    if not trits:
        return b""
    n = (len(trits) // 6) * 6
    out = bytearray()
    for i in range(0, n, 6):
        val = 0
        for j, t in enumerate(trits[i:i+6]):
            if t not in (0, 1, 2):
                raise ValueError("Trit invalide")
            val += t * (3 ** j)
        if not (0 <= val <= 255):
            raise ValueError(f"Pack 6-trits invalide → {val}")
        out.append(val)
    result = bytes(out)
    return result[:byte_length] if (byte_length is not None) else result

def trits_to_dna(trits: List[int], start: str = 'A') -> str:
    last = start
    dna = []
    for t in trits:
        nxt = GOLDMAN_ENCODE[last][t]
        dna.append(nxt)
        last = nxt
    return ''.join(dna)

def dna_to_trits(dna: str, start: str = 'A') -> List[int]:
    last = start
    trits: List[int] = []
    for base in dna:
        if base not in GOLDMAN_DECODE.get(last, {}):
            raise ValueError(f"Transition non valide: {last}->{base}")
        trits.append(GOLDMAN_DECODE[last][base])
        last = base
    return trits

def bytes_to_dna_goldman(data: bytes, start: str = 'A') -> str:
    return trits_to_dna(bytes_to_trits(data), start=start)

def dna_to_bytes_goldman(dna: str, start: str = 'A') -> bytes:
    return trits_to_bytes(dna_to_trits(dna, start=start))