
def crc8(data: bytes, poly: int = 0x07, init: int = 0x00) -> int:
    """CRC-8 (polynôme x^8 + x^2 + x + 1 = 0x07)."""
    c = init
    for b in data:
        c ^= b
        for _ in range(8):
            c = ((c << 1) & 0xFF) ^ poly if (c & 0x80) else (c << 1) & 0xFF
    return c & 0xFF

def crc16_ccitt(data: bytes, poly: int = 0x1021, init: int = 0xFFFF) -> int:
    """CRC-16-CCITT (X25). Retourne un entier 0..65535."""
    crc = init & 0xFFFF
    for b in data:
        crc ^= (b << 8) & 0xFFFF
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ poly) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc & 0xFFFF