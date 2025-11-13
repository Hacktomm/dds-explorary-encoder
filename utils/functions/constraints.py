
def gc_content(seq: str) -> float:
    if not seq:
        return 0.0
    gc = sum(1 for c in seq if c in "GC")
    return gc / len(seq)

def max_run_length(seq: str) -> int:
    if not seq:
        return 0
    m, cur, prev = 1, 1, seq[0]
    for c in seq[1:]:
        if c == prev:
            cur += 1
            m = max(m, cur)
        else:
            cur = 1
            prev = c
    return m

def passes_constraints(seq: str, max_run: int = 3, gc_min: float = 0.40, gc_max: float = 0.60) -> bool:
    if not seq:
        return False
    if max_run_length(seq) > max_run:
        return False
    gc = gc_content(seq)
    return gc_min <= gc <= gc_max