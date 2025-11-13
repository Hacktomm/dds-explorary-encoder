from typing import List
from collections import Counter


def consensus(reads: List[str]) -> str:
    """Vote majoritaire par position. N'utilise pas de scores de qualité."""
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