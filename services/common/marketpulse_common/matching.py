"""Entity matching from config/entities.yaml rules. Pure Python; no I/O.

Rules per entity (see EntityRule): strong_terms (one hit suffices), context_terms (count only when a
context_required term co-occurs), exclude_terms (any hit rejects the entity).
Term semantics:
  * "$AAPL"-style cashtag  -> case-insensitive, must not be preceded by a word char or '$'
  * ALL-CAPS token (AAPL, RIL, TCS) -> CASE-SENSITIVE whole-token match (so 'ril'/'tcs' in prose do not match)
  * anything else (names, phrases) -> case-insensitive, token-bounded
relevance score: 1.0 cashtag, 0.9 strong term, 0.6 context-qualified term.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable

SCORE_CASHTAG, SCORE_STRONG, SCORE_CONTEXT = 1.0, 0.9, 0.6


def _compile(term: str) -> re.Pattern[str]:
    t = term.strip()
    esc = re.escape(t)
    if t.startswith("$"):
        return re.compile(r"(?<![\w$])" + esc + r"(?!\w)", re.IGNORECASE)
    caps = t.replace(".", "").replace("&", "").replace(" ", "")
    flags = 0 if (caps.isupper() or (caps.isalnum() and caps == caps.upper() and any(c.isalpha() for c in caps))) and " " not in t else re.IGNORECASE
    return re.compile(r"(?<!\w)" + esc + r"(?!\w)", flags)


@dataclass(frozen=True)
class EntityMatch:
    entity_id: str
    score: float
    hits: tuple[str, ...] = field(default_factory=tuple)


class _Compiled:
    def __init__(self, rule: Any):
        self.strong = [(t, _compile(t), t.startswith("$")) for t in rule.strong_terms]
        self.context = [(t, _compile(t)) for t in rule.context_terms]
        self.required = [_compile(t) for t in rule.context_required]
        self.exclude = [_compile(t) for t in rule.exclude_terms]


class EntityMatcher:
    def __init__(self, rules: dict[str, Any]):
        self._rules = {eid: _Compiled(r) for eid, r in rules.items()}

    def match(self, text: str) -> list[EntityMatch]:
        if not text:
            return []
        out: list[EntityMatch] = []
        for eid, c in self._rules.items():
            if any(p.search(text) for p in c.exclude):
                continue
            hits: list[str] = []
            score = 0.0
            for term, pat, cashtag in c.strong:
                if pat.search(text):
                    hits.append(term)
                    score = max(score, SCORE_CASHTAG if cashtag else SCORE_STRONG)
            if c.context and any(p.search(text) for p in c.required):
                for term, pat in c.context:
                    if pat.search(text):
                        hits.append(term)
                        score = max(score, SCORE_CONTEXT)
            if hits:
                out.append(EntityMatch(eid, score, tuple(dict.fromkeys(hits))))
        return out


def matched_ids(matches: Iterable[EntityMatch]) -> list[str]:
    return sorted(m.entity_id for m in matches)
