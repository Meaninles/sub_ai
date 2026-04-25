from __future__ import annotations

import html
from dataclasses import dataclass

from .models import DigestCard, DigestChunk, PackedDigestChunk


MESSAGE_HARD_CAP = 3800
MAX_CHUNKS = 5
MAX_ITEMS_PER_DAY = 20


def build_card_text(card: DigestCard, index: int) -> str:
    safe_name = html.escape(card.project_name)
    safe_maturity = html.escape(card.maturity)
    safe_category = html.escape(card.category)
    safe_summary = html.escape(" ".join(card.summary.split())[:200])
    safe_url = html.escape(card.url, quote=True)
    return f"<b>{index}. {safe_name}</b> | {safe_maturity} | {safe_category} | <a href=\"{safe_url}\">打开</a>\n{safe_summary}"


def build_digest_chunks(cards: list[DigestCard]) -> tuple[list[PackedDigestChunk], list[DigestCard]]:
    ranked = cards[:MAX_ITEMS_PER_DAY]
    chunks: list[PackedDigestChunk] = []
    trimmed: list[DigestCard] = []
    current_lines: list[str] = []
    current_cards: list[DigestCard] = []
    current_items = 0
    chunk_index = 1

    for index, card in enumerate(ranked, start=1):
        card_text = build_card_text(card, index)
        trial_lines = [*current_lines, card_text] if not current_lines else [*current_lines, "", card_text]
        trial_text = "\n".join(trial_lines)
        if len(trial_text) > MESSAGE_HARD_CAP:
            if not current_lines:
                trimmed.append(card)
                continue
            chunks.append(
                PackedDigestChunk(
                    chunk=DigestChunk(chunk_index=chunk_index, text="\n".join(current_lines), item_count=current_items),
                    cards=current_cards[:],
                )
            )
            chunk_index += 1
            current_lines = [card_text]
            current_cards = [card]
            current_items = 1
            if chunk_index > MAX_CHUNKS + 1:
                trimmed.append(card)
                current_lines = []
                current_cards = []
                current_items = 0
        else:
            if current_lines:
                current_lines.append("")
            current_lines.append(card_text)
            current_cards.append(card)
            current_items += 1

    if current_lines and chunk_index <= MAX_CHUNKS:
        chunks.append(
            PackedDigestChunk(
                chunk=DigestChunk(chunk_index=chunk_index, text="\n".join(current_lines), item_count=current_items),
                cards=current_cards[:],
            )
        )

    if len(chunks) > MAX_CHUNKS:
        overflow = chunks[MAX_CHUNKS:]
        chunks = chunks[:MAX_CHUNKS]
        for overflow_chunk in overflow:
            trimmed.extend(overflow_chunk.cards)
    return chunks, trimmed
