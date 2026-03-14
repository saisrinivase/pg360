#!/usr/bin/env python3
"""Lightweight presentation QA for PG360 HTML reports.

This is not a pixel-perfect visual test. It approximates a human scan by
flagging content density and wording patterns that commonly lead to cramped
cards, unclear labels, or visually noisy report sections.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import List


MAX_EXEC_LABEL = 24
MAX_EXEC_VALUE = 22
MAX_EXEC_SUB = 72
MAX_INDEX_TITLE = 34
MAX_INDEX_DESC = 96
MAX_UNBROKEN_TOKEN = 28


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def longest_token(text: str) -> int:
    parts = re.findall(r"[^\s/,_-]+", text or "")
    return max((len(p) for p in parts), default=0)


@dataclass
class CardRecord:
    classes: List[str]
    label: str = ""
    value: str = ""
    sub: str = ""
    warnings: List[str] = field(default_factory=list)


@dataclass
class IndexCardRecord:
    title: str = ""
    desc: str = ""
    warnings: List[str] = field(default_factory=list)


class PG360QAParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.stack: List[dict] = []
        self.exec_cards: List[CardRecord] = []
        self.index_cards: List[IndexCardRecord] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs = dict(attrs)
        entry = {"tag": tag, "attrs": attrs, "text": ""}
        self.stack.append(entry)

        if tag in {"div", "span"}:
            classes = attrs.get("class", "").split()
            if tag == "div" and "card" in classes:
                entry["card"] = CardRecord(classes=classes)
            if "card-label" in classes:
                entry["role"] = "card-label"
            elif "card-value" in classes:
                entry["role"] = "card-value"
            elif "card-sub" in classes:
                entry["role"] = "card-sub"
            elif "idx-title" in classes:
                entry["role"] = "idx-title"
            elif "idx-desc" in classes:
                entry["role"] = "idx-desc"

        if tag == "a" and "index-card" in attrs.get("class", "").split():
            entry["index_card"] = IndexCardRecord()

    def handle_data(self, data: str) -> None:
        if self.stack:
            self.stack[-1]["text"] += data

    def handle_endtag(self, tag: str) -> None:
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i]["tag"] == tag:
                entry = self.stack.pop(i)
                text = normalize_ws(entry.get("text", ""))
                role = entry.get("role")

                parent_card = self._find_nearest("card")
                parent_index = self._find_nearest("index_card")

                if role == "card-label" and parent_card is not None:
                    parent_card.label = text
                elif role == "card-value" and parent_card is not None:
                    parent_card.value = text
                elif role == "card-sub" and parent_card is not None:
                    parent_card.sub = text
                elif role == "idx-title" and parent_index is not None:
                    parent_index.title = text
                elif role == "idx-desc" and parent_index is not None:
                    parent_index.desc = text

                if "card" in entry:
                    card = entry["card"]
                    if "executive-grid" in card.classes or self._inside_executive_grid(entry):
                        self.exec_cards.append(card)

                if "index_card" in entry:
                    self.index_cards.append(entry["index_card"])
                break

    def _find_nearest(self, key: str):
        for item in reversed(self.stack):
            if key in item:
                return item[key]
        return None

    def _inside_executive_grid(self, entry: dict) -> bool:
        classes = entry.get("attrs", {}).get("class", "").split()
        if "executive-grid" in classes:
            return True
        for item in reversed(self.stack):
            cls = item.get("attrs", {}).get("class", "").split()
            if "executive-grid" in cls:
                return True
        return False


def analyze_exec_cards(cards: List[CardRecord]) -> List[str]:
    findings: List[str] = []
    for idx, card in enumerate(cards, start=1):
        if len(card.label) > MAX_EXEC_LABEL:
            card.warnings.append(f"label {len(card.label)} chars")
        if len(card.value) > MAX_EXEC_VALUE:
            card.warnings.append(f"value {len(card.value)} chars")
        if len(card.sub) > MAX_EXEC_SUB:
            card.warnings.append(f"subtext {len(card.sub)} chars")
        if longest_token(card.label) > MAX_UNBROKEN_TOKEN:
            card.warnings.append("label has long unbroken token")
        if longest_token(card.value) > MAX_UNBROKEN_TOKEN:
            card.warnings.append("value has long unbroken token")
        if not card.label or not card.value:
            card.warnings.append("missing label or value")
        if card.label and card.sub and card.label.lower() in card.sub.lower():
            card.warnings.append("subtext repeats label")

        if card.warnings:
            findings.append(
                f"Executive card {idx}: {card.label or '(blank)'} -> " + ", ".join(card.warnings)
            )
    return findings


def analyze_index_cards(cards: List[IndexCardRecord]) -> List[str]:
    findings: List[str] = []
    for idx, card in enumerate(cards, start=1):
        if len(card.title) > MAX_INDEX_TITLE:
            card.warnings.append(f"title {len(card.title)} chars")
        if len(card.desc) > MAX_INDEX_DESC:
            card.warnings.append(f"description {len(card.desc)} chars")
        if longest_token(card.title) > MAX_UNBROKEN_TOKEN:
            card.warnings.append("title has long unbroken token")
        if not card.title:
            card.warnings.append("missing title")
        if card.warnings:
            findings.append(
                f"Index card {idx}: {card.title or '(blank)'} -> " + ", ".join(card.warnings)
            )
    return findings


def default_report_path() -> Path:
    report_root = Path("/Users/saiendla/Desktop/pg360/reports/latest")
    preferred = report_root / "pg360_latest.html"
    if preferred.exists():
        return preferred

    candidates = sorted(
        report_root.glob("pg360_*.html"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if candidates:
        return candidates[0]

    return report_root / "pg360_latest.html"


def main() -> int:
    ap = argparse.ArgumentParser(description="PG360 presentation QA")
    ap.add_argument(
        "html",
        nargs="?",
        default=str(default_report_path()),
        help="Path to generated PG360 HTML report",
    )
    args = ap.parse_args()

    html_path = Path(args.html)
    if not html_path.exists():
        print(f"ERROR: report not found: {html_path}")
        return 2

    parser = PG360QAParser()
    parser.feed(html_path.read_text(encoding="utf-8", errors="replace"))

    exec_findings = analyze_exec_cards(parser.exec_cards)
    index_findings = analyze_index_cards(parser.index_cards)

    print(f"REPORT: {html_path}")
    print(f"EXECUTIVE_CARDS: {len(parser.exec_cards)}")
    print(f"INDEX_CARDS: {len(parser.index_cards)}")
    print(f"EXEC_WARNINGS: {len(exec_findings)}")
    print(f"INDEX_WARNINGS: {len(index_findings)}")

    if exec_findings:
        print("\n[Executive cards]")
        for item in exec_findings:
            print(f"- {item}")

    if index_findings:
        print("\n[Index cards]")
        for item in index_findings[:20]:
            print(f"- {item}")
        if len(index_findings) > 20:
            print(f"- ... {len(index_findings) - 20} more")

    if not exec_findings and not index_findings:
        print("\nPASS: No presentation-density warnings triggered by current thresholds.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
