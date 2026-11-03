"""CSV/spreadsheet formula-injection neutraliser (Ops/Data Check 17).

Backed-up M365 content is attacker-authored: an external sender controls an
email subject, a Teams display name, a group name, etc. When those strings are
written verbatim into an exported .csv and the operator opens it in Excel/
Sheets, a cell beginning ``=``/``+``/``-``/``@`` (or a tab/CR/LF) is evaluated
as a formula (``=HYPERLINK(...)`` exfil, legacy DDE, ``=WEBSERVICE(...)``).

``csv_safe`` prefixes a single quote to any cell whose first non-space char is a
formula trigger, and folds embedded CR/LF to spaces, so the value renders as
literal text. Non-string cells pass through unchanged.
"""
from __future__ import annotations

from typing import Any

_TRIGGERS = ("=", "+", "-", "@", "\t", "\r", "\n")


def csv_safe(value: Any) -> Any:
    """Neutralise a single spreadsheet cell. Only touches str values."""
    if not isinstance(value, str):
        return value
    v = value.replace("\r", " ").replace("\n", " ")
    if v[:1] in _TRIGGERS:
        return "'" + v
    return v


class SafeCsvWriter:
    """Wrap a ``csv.writer`` so every cell of every row is neutralised via
    ``csv_safe`` before being written. Drop-in for the ``.writerow`` /
    ``.writerows`` surface used by the export endpoints — one edit at the
    writer-creation site protects all rows, header rows included (constant
    headers are unaffected since they don't begin with a formula trigger)."""

    def __init__(self, writer):
        self._w = writer

    def writerow(self, row):
        self._w.writerow([csv_safe(c) for c in row])

    def writerows(self, rows):
        for r in rows:
            self.writerow(r)


class SafeDictWriter:
    """Wrap a ``csv.DictWriter`` so every cell value is neutralised via
    ``csv_safe``. Header is written as-is (field names are constants)."""

    def __init__(self, writer):
        self._w = writer

    def writeheader(self):
        self._w.writeheader()

    def writerow(self, row):
        self._w.writerow({k: csv_safe(v) for k, v in row.items()})

    def writerows(self, rows):
        for r in rows:
            self.writerow(r)
