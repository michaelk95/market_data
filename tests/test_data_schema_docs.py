"""docs/data-schemas.md must match schema.py (issue #72).

Each table has a ``## `<table>` `` section whose column table lists exactly
the schema's data columns with their types, plus its dedup key and layout.
The bitemporal columns are documented once, in their own section.
"""

from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa
import pytest

from market_data.schema import (
    BITEMPORAL_COLUMNS,
    DEDUP_KEYS,
    PARTITION_COLS,
    TABLE_SCHEMAS,
)

DOC = Path(__file__).resolve().parents[1] / "docs" / "data-schemas.md"

# Arrow type names as the doc spells them.
_TYPE_NAMES = {
    "double": "float64",
    "date32[day]": "date32",
    "timestamp[us, tz=UTC]": "timestamp[us, UTC]",
}

_ROW = re.compile(r"^\| `(?P<name>[a-z_]+)` \| (?P<type>[^|]+?) \|", re.MULTILINE)


def _type_name(field: pa.Field) -> str:
    return _TYPE_NAMES.get(str(field.type), str(field.type))


def _section(heading: str) -> str:
    text = DOC.read_text(encoding="utf-8")
    match = re.search(rf"^## {re.escape(heading)}\n(.*?)(?=^## |\Z)", text, re.MULTILINE | re.DOTALL)
    assert match, f"docs/data-schemas.md has no '## {heading}' section"
    return match.group(1)


def _documented_columns(section: str) -> dict[str, str]:
    return {m["name"]: m["type"] for m in _ROW.finditer(section)}


@pytest.mark.parametrize("table", sorted(TABLE_SCHEMAS))
def test_table_columns_documented(table: str) -> None:
    schema = TABLE_SCHEMAS[table]
    expected = {f.name: _type_name(f) for f in schema if f.name not in BITEMPORAL_COLUMNS}

    assert _documented_columns(_section(f"`{table}`")) == expected


@pytest.mark.parametrize("table", sorted(TABLE_SCHEMAS))
def test_table_dedup_key_and_layout_documented(table: str) -> None:
    section = _section(f"`{table}`")
    keys = ", ".join(f"`{k}`" for k in DEDUP_KEYS[table])
    path = f"data/{table}/year=YYYY/data.parquet" if PARTITION_COLS[table] else f"data/{table}/data.parquet"

    assert f"**Dedup key:** {keys}" in section
    assert f"`{path}`" in section


def test_bitemporal_columns_documented() -> None:
    fields = TABLE_SCHEMAS["ohlcv"]
    expected = {c: _type_name(fields.field(c)) for c in BITEMPORAL_COLUMNS}

    assert _documented_columns(_section("Bitemporal columns")) == expected
