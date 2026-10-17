"""Tests for scripts/repair_fundamentals_pit.py.

Fundamentals rows used to carry the latest EDGAR filing date as report_date,
although their values are a yfinance .info snapshot taken later. The repair
moves every row to its snapshot date so no row is visible before it was
collected.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest
import repair_fundamentals_pit as repair

from market_data.storage import read_table, write_table


def _row(
    symbol: str,
    report_date: date,
    collected_at: datetime,
    *,
    known: bool = True,
    market_cap: float = 1.0e9,
    total_revenue: float = 5.0e8,
    **extra,
) -> dict:
    return {
        "symbol": symbol,
        "market_cap": market_cap,
        "trailing_pe": 20.0,
        "total_revenue": total_revenue,
        "report_date_known": known,
        "period_start_date": report_date,
        "period_end_date": report_date,
        "report_date": report_date,
        "report_time_marker": "post-market",
        "source": "yfinance",
        "collected_at": collected_at,
        **extra,
    }


def _utc(y: int, m: int, d: int, h: int = 10) -> datetime:
    return datetime(y, m, d, h, tzinfo=timezone.utc)


def _bak(symbol: str, as_of: date, market_cap: float = 1.0e9, total_revenue: float = 5.0e8) -> dict:
    return {"symbol": symbol, "as_of": as_of, "market_cap": market_cap, "total_revenue": total_revenue}


_NO_BAK = pd.DataFrame(columns=["symbol", "as_of", "market_cap", "total_revenue"])


# ---------------------------------------------------------------------------
# repair_frame — pure transform
# ---------------------------------------------------------------------------


class TestRepairFrame:
    def test_filing_date_row_moves_to_collection_date(self):
        df = pd.DataFrame([_row("AAPL", date(2026, 7, 31), _utc(2026, 8, 18))])
        out = repair.repair_frame(df, _NO_BAK).iloc[0]
        assert out["report_date"] == date(2026, 8, 18)
        assert out["period_start_date"] == date(2026, 8, 18)
        assert out["period_end_date"] == date(2026, 8, 18)
        assert out["last_filing_date"] == date(2026, 7, 31)
        assert bool(out["report_date_known"]) is True

    def test_filing_on_collection_day_is_kept_as_last_filing_date(self):
        df = pd.DataFrame([_row("AAPL", date(2026, 8, 18), _utc(2026, 8, 18))])
        out = repair.repair_frame(df, _NO_BAK).iloc[0]
        assert out["report_date"] == date(2026, 8, 18)
        assert out["last_filing_date"] == date(2026, 8, 18)

    def test_unknown_row_keeps_collection_date_and_has_no_filing_date(self):
        df = pd.DataFrame([_row("ETF", date(2026, 8, 18), _utc(2026, 8, 18), known=False)])
        out = repair.repair_frame(df, _NO_BAK).iloc[0]
        assert out["report_date"] == date(2026, 8, 18)
        assert pd.isna(out["last_filing_date"])
        assert bool(out["report_date_known"]) is True

    def test_migrated_row_uses_bak_as_of(self):
        df = pd.DataFrame([_row("AAMI", date(2026, 2, 27), _utc(2026, 4, 16, 3))])
        bak = pd.DataFrame([_bak("AAMI", date(2026, 4, 5))])
        out = repair.repair_frame(df, bak).iloc[0]
        assert out["report_date"] == date(2026, 4, 5)
        assert out["last_filing_date"] == date(2026, 2, 27)

    def test_migrated_unknown_row_uses_bak_as_of(self):
        df = pd.DataFrame([_row("XYZ", date(2026, 4, 5), _utc(2026, 4, 16, 3), known=False)])
        bak = pd.DataFrame([_bak("XYZ", date(2026, 4, 5))])
        out = repair.repair_frame(df, bak).iloc[0]
        assert out["report_date"] == date(2026, 4, 5)
        assert pd.isna(out["last_filing_date"])

    def test_bak_only_applies_to_rows_with_matching_values(self):
        # A later snapshot of the same symbol has different values: it must not
        # be pulled back to the .bak as_of date.
        df = pd.DataFrame([
            _row("AAMI", date(2026, 2, 27), _utc(2026, 4, 16, 3)),
            _row("AAMI", date(2026, 5, 7), _utc(2026, 6, 4), market_cap=2.5e9),
        ])
        bak = pd.DataFrame([_bak("AAMI", date(2026, 4, 5))])
        out = repair.repair_frame(df, bak).sort_values("report_date")
        assert list(out["report_date"]) == [date(2026, 4, 5), date(2026, 6, 4)]

    def test_bak_dates_only_the_earliest_matching_row(self):
        # A stale quote can repeat the exact .bak values in later snapshots;
        # those are later snapshots, not the migrated row.
        df = pd.DataFrame([
            _row("SWKH", date(2026, 2, 27), _utc(2026, 4, 16, 3)),
            _row("SWKH", date(2026, 3, 31), _utc(2026, 5, 5)),
        ])
        bak = pd.DataFrame([_bak("SWKH", date(2026, 4, 5))])
        out = repair.repair_frame(df, bak).sort_values("report_date")
        assert list(out["report_date"]) == [date(2026, 4, 5), date(2026, 5, 5)]

    def test_bak_never_moves_a_row_later_than_collection(self):
        # A .bak as_of after the collection date can't be the snapshot date.
        df = pd.DataFrame([_row("AAMI", date(2026, 2, 27), _utc(2026, 4, 16, 3))])
        bak = pd.DataFrame([_bak("AAMI", date(2026, 5, 1))])
        out = repair.repair_frame(df, bak).iloc[0]
        assert out["report_date"] == date(2026, 4, 16)

    def test_missing_bak_falls_back_to_collection_date(self):
        df = pd.DataFrame([_row("AAMI", date(2026, 2, 27), _utc(2026, 4, 16, 3))])
        out = repair.repair_frame(df, _NO_BAK).iloc[0]
        assert out["report_date"] == date(2026, 4, 16)

    def test_already_repaired_rows_are_unchanged(self):
        df = pd.DataFrame([
            _row("AAPL", date(2026, 7, 31), _utc(2026, 8, 18)),
            _row("ETF", date(2026, 8, 18), _utc(2026, 8, 18), known=False),
        ])
        once = repair.repair_frame(df, _NO_BAK)
        twice = repair.repair_frame(once, _NO_BAK)
        pd.testing.assert_frame_equal(once, twice)

    def test_new_style_rows_keep_their_last_filing_date(self):
        df = pd.DataFrame([
            _row("AAPL", date(2026, 7, 31), _utc(2026, 8, 18)),
            _row("AAPL", date(2026, 10, 23), _utc(2026, 10, 23), last_filing_date=date(2026, 10, 1)),
        ])
        out = repair.repair_frame(df, _NO_BAK).sort_values("report_date")
        assert list(out["report_date"]) == [date(2026, 8, 18), date(2026, 10, 23)]
        assert list(out["last_filing_date"]) == [date(2026, 7, 31), date(2026, 10, 1)]

    def test_other_columns_are_preserved(self):
        df = pd.DataFrame([_row("AAPL", date(2026, 7, 31), _utc(2026, 8, 18), analyst_count=12.0)])
        out = repair.repair_frame(df, _NO_BAK).iloc[0]
        assert out["analyst_count"] == 12.0
        assert out["market_cap"] == 1.0e9


class TestCheckInvariants:
    def test_rejects_report_date_before_collection(self):
        df = repair.repair_frame(
            pd.DataFrame([_row("AAPL", date(2026, 7, 31), _utc(2026, 8, 18))]), _NO_BAK
        )
        df.loc[0, "report_date"] = date(2026, 8, 1)
        with pytest.raises(ValueError, match="report_date"):
            repair.check_invariants(df)

    def test_rejects_duplicate_keys(self):
        df = pd.DataFrame([
            _row("AAPL", date(2026, 7, 31), _utc(2026, 8, 18, 10)),
            _row("AAPL", date(2026, 8, 18), _utc(2026, 8, 18, 11), known=False),
        ])
        with pytest.raises(ValueError, match="duplicate"):
            repair.check_invariants(repair.repair_frame(df, _NO_BAK))


# ---------------------------------------------------------------------------
# run — whole-table rewrite
# ---------------------------------------------------------------------------


def _seed(data_dir) -> None:
    df = pd.DataFrame([
        _row("OLD", date(2022, 5, 16), _utc(2026, 4, 16, 3)),
        _row("AAMI", date(2026, 2, 27), _utc(2026, 4, 16, 3)),
        _row("AAPL", date(2026, 7, 31), _utc(2026, 8, 18)),
        _row("AAPL", date(2026, 5, 1), _utc(2026, 5, 5)),
    ])
    write_table(df, "fundamentals", data_dir)
    bak_dir = data_dir / "fundamentals"
    for symbol in ("OLD", "AAMI"):
        pd.DataFrame([_bak(symbol, date(2026, 4, 5))]).to_parquet(
            bak_dir / f"{symbol}.parquet.bak", index=False
        )


def _snapshot_files(data_dir) -> dict[str, bytes]:
    root = data_dir / "fundamentals"
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


class TestRun:
    def test_dry_run_is_default_and_writes_nothing(self, tmp_path):
        _seed(tmp_path)
        before = _snapshot_files(tmp_path)
        summary = repair.run(data_dir=tmp_path)
        assert _snapshot_files(tmp_path) == before
        assert summary["rows"] == 4
        assert summary["rows_moved"] == 4
        assert not list(tmp_path.glob("fundamentals_pit_backup_*"))

    def test_apply_rewrites_partitions(self, tmp_path):
        _seed(tmp_path)
        repair.run(data_dir=tmp_path, apply=True)

        assert not (tmp_path / "fundamentals" / "year=2022").exists()
        df = read_table("fundamentals", tmp_path).sort_values(["symbol", "report_date"])
        assert len(df) == 4
        assert list(zip(df["symbol"], df["report_date"])) == [
            ("AAMI", date(2026, 4, 5)),
            ("AAPL", date(2026, 5, 5)),
            ("AAPL", date(2026, 8, 18)),
            ("OLD", date(2026, 4, 5)),
        ]
        assert (df["report_date"] == df["period_start_date"]).all()
        assert df["report_date_known"].all()

    def test_apply_backs_up_old_partitions(self, tmp_path):
        _seed(tmp_path)
        repair.run(data_dir=tmp_path, apply=True)
        backups = list(tmp_path.glob("fundamentals_pit_backup_*"))
        assert len(backups) == 1
        assert (backups[0] / "year=2022" / "data.parquet").exists()
        assert (backups[0] / "year=2026" / "data.parquet").exists()

    def test_apply_is_idempotent(self, tmp_path):
        _seed(tmp_path)
        repair.run(data_dir=tmp_path, apply=True)
        first = read_table("fundamentals", tmp_path)
        summary = repair.run(data_dir=tmp_path, apply=True)
        assert summary["rows_moved"] == 0
        pd.testing.assert_frame_equal(first, read_table("fundamentals", tmp_path))

    def test_invariant_failure_writes_nothing(self, tmp_path):
        df = pd.DataFrame([
            _row("AAPL", date(2026, 7, 31), _utc(2026, 8, 18, 10)),
            _row("AAPL", date(2026, 8, 18), _utc(2026, 8, 18, 11), known=False),
        ])
        write_table(df, "fundamentals", tmp_path)
        before = _snapshot_files(tmp_path)
        with pytest.raises(ValueError):
            repair.run(data_dir=tmp_path, apply=True)
        assert _snapshot_files(tmp_path) == before

    def test_empty_table_is_a_no_op(self, tmp_path):
        summary = repair.run(data_dir=tmp_path, apply=True)
        assert summary["rows"] == 0
