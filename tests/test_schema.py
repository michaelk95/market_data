"""Tests for market_data.schema — bitemporal schema definitions."""

import datetime

import pandas as pd
import pyarrow as pa
import pytest

from market_data.schema import (
    BITEMPORAL_COLUMNS,
    DEDUP_KEYS,
    PARTITION_COLS,
    SORT_KEYS,
    TABLE_SCHEMAS,
    DataSource,
    ReportTimeMarker,
    validate_bitemporal_columns,
)

# ---------------------------------------------------------------------------
# Enum tests
# ---------------------------------------------------------------------------


class TestReportTimeMarker:
    def test_values(self):
        assert ReportTimeMarker.PRE_MARKET == "pre-market"
        assert ReportTimeMarker.DURING_HOURS == "during-hours"
        assert ReportTimeMarker.POST_MARKET == "post-market"

    def test_is_str_subclass(self):
        assert isinstance(ReportTimeMarker.PRE_MARKET, str)

    def test_all_three_variants_exist(self):
        assert len(ReportTimeMarker) == 3


class TestDataSource:
    def test_values(self):
        assert DataSource.YFINANCE == "yfinance"
        assert DataSource.FRED == "fred"
        assert DataSource.ISHARES == "ishares"

    def test_is_str_subclass(self):
        assert isinstance(DataSource.FRED, str)

    def test_all_three_variants_exist(self):
        assert len(DataSource) == 3


# ---------------------------------------------------------------------------
# Bitemporal columns list
# ---------------------------------------------------------------------------


class TestBitemporalColumns:
    def test_required_fields_present(self):
        required = {
            "period_start_date",
            "period_end_date",
            "report_date",
            "report_time_marker",
            "source",
            "collected_at",
        }
        assert required == set(BITEMPORAL_COLUMNS)

    def test_is_list(self):
        assert isinstance(BITEMPORAL_COLUMNS, list)


# ---------------------------------------------------------------------------
# TABLE_SCHEMAS
# ---------------------------------------------------------------------------


class TestTableSchemas:
    expected_tables = {"ohlcv", "indices", "fundamentals", "macro", "options"}

    def test_all_tables_present(self):
        assert set(TABLE_SCHEMAS) == self.expected_tables

    @pytest.mark.parametrize("table_name", ["ohlcv", "indices", "fundamentals", "macro", "options"])
    def test_each_schema_is_pyarrow(self, table_name):
        assert isinstance(TABLE_SCHEMAS[table_name], pa.Schema)

    @pytest.mark.parametrize("table_name", ["ohlcv", "indices", "fundamentals", "macro", "options"])
    def test_each_schema_contains_bitemporal_fields(self, table_name):
        schema = TABLE_SCHEMAS[table_name]
        field_names = schema.names
        for col in BITEMPORAL_COLUMNS:
            assert col in field_names, (
                f"Table '{table_name}' schema is missing bitemporal column '{col}'"
            )

    def test_ohlcv_has_ohlcv_columns(self):
        names = TABLE_SCHEMAS["ohlcv"].names
        for col in ("symbol", "open", "high", "low", "close", "volume"):
            assert col in names

    def test_fundamentals_has_analyst_columns(self):
        names = TABLE_SCHEMAS["fundamentals"].names
        for col in ("analyst_target_mean", "analyst_recommendation", "analyst_count"):
            assert col in names

    def test_macro_has_series_id_and_value(self):
        names = TABLE_SCHEMAS["macro"].names
        assert "series_id" in names
        assert "value" in names

    def test_options_has_contract_columns(self):
        names = TABLE_SCHEMAS["options"].names
        for col in ("expiry", "strike", "option_type", "implied_vol", "in_the_money"):
            assert col in names

    def test_collected_at_is_utc_timestamp(self):
        for table_name, schema in TABLE_SCHEMAS.items():
            field = schema.field("collected_at")
            assert pa.types.is_timestamp(field.type), table_name
            assert field.type.tz == "UTC", table_name

    def test_date_fields_are_date32(self):
        date_fields = ("period_start_date", "period_end_date", "report_date")
        for table_name, schema in TABLE_SCHEMAS.items():
            for fname in date_fields:
                field = schema.field(fname)
                assert field.type == pa.date32(), (
                    f"{table_name}.{fname} should be date32"
                )


# ---------------------------------------------------------------------------
# Lookup dictionaries completeness
# ---------------------------------------------------------------------------


class TestLookupDictionaries:
    all_tables = {"ohlcv", "indices", "fundamentals", "macro", "options"}

    def test_dedup_keys_covers_all_tables(self):
        assert set(DEDUP_KEYS) == self.all_tables

    def test_sort_keys_covers_all_tables(self):
        assert set(SORT_KEYS) == self.all_tables

    def test_partition_cols_covers_all_tables(self):
        assert set(PARTITION_COLS) == self.all_tables

    @pytest.mark.parametrize("table_name", ["ohlcv", "indices", "fundamentals", "macro", "options"])
    def test_dedup_keys_are_lists_of_strings(self, table_name):
        keys = DEDUP_KEYS[table_name]
        assert isinstance(keys, list)
        assert all(isinstance(k, str) for k in keys)
        assert len(keys) > 0

    @pytest.mark.parametrize("table_name", ["ohlcv", "indices", "fundamentals", "macro", "options"])
    def test_dedup_keys_are_valid_schema_columns(self, table_name):
        schema_names = set(TABLE_SCHEMAS[table_name].names)
        for key in DEDUP_KEYS[table_name]:
            assert key in schema_names, (
                f"DEDUP_KEYS['{table_name}'] references unknown column '{key}'"
            )

    def test_partitioned_tables(self):
        partitioned = {t for t, cols in PARTITION_COLS.items() if cols}
        assert partitioned == {"ohlcv", "fundamentals", "options"}

    def test_non_partitioned_tables(self):
        non_partitioned = {t for t, cols in PARTITION_COLS.items() if not cols}
        assert non_partitioned == {"indices", "macro"}

    def test_ohlcv_dedup_includes_symbol_and_period(self):
        assert "symbol" in DEDUP_KEYS["ohlcv"]
        assert "period_start_date" in DEDUP_KEYS["ohlcv"]

    def test_macro_dedup_uses_series_id(self):
        assert "series_id" in DEDUP_KEYS["macro"]
        assert "period_start_date" in DEDUP_KEYS["macro"]

    def test_options_dedup_is_contract_key(self):
        keys = set(DEDUP_KEYS["options"])
        assert {"symbol", "period_start_date", "expiry", "strike", "option_type"} == keys


# ---------------------------------------------------------------------------
# validate_bitemporal_columns
# ---------------------------------------------------------------------------


def _make_bitemporal_df(**overrides) -> pd.DataFrame:
    """Build a minimal one-row DataFrame with all bitemporal columns."""
    base = {
        "period_start_date": datetime.date(2024, 1, 2),
        "period_end_date": datetime.date(2024, 1, 2),
        "report_date": datetime.date(2024, 1, 3),
        "report_time_marker": ReportTimeMarker.POST_MARKET,
        "source": DataSource.YFINANCE,
        "collected_at": pd.Timestamp("2024-01-03 21:00:00", tz="UTC"),
    }
    base.update(overrides)
    return pd.DataFrame([base])


class TestValidateBitemporalColumns:
    def test_valid_df_passes(self):
        df = _make_bitemporal_df()
        validate_bitemporal_columns(df)  # should not raise

    def test_extra_columns_pass(self):
        df = _make_bitemporal_df(symbol="AAPL", close=185.0)
        validate_bitemporal_columns(df)  # extra columns are fine

    @pytest.mark.parametrize("missing_col", BITEMPORAL_COLUMNS)
    def test_missing_column_raises(self, missing_col):
        df = _make_bitemporal_df()
        df = df.drop(columns=[missing_col])
        with pytest.raises(ValueError, match=missing_col):
            validate_bitemporal_columns(df)

    def test_error_message_lists_all_missing(self):
        df = pd.DataFrame([{"symbol": "AAPL"}])
        with pytest.raises(ValueError) as exc_info:
            validate_bitemporal_columns(df)
        msg = str(exc_info.value)
        for col in BITEMPORAL_COLUMNS:
            assert col in msg

    def test_non_dataframe_raises_type_error(self):
        with pytest.raises(TypeError):
            validate_bitemporal_columns({"period_start_date": "2024-01-02"})  # type: ignore[arg-type]

    def test_empty_dataframe_with_correct_columns_passes(self):
        df = pd.DataFrame(columns=BITEMPORAL_COLUMNS)
        validate_bitemporal_columns(df)
