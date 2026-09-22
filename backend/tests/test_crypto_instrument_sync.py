"""crypto 标的维表合并写入测试（app.services.instrument_sync.sync_crypto_instruments）。

关键点：crypto 走币安插件独立拉取，必须**合并**进 instruments.parquet，
不能像 sync_instruments 那样全量覆盖（否则会删掉 cn/hk/us 行）。
"""
from __future__ import annotations

import polars as pl

from app.services import instrument_sync


def _crypto_item(symbol: str, name: str) -> dict:
    return {
        "symbol": symbol, "name": name, "code": name,
        "exchange": "BINANCE", "region": "crypto", "type": "stock",
        "ext": {"listing_date": None, "total_shares": None, "float_shares": None,
                "tick_size": 0.01, "limit_up": None, "limit_down": None},
    }


def _stock_row(symbol: str, market: str) -> dict:
    return {
        "symbol": symbol, "name": symbol, "code": symbol.split(".")[0],
        "exchange": "SH" if market == "cn" else "US", "region": market,
        "type": "stock", "market": market,
        "listing_date": None, "total_shares": None, "float_shares": None,
        "tick_size": None, "limit_up": None, "limit_down": None,
        "as_of": None,
    }


def test_merge_preserves_other_markets(tmp_path, monkeypatch):
    # 已有 cn/us 标的
    out = tmp_path / "instruments" / "instruments.parquet"
    out.parent.mkdir(parents=True)
    pl.DataFrame([_stock_row("600000.SH", "cn"), _stock_row("AAPL.US", "us")]
                 ).write_parquet(out)

    monkeypatch.setattr(instrument_sync, "_fetch_crypto_instruments",
                        lambda: [_crypto_item("BTCUSDT.CRYPTO", "BTC")])
    n = instrument_sync.sync_crypto_instruments(tmp_path)

    assert n == 1
    df = pl.read_parquet(out)
    assert set(df["market"].to_list()) == {"cn", "us", "crypto"}
    assert "BTCUSDT.CRYPTO" in df["symbol"].to_list()
    # crypto 行的 market 由 _market_of_exchange("BINANCE") 推导
    row = df.filter(pl.col("symbol") == "BTCUSDT.CRYPTO")
    assert row["market"].to_list() == ["crypto"]


def test_merge_replaces_stale_crypto_rows(tmp_path, monkeypatch):
    out = tmp_path / "instruments" / "instruments.parquet"
    out.parent.mkdir(parents=True)
    pl.DataFrame([
        _stock_row("600000.SH", "cn"),
        {**_stock_row("OLDEAD.CRYPTO", "crypto"), "exchange": "BINANCE"},
    ]).write_parquet(out)

    monkeypatch.setattr(instrument_sync, "_fetch_crypto_instruments",
                        lambda: [_crypto_item("ETHUSDT.CRYPTO", "ETH")])
    instrument_sync.sync_crypto_instruments(tmp_path)

    df = pl.read_parquet(out)
    assert "OLDEAD.CRYPTO" not in df["symbol"].to_list()  # 旧 crypto 行被剔除
    assert "ETHUSDT.CRYPTO" in df["symbol"].to_list()
    assert "600000.SH" in df["symbol"].to_list()


def test_failure_keeps_existing_data(tmp_path, monkeypatch):
    out = tmp_path / "instruments" / "instruments.parquet"
    out.parent.mkdir(parents=True)
    before = pl.DataFrame([_stock_row("600000.SH", "cn")])
    before.write_parquet(out)

    def _boom():
        raise ConnectionError("binance unreachable")

    monkeypatch.setattr(instrument_sync, "_fetch_crypto_instruments", _boom)
    n = instrument_sync.sync_crypto_instruments(tmp_path)

    assert n == 0
    assert pl.read_parquet(out).equals(before)  # fail-closed：旧数据原样保留


def test_empty_result_keeps_existing_data(tmp_path, monkeypatch):
    out = tmp_path / "instruments" / "instruments.parquet"
    out.parent.mkdir(parents=True)
    before = pl.DataFrame([_stock_row("600000.SH", "cn")])
    before.write_parquet(out)

    monkeypatch.setattr(instrument_sync, "_fetch_crypto_instruments", lambda: [])
    assert instrument_sync.sync_crypto_instruments(tmp_path) == 0
    assert pl.read_parquet(out).equals(before)


def test_first_sync_creates_file(tmp_path, monkeypatch):
    monkeypatch.setattr(instrument_sync, "_fetch_crypto_instruments",
                        lambda: [_crypto_item("BTCUSDT.CRYPTO", "BTC")])
    n = instrument_sync.sync_crypto_instruments(tmp_path)

    assert n == 1
    df = pl.read_parquet(tmp_path / "instruments" / "instruments.parquet")
    assert df["symbol"].to_list() == ["BTCUSDT.CRYPTO"]
    assert df["market"].to_list() == ["crypto"]
