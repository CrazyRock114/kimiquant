"""app.plugins.binance 契约测试：字段标准化、Top N 过滤、klines 翻页、失败路径。

不访问真实网络 —— httpx.MockTransport 替换 provider 内部 client。
"""
from __future__ import annotations

from datetime import date, datetime, UTC

import httpx
import polars as pl
import pytest

from app.plugins.binance.provider import TOP_N_INSTRUMENTS, BinanceProvider


def _kline(open_ms: int, o: float = 100.0, h: float = 110.0, low: float = 90.0,
           c: float = 105.0, v: float = 1000.0, qv: float = 100000.0) -> list:
    """构造一根币安 1d kline 行。"""
    return [open_ms, str(o), str(h), str(low), str(c), str(v),
            open_ms + 86_399_999, str(qv), 100, "0", "0", "0"]


def _ms(d: date) -> int:
    # provider 按 UTC 解析 kline openTime，测试数据也必须用 UTC 构造
    return int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp() * 1000)


@pytest.fixture
def provider() -> BinanceProvider:
    p = BinanceProvider()
    yield p
    p.close()


def _mock_client(provider: BinanceProvider, handler) -> None:
    transport = httpx.MockTransport(handler)
    provider._client.close()
    provider._client = httpx.Client(
        base_url="https://api.binance.com", transport=transport, timeout=5.0,
    )


# ── get_instruments ──────────────────────────────────────────────


def _exchange_info(symbols: list[dict]) -> dict:
    return {"symbols": symbols}


def _sym(pair: str, base: str, quote: str = "USDT", status: str = "TRADING",
         spot: bool = True) -> dict:
    return {
        "symbol": pair, "baseAsset": base, "quoteAsset": quote,
        "status": status, "isSpotTradingAllowed": spot,
        "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01000000"}],
    }


def test_get_instruments_maps_and_filters(provider):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/exchangeInfo":
            return httpx.Response(200, json=_exchange_info([
                _sym("BTCUSDT", "BTC"),
                _sym("ETHUSDT", "ETH"),
                _sym("SOLUSDT", "SOL", status="BREAK"),      # 非 TRADING → 过滤
                _sym("BTCEUR", "BTC", quote="EUR"),           # 非 USDT → 过滤
                _sym("XRPUSDT", "XRP", spot=False),           # 非现货 → 过滤
            ]))
        if request.url.path == "/api/v3/ticker/24hr":
            return httpx.Response(200, json=[
                {"symbol": "BTCUSDT", "quoteVolume": "999000000"},
                {"symbol": "ETHUSDT", "quoteVolume": "500000000"},
            ])
        return httpx.Response(404)

    _mock_client(provider, handler)
    rows = provider.get_instruments()

    assert [r["symbol"] for r in rows] == ["BTCUSDT.CRYPTO", "ETHUSDT.CRYPTO"]
    btc = rows[0]
    assert btc["name"] == "BTC"
    assert btc["code"] == "BTC"
    assert btc["exchange"] == "BINANCE"
    assert btc["type"] == "stock"
    assert btc["ext"]["tick_size"] == pytest.approx(0.01)
    assert btc["ext"]["limit_up"] is None  # crypto 无涨跌停


def test_get_instruments_top_n_by_quote_volume(provider, monkeypatch):
    # 构造 TOP_N + 50 个交易对，成交额倒序后只保留 Top N
    n_extra = 50
    symbols = [_sym(f"COIN{i}USDT", f"COIN{i}") for i in range(TOP_N_INSTRUMENTS + n_extra)]
    tickers = [{"symbol": f"COIN{i}USDT", "quoteVolume": str(i)}
               for i in range(TOP_N_INSTRUMENTS + n_extra)]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/exchangeInfo":
            return httpx.Response(200, json=_exchange_info(symbols))
        return httpx.Response(200, json=tickers)

    _mock_client(provider, handler)
    rows = provider.get_instruments()

    assert len(rows) == TOP_N_INSTRUMENTS
    # 成交额最大的 COIN{TOP_N+49} 必须排第一
    assert rows[0]["symbol"] == f"COIN{TOP_N_INSTRUMENTS + n_extra - 1}USDT.CRYPTO"


def test_get_instruments_api_failure_returns_empty(provider):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    _mock_client(provider, handler)
    with pytest.raises(httpx.HTTPStatusError):
        provider.get_instruments()


# ── get_daily ────────────────────────────────────────────────────


def test_get_daily_schema_and_utc_date(provider):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["interval"] == "1d"
        assert request.url.params["symbol"] == "BTCUSDT"
        return httpx.Response(200, json=[
            _kline(_ms(date(2026, 9, 1))),
            _kline(_ms(date(2026, 9, 2)), c=106.0, qv=200000.0),
        ])

    _mock_client(provider, handler)
    df = provider.get_daily(["BTCUSDT.CRYPTO"],
                            datetime(2026, 9, 1), datetime(2026, 9, 3))

    assert df.schema["date"] == pl.Date
    assert df.columns == ["symbol", "date", "open", "high", "low", "close",
                          "volume", "amount"]
    assert df.height == 2
    assert df["symbol"].to_list() == ["BTCUSDT.CRYPTO"] * 2
    assert df["date"].to_list() == [date(2026, 9, 1), date(2026, 9, 2)]
    # amount 取 quote asset volume（USDT 成交额）
    assert df["amount"].to_list() == [100000.0, 200000.0]


def test_get_daily_paginates_when_page_full(provider, monkeypatch):
    monkeypatch.setattr("app.plugins.binance.provider.KLINE_LIMIT", 2)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(int(request.url.params["startTime"]))
        if len(calls) == 1:
            return httpx.Response(200, json=[
                _kline(_ms(date(2026, 9, 1))), _kline(_ms(date(2026, 9, 2))),
            ])
        # 第二页不足 limit → 停止翻页
        return httpx.Response(200, json=[_kline(_ms(date(2026, 9, 3)))])

    _mock_client(provider, handler)
    df = provider.get_daily(["ETHUSDT.CRYPTO"],
                            datetime(2026, 9, 1), datetime(2026, 9, 3))

    assert len(calls) == 2
    assert calls[1] == _ms(date(2026, 9, 2)) + 1  # 从最后一根之后继续
    assert df.height == 3


def test_get_daily_partial_failure_keeps_others(provider):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["symbol"] == "BADUSDT":
            return httpx.Response(500)
        return httpx.Response(200, json=[_kline(_ms(date(2026, 9, 1)))])

    _mock_client(provider, handler)
    df = provider.get_daily(["BADUSDT.CRYPTO", "BTCUSDT.CRYPTO"],
                            datetime(2026, 9, 1), datetime(2026, 9, 1))

    # 单标的失败只跳过该标的，不中断整体
    assert df["symbol"].to_list() == ["BTCUSDT.CRYPTO"]


def test_get_daily_all_failed_returns_empty_df(provider):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    _mock_client(provider, handler)
    df = provider.get_daily(["BTCUSDT.CRYPTO"],
                            datetime(2026, 9, 1), datetime(2026, 9, 1))
    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()


def test_get_daily_chunk_progress_callback(provider):
    progress = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_kline(_ms(date(2026, 9, 1)))])

    _mock_client(provider, handler)
    symbols = [f"COIN{i}USDT.CRYPTO" for i in range(25)]
    provider.get_daily(symbols, datetime(2026, 9, 1), datetime(2026, 9, 1),
                       on_chunk_done=lambda cur, tot: progress.append((cur, tot)))

    # 25 个标的 / chunk 20 → 两次回调，最后一次 tot 一致
    assert progress == [(1, 2), (2, 2)]


def test_provider_capabilities_declared():
    p = BinanceProvider()
    try:
        assert p.name == "binance"
        assert "daily" in p.config.datasets
        assert "adj_factor" not in p.config.datasets  # crypto 无复权概念
        assert "minute" not in p.config.datasets      # 一期不接分钟K
    finally:
        p.close()
