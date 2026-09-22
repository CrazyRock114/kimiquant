"""币安公开行情数据源（crypto 市场）。

直连 Binance Spot 公开 REST API，无需 API Key：
  - 标的维表: GET /api/v3/exchangeInfo + /api/v3/ticker/24hr（按 24h 成交额取 Top N）
  - 日K:      GET /api/v3/klines?interval=1d（UTC 自然日分区）

口径说明：
  - symbol 统一为项目格式 BTCUSDT.CRYPTO（见 app.markets 市场注册表）。
  - date 取 kline openTime 的 UTC 日期（7x24 市场无交易日概念，UTC 自然日即分区日）。
  - amount 取 kline 的 quote asset volume（USDT 成交额），港美股恒为 0 的缺口在
    crypto 不存在，成交额类过滤器可正常工作。
  - 国内网络不可达时 httpx 自动读取 HTTPS_PROXY 环境变量走代理。
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, UTC
from typing import ClassVar

import httpx
import polars as pl

logger = logging.getLogger(__name__)

BASE_URL = os.environ.get("BINANCE_BASE_URL", "https://api.binance.com")
QUOTE_ASSET = "USDT"
# 只同步成交额 Top N 的现货交易对：全量 USDT 对逾千个，绝大多数无流动性，
# 全拉会拖垮盘后管道且无分析价值。可用 BINANCE_TOP_N 环境变量调整。
TOP_N_INSTRUMENTS = int(os.environ.get("BINANCE_TOP_N", "200"))
REQUEST_INTERVAL = 0.12  # ~8 req/s，远低于币安权重限速
KLINE_LIMIT = 1000       # /api/v3/klines 单页上限


class BinanceConfig:
    # key 是数据集名（provider_has_dataset 据此判断能力）
    datasets: ClassVar[dict] = {"daily": True}


class BinanceProvider:
    """Provider 契约对齐 GenericHTTPProvider（见 docs/plugin-development.md）。"""

    name = "binance"
    builtin = True

    def __init__(self) -> None:
        self.config = BinanceConfig()
        # trust_env 默认开启：HTTPS_PROXY/HTTP_PROXY 环境变量自动生效
        self._client = httpx.Client(
            base_url=BASE_URL, timeout=15.0,
            headers={"User-Agent": "tickflow-stock-panel"},
        )
        self._last_request = 0.0

    def close(self) -> None:
        self._client.close()

    # ---- 内部工具 ----

    def _get(self, path: str, params: dict | None = None, retries: int = 3):
        """带限速与指数退避的 GET。418/429（限速）与网络错误重试，其余直接抛。"""
        delay = 1.0
        for attempt in range(retries):
            # 简单匀速限速（进程内单实例使用，无需跨实例协调）
            wait = REQUEST_INTERVAL - (time.monotonic() - self._last_request)
            if wait > 0:
                time.sleep(wait)
            try:
                resp = self._client.get(path, params=params)
                self._last_request = time.monotonic()
                if resp.status_code in (418, 429):
                    retry_after = float(resp.headers.get("Retry-After", delay))
                    logger.warning("binance 限速 %s, %.1fs 后重试", resp.status_code, retry_after)
                    time.sleep(retry_after)
                    delay *= 2
                    continue
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as e:
                if attempt == retries - 1:
                    raise
                logger.warning("binance %s 第 %d 次失败: %s", path, attempt + 1, e)
                time.sleep(delay)
                delay *= 2
        return None  # pragma: no cover - 循环必然 return/raise

    @staticmethod
    def _pair_of(symbol: str) -> str:
        """BTCUSDT.CRYPTO → BTCUSDT（币安 API 交易对名）。"""
        s = symbol.strip().upper()
        return s[: -len(".CRYPTO")] if s.endswith(".CRYPTO") else s

    # ---- Provider 契约 ----

    def get_instruments(self, asset_type: str = "stock") -> list[dict]:
        """标的维表：USDT 现货 TRADING 对，按 24h 成交额取 Top N。

        返回 tickflow Instrument 形状的 dict 行，供 instrument_sync._flatten_instruments 复用。
        """
        info = self._get("/api/v3/exchangeInfo")
        tickers = self._get("/api/v3/ticker/24hr")
        if not isinstance(info, dict) or not isinstance(tickers, list):
            return []

        # 24h 成交额（USDT 计价）用于流动性排序
        quote_vol: dict[str, float] = {}
        for t in tickers:
            sym = t.get("symbol")
            try:
                quote_vol[sym] = float(t.get("quoteVolume", 0) or 0)
            except (TypeError, ValueError):
                continue

        rows: list[dict] = []
        for s in info.get("symbols", []):
            if s.get("quoteAsset") != QUOTE_ASSET:
                continue
            if s.get("status") != "TRADING" or not s.get("isSpotTradingAllowed", True):
                continue
            pair = s.get("symbol")
            if not pair:
                continue
            tick_size = None
            for f in s.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    try:
                        tick_size = float(f["tickSize"])
                    except (KeyError, TypeError, ValueError):
                        tick_size = None
            rows.append({
                "symbol": f"{pair}.CRYPTO",
                "name": s.get("baseAsset") or pair,
                "code": s.get("baseAsset") or pair,
                "exchange": "BINANCE",
                "region": "crypto",
                "type": "stock",
                "ext": {
                    "listing_date": None,
                    "total_shares": None,
                    "float_shares": None,
                    "tick_size": tick_size,
                    "limit_up": None,
                    "limit_down": None,
                },
                "_quote_volume": quote_vol.get(pair, 0.0),
            })

        rows.sort(key=lambda r: r["_quote_volume"], reverse=True)
        rows = rows[:TOP_N_INSTRUMENTS]
        for r in rows:
            del r["_quote_volume"]
        logger.info("binance instruments: %d USDT 现货对 (Top %d)", len(rows), TOP_N_INSTRUMENTS)
        return rows

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime,
        end_time: datetime,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> pl.DataFrame:
        """批量日K。返回 canonical 列 [symbol, date, open, high, low, close, volume, amount]。

        date 为 UTC 自然日（pl.Date）。单标的失败只跳过该标的（保持旧数据），不中断整体。
        """
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        chunk_size = 20
        rows: list[dict] = []
        failed: list[str] = []

        for i, symbol in enumerate(symbols):
            pair = self._pair_of(symbol)
            try:
                rows.extend(self._fetch_klines(symbol, pair, start_ms, end_ms))
            except Exception as e:
                logger.warning("binance klines %s 拉取失败: %s", pair, e)
                failed.append(symbol)
            if on_chunk_done and ((i + 1) % chunk_size == 0 or i + 1 == len(symbols)):
                on_chunk_done((i + chunk_size) // chunk_size,
                              (len(symbols) + chunk_size - 1) // chunk_size)

        if failed:
            logger.warning("crypto 日K部分失败: %d/%d 标的未获取 (样例: %s)",
                           len(failed), len(symbols), failed[:10])
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def _fetch_klines(self, symbol: str, pair: str, start_ms: int, end_ms: int) -> list[dict]:
        """单交易对翻页拉 1d klines（每页最多 KLINE_LIMIT 根）。"""
        out: list[dict] = []
        cursor = start_ms
        while cursor <= end_ms:
            batch = self._get("/api/v3/klines", params={
                "symbol": pair, "interval": "1d",
                "startTime": cursor, "endTime": end_ms, "limit": KLINE_LIMIT,
            })
            if not batch:
                break
            for k in batch:
                # kline: [openTime, open, high, low, close, volume(base), closeTime,
                #         quoteVolume(amount), trades, takerBuyBase, takerBuyQuote, ignore]
                out.append({
                    "symbol": symbol,
                    "date": datetime.fromtimestamp(k[0] / 1000, tz=UTC).date(),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "amount": float(k[7]),
                })
            if len(batch) < KLINE_LIMIT:
                break
            # 下一页从最后一根之后开始
            cursor = batch[-1][0] + 1
        return out
