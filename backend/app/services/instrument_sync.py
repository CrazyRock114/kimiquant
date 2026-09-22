"""标的维表同步服务。

盘前 9:10 调用 tf.exchanges.get_instruments("SH"/"SZ"/"BJ", type="stock")
获取全量标的元数据，flatten ext 字段，写入 instruments.parquet。

Starter+ 盘后可用 quotes.get(universes) 顺便补充 name。
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from app.tickflow.client import get_client

logger = logging.getLogger(__name__)

# A 股三个交易所; 港美股按市场注册表（见 app.markets）
_EXCHANGES = ["SH", "SZ", "BJ"]

# 市场 → TickFlow 交易所（多市场扩展）
_MARKET_EXCHANGES: dict[str, list[str]] = {
    "cn": ["SH", "SZ", "BJ"],
    "hk": ["HK"],
    "us": ["US"],
    "crypto": ["BINANCE"],
}


def _market_of_exchange(exchange: str) -> str:
    for market, exchanges in _MARKET_EXCHANGES.items():
        if exchange in exchanges:
            return market
    return "cn"


def _flatten_instruments(items: list[dict]) -> list[dict]:
    """把 SDK 返回的 Instrument 列表 flatten 成扁平行。"""
    rows = []
    for item in items:
        row = {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "code": item.get("code"),
            "exchange": item.get("exchange"),
            "region": item.get("region"),
            "type": item.get("type"),
        }
        row["market"] = _market_of_exchange(item.get("exchange") or "")
        ext = item.get("ext") or {}
        row["listing_date"] = ext.get("listing_date")
        row["total_shares"] = ext.get("total_shares")
        row["float_shares"] = ext.get("float_shares")
        row["tick_size"] = ext.get("tick_size")
        row["limit_up"] = ext.get("limit_up")
        row["limit_down"] = ext.get("limit_down")
        rows.append(row)
    return rows


def _fetch_instruments_via_provider() -> list[dict] | None:
    """若当前日K数据源不是 tickflow 且该 provider 提供 get_instruments, 用它拉标的维表。

    返回 flatten 行列表; 未命中(仍应走 tickflow)时返回 None。
    标的维表跟随日K数据源(二者天然耦合, 无独立偏好项)。
    """
    from app.services import preferences

    provider_name = preferences.get_daily_data_provider()
    if provider_name == "tickflow":
        return None
    from app.data_providers import custom as custom_sources

    if not custom_sources.is_custom_provider(provider_name):
        return None
    provider = custom_sources.get_provider(provider_name)
    if not hasattr(provider, "get_instruments"):
        return None
    try:
        items = provider.get_instruments("stock") or []
    except Exception as e:  # noqa: BLE001
        logger.warning("provider %s get_instruments 失败: %s", provider_name, e)
        return None
    rows = _flatten_instruments(items)
    logger.info("instruments via %s: %d stocks", provider_name, len(rows))
    return rows


def sync_instruments(data_dir: Path, markets: list[str] | None = None) -> int:
    """全量同步标的维表 → data/instruments/instruments.parquet。

    markets: 指定同步哪些市场（cn/hk/us/crypto），缺省同步全部市场。
    返回写入的行数。
    """
    from app.markets import ALL_MARKETS, get_market

    if markets is None:
        markets = list(ALL_MARKETS)

    all_rows = _fetch_instruments_via_provider()
    if all_rows is None:
        # 未命中非 tickflow provider → 走 tickflow 直连
        tf = get_client()
        all_rows = []
        for market in markets:
            if market == "crypto":
                # crypto 不走 TickFlow：币安插件公开接口（失败只跳过，不影响其他市场）
                try:
                    items = _fetch_crypto_instruments()
                    if items:
                        all_rows.extend(_flatten_instruments(items))
                        logger.info("instruments crypto (BINANCE): %d pairs", len(items))
                except Exception as e:  # noqa: BLE001
                    logger.warning("crypto instruments failed: %s", e)
                continue
            for ex in get_market(market).exchanges:
                try:
                    items = tf.exchanges.get_instruments(ex, instrument_type="stock")
                    if items:
                        all_rows.extend(_flatten_instruments(items))
                        logger.info("instruments %s (%s): %d stocks", ex, market, len(items))
                except Exception as e:
                    logger.warning("get_instruments(%s) failed: %s", ex, e)

    if not all_rows:
        return 0

    df = pl.DataFrame(all_rows)
    df = df.with_columns(pl.lit(date.today()).alias("as_of"))

    out = data_dir / "instruments" / "instruments.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)

    logger.info("instruments synced: %d rows → %s", df.height, out)
    return df.height


def enrich_names_from_quotes(
    data_dir: Path,
    quotes_data: list[dict],
) -> int:
    """从 quotes 响应中提取 name，更新 instruments 维表（兜底补充）。

    盘后 quotes.get(universes) 返回的数据中包含 ext.name，
    用来补充 instruments 中可能缺失的 name。
    """
    if not quotes_data:
        return 0

    # 构建 symbol → name 映射
    name_map: dict[str, str] = {}
    for q in quotes_data:
        symbol = q.get("symbol", "")
        ext = q.get("ext") or {}
        name = ext.get("name") or q.get("name", "")
        if symbol and name:
            name_map[symbol] = name

    if not name_map:
        return 0

    inst_path = data_dir / "instruments" / "instruments.parquet"
    if not inst_path.exists():
        return 0

    df = pl.read_parquet(inst_path)

    # 只更新空 name 的行
    updates = pl.DataFrame({
        "symbol": list(name_map.keys()),
        "_new_name": list(name_map.values()),
    })
    df = df.join(updates, on="symbol", how="left")
    df = df.with_columns(
        pl.when(pl.col("name").is_null() | (pl.col("name") == ""))
        .then(pl.col("_new_name"))
        .otherwise(pl.col("name"))
        .alias("name"),
    ).drop("_new_name")

    df.write_parquet(inst_path)
    logger.info("instruments name enriched from quotes: %d names", len(name_map))
    return len(name_map)

def _fetch_crypto_instruments() -> list[dict]:
    """从币安插件拉取 crypto 标的维表（USDT 现货 Top N，见 plugins/binance）。"""
    from app.plugins.binance.provider import BinanceProvider

    provider = BinanceProvider()
    try:
        return provider.get_instruments("stock") or []
    finally:
        provider.close()


def sync_crypto_instruments(data_dir: Path) -> int:
    """同步 crypto 标的维表并**合并**进 instruments.parquet（不覆盖其他市场）。

    sync_instruments 是全市场覆盖写；crypto 走币安插件独立拉取，
    读取现有 parquet → 剔除旧 crypto 行 → 追加新行 → 写回。
    返回 crypto 行数；币安不可达或无数据时返回 0 并保留旧数据（fail-closed）。
    """
    try:
        items = _fetch_crypto_instruments()
    except Exception as e:  # noqa: BLE001
        logger.warning("crypto instruments 同步失败: %s", e)
        return 0
    if not items:
        return 0

    rows = _flatten_instruments(items)
    new = pl.DataFrame(rows).with_columns(pl.lit(date.today()).alias("as_of"))

    out = data_dir / "instruments" / "instruments.parquet"
    if out.exists():
        old = pl.read_parquet(out)
        if not old.is_empty() and "market" in old.columns:
            old = old.filter(pl.col("market") != "crypto")
        if not old.is_empty():
            new = pl.concat([old, new], how="diagonal_relaxed")

    out.parent.mkdir(parents=True, exist_ok=True)
    new.write_parquet(out)
    logger.info("crypto instruments synced: %d rows → %s (合并写入)", len(rows), out)
    return len(rows)
