"""撮合器市场规则测试: lot_size 传导 / 零碎成交 / T+0（审计 P0 修复回归）。

通过 BacktestEngine.simulate_portfolio + 合成 panel 直接驱动撮合,
验证 MatcherConfig 新增的 lot_size / allow_fractional / t_plus 生效,
且默认行为(cn: 100股整手 + T+1)完全不变。
"""
from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from app.backtest.engine import BacktestEngine, MatcherConfig


def _panel(symbols: list[str], days: int = 4, price: float = 10.0,
           overrides: dict[tuple[str, int], dict] | None = None) -> pl.DataFrame:
    overrides = overrides or {}
    start = date(2024, 1, 1)
    rows = []
    for sym in symbols:
        for i in range(days):
            patch = overrides.get((sym, i), {})
            rows.append({
                "symbol": sym, "name": sym,
                "date": start + timedelta(days=i),
                "open": patch.get("open", price),
                "high": patch.get("high", price),
                "low": patch.get("low", price),
                "close": patch.get("close", price),
                "volume": patch.get("volume", 100_000),
                "score": patch.get("score", 1),
                "signal_limit_up": False,
                "signal_limit_down": False,
            })
    return pl.DataFrame(rows).sort(["symbol", "date"])


def _mask(panel: pl.DataFrame, marks: set[tuple[str, int]]) -> pl.Series:
    base = date(2024, 1, 1)
    return pl.Series([
        (row["symbol"], (row["date"] - base).days) in marks
        for row in panel.select(["symbol", "date"]).iter_rows(named=True)
    ], dtype=pl.Boolean)


def _engine() -> BacktestEngine:
    return BacktestEngine(repo=None)


def _run(panel, entries, exits, cfg: MatcherConfig):
    return _engine().simulate_portfolio(panel, entries, exits, cfg)


# ── lot_size 传导 ──────────────────────────────────────────────


def test_default_lot_100_unchanged():
    """默认(cn 口径)仍按 100 股整手取整 — 既有行为不得变化。"""
    panel = _panel(["A"], days=2, price=7.0)
    result = _run(panel, _mask(panel, {("A", 0)}), _mask(panel, set()),
                  MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                                max_positions=1, initial_capital=100_000))
    assert len(result.trades) == 1
    shares = result.trades[0].shares
    # floor(100000/7/100)*100 = 14200
    assert shares == 14200.0
    assert result.trades[0].lots == shares / 100


def test_lot_size_from_config_respected():
    """lot_size=500 时按 500 取整(而非硬编码 100)。"""
    panel = _panel(["A"], days=2, price=7.0)
    result = _run(panel, _mask(panel, {("A", 0)}), _mask(panel, set()),
                  MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                                max_positions=1, initial_capital=100_000, lot_size=500))
    shares = result.trades[0].shares
    # floor(100000/7/500)*500 = 14000
    assert shares == 14000.0
    assert result.trades[0].lots == shares / 500


# ── 零碎成交（加密货币）───────────────────────────────────────


def test_fractional_allows_sub_unit_purchase():
    """高价币(110000)在小账户下不再因整手取整买不进 — 审计 P0-1 回归。"""
    panel = _panel(["BTC"], days=2, price=110_000.0)
    result = _run(panel, _mask(panel, {("BTC", 0)}), _mask(panel, set()),
                  MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                                max_positions=1, initial_capital=100_000,
                                lot_size=1, allow_fractional=True))
    assert len(result.trades) == 1
    # 100000 / 110000 ≈ 0.9091 BTC（引擎股数保留 4 位小数）
    assert result.trades[0].shares == pytest.approx(0.9091, abs=1e-4)


def test_fractional_blocked_below_min_notional():
    """零碎成交仍有最小名义金额(10)约束, 防止粉尘单。"""
    panel = _panel(["BTC"], days=2, price=110_000.0)
    result = _run(panel, _mask(panel, {("BTC", 0)}), _mask(panel, set()),
                  MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                                max_positions=1, initial_capital=5,
                                lot_size=1, allow_fractional=True))
    assert len(result.trades) == 0


def test_non_fractional_high_price_still_blocked():
    """对照: 非零碎模式下高价标的整手取整为 0 → 仍拒绝(行为未变)。"""
    panel = _panel(["BTC"], days=2, price=110_000.0)
    result = _run(panel, _mask(panel, {("BTC", 0)}), _mask(panel, set()),
                  MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                                max_positions=1, initial_capital=100_000,
                                lot_size=1, allow_fractional=False))
    # lot_size=1 时 floor(100000/110000/1)*1 = 0 → 拒绝
    assert len(result.trades) == 0


# ── T+0 / T+1 ─────────────────────────────────────────────────
# 注意: 日线引擎每日执行顺序为 风控 → 计划出场 → 建仓(engine.py:2741 注释),
# 当日新建仓位当日内不会再被风控检查 — 这与 T+N 无关, 是日线粒度的固有边界。
# t_plus 的实际差异点: ① T+1 拦截卖出当日回补(buy_same_day_reentry);
# ② 风控循环对"当日新建仓位"的跳过在 T+0 下不再兜底(防御性条件)。


def test_t_plus_1_blocks_same_day_reentry():
    panel = _panel(["A"], days=3, price=10.0)
    entries = _mask(panel, {("A", 0), ("A", 1)})
    exits = _mask(panel, {("A", 1)})
    result = _run(panel, entries, exits,
                  MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                                max_positions=1, initial_capital=100_000, t_plus=1))
    assert len(result.trades) == 1  # 当日回补被拦截


def test_t_plus_0_allows_same_day_reentry():
    panel = _panel(["A"], days=3, price=10.0)
    entries = _mask(panel, {("A", 0), ("A", 1)})
    exits = _mask(panel, {("A", 1)})
    result = _run(panel, entries, exits,
                  MatcherConfig(matching="close_t", fees_pct=0, slippage_bps=0,
                                max_positions=1, initial_capital=100_000, t_plus=0))
    assert len(result.trades) == 2  # 卖出后当日再买回
