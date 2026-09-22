"""通达信行情协议数据源（pytdx，沪深京 A 股）。

直连通达信公开行情服务器（TCP 7709），免 Key：
  - 实时快照: get_security_quotes（沪深京批量，含五档买一/卖一）
  - 日K:       get_security_bars(category=9, 800 根/页)
  - 分钟K:     get_security_bars(category=8/0)
  - 除权因子:  get_xdxr_info → 按交易所除权公式换算 ex_factor
  - 标的维表:  get_security_list（按代码号段过滤 A 股）

口径说明（与项目数据契约对齐）:
  - volume 单位为「手」(1 手 = 100 股), 与 TickFlow 日K 口径一致
    (indicators/pipeline.py 换手率公式 volume(手) * 10000 / float_shares(股))。
  - ex_factor 为「单次除权事件的 pre/post 比值(非累积)」,
    与 indicators/pipeline._apply_adj_factor 的约定一致。
  - change_pct / amplitude 为小数制 (0.0366 = 3.66%)。

⚠️ 合规: 协议为社区逆向实现, 无官方公开规范, 仅覆盖 A 股。

服务器: 网传 IP 列表 2024-2025 大面积失效(可连 TCP 但 quotes/K线返空),
券商域名服务器(如国泰君安 *.gtjas.com)可用。默认列表可被
TDX_HOSTS="host:port,host:port" 覆盖; 启动时按延迟排序逐个探测
(quotes 探针), 首个全功能服务器即选中。
"""
from __future__ import annotations

import contextlib
import logging
import os
import socket
import threading
import time
from datetime import datetime
from typing import ClassVar

import polars as pl

logger = logging.getLogger(__name__)

PORT = 7709
DEFAULT_HOSTS = [
    # 券商域名服务器（2026-09 实测全功能）
    "jstdx.gtjas.com", "shtdx.gtjas.com", "sztdx.gtjas.com",
    # 常见 IP 服务器（多数已降级/失效, 探测后自动跳过）
    "180.153.18.170", "180.153.18.171", "124.71.187.122",
    "115.238.56.198", "218.75.126.9", "47.107.75.159", "59.175.238.38",
]
QUOTES_BATCH = 80        # get_security_quotes 单次上限
BARS_PAGE = 800          # get_security_bars 单页上限
REQUEST_INTERVAL = 0.03  # 全量拉取时请求间隔, 避免触发服务器限制
UNIVERSE_TTL = 86400     # A 股标的池缓存秒数(标的列表变化以天计)

# A 股股票代码号段（板块指数 881xxx/999xxx 等不在其列）
_STOCK_PREFIX = {
    "SH": ("600", "601", "603", "605", "688", "689"),
    "SZ": ("000", "001", "002", "003", "300", "301"),
    "BJ": ("43", "83", "87", "920"),
}
_SUFFIX_TO_TDX = {"SH": 1, "SZ": 0, "BJ": 2}
_TDX_TO_EXCHANGE = {0: "SZ", 1: "SH", 2: "BJ"}

_FREQ_TO_CATEGORY = {"1m": 8, "5m": 0}


def _parse_symbol(symbol: str) -> tuple[int, str]:
    """600000.SH → (tdx_market=1, '600000')。无法识别时抛 ValueError。"""
    s = symbol.strip().upper()
    code, _, suffix = s.partition(".")
    market = _SUFFIX_TO_TDX.get(suffix)
    if market is None or not code:
        raise ValueError(f"pytdx 仅支持沪深京 A 股代码(600000.SH/000001.SZ/920344.BJ), 得到: {symbol!r}")
    return market, code


def _is_a_stock(exchange: str, code: str) -> bool:
    return code.startswith(_STOCK_PREFIX.get(exchange, ()))


class PytdxConfig:
    # key 是数据集名（provider_has_dataset 据此判断能力）
    datasets: ClassVar[dict] = {"daily": True, "realtime": True, "minute": True, "adj_factor": True}


class PytdxProvider:
    """Provider 契约对齐 GenericHTTPProvider（见 docs/plugin-development.md）。"""

    name = "pytdx"
    builtin = True

    def __init__(self, hosts: list[str] | None = None) -> None:
        self.config = PytdxConfig()
        env_hosts = os.environ.get("TDX_HOSTS", "").strip()
        if env_hosts:
            self._hosts = [h.split(":")[0].strip() for h in env_hosts.split(",") if h.strip()]
        else:
            self._hosts = list(hosts or DEFAULT_HOSTS)
        self._lock = threading.Lock()
        self._api = None          # pytdx TdxHq_API 实例（惰性连接）
        self._connected_host: str | None = None
        self._last_request = 0.0
        self._universe: list[tuple[int, str, str]] | None = None  # (tdx_market, code, name)
        self._universe_at = 0.0

    def close(self) -> None:
        with self._lock:
            self._disconnect()

    # ---- 连接管理（全部 pytdx 访问经 _execute, 断线自动重选服务器重试一次）----

    def _disconnect(self) -> None:
        if self._api is not None:
            with contextlib.suppress(Exception):
                self._api.disconnect()
        self._api = None
        self._connected_host = None

    def _create_client(self, host: str):
        from pytdx.hq import TdxHq_API

        api = TdxHq_API(heartbeat=True, auto_retry=True)
        if not api.connect(host, PORT, time_out=5):
            raise ConnectionError(f"connect {host}:{PORT} failed")
        return api

    def _probe(self, api) -> bool:
        """探针: 能拿到 600000 实时快照才算全功能服务器。"""
        try:
            q = api.get_security_quotes([(1, "600000")])
            return bool(q and q[0].get("price"))
        except Exception:
            return False

    def _connect_best(self) -> None:
        """按 TCP 延迟排序逐个探测, 选中首个全功能服务器。"""
        def _latency(host: str) -> float:
            t0 = time.perf_counter()
            try:
                s = socket.create_connection((host, PORT), timeout=3)
                s.close()
                return time.perf_counter() - t0
            except OSError:
                return float("inf")

        self._disconnect()
        for host in sorted(self._hosts, key=_latency):
            try:
                api = self._create_client(host)
            except Exception as e:
                logger.debug("pytdx %s 连接失败: %s", host, e)
                continue
            if self._probe(api):
                self._api = api
                self._connected_host = host
                logger.info("pytdx 选中服务器 %s:%d", host, PORT)
                return
            logger.debug("pytdx %s 探针失败(quotes 为空), 跳过", host)
            with contextlib.suppress(Exception):
                api.disconnect()
        raise ConnectionError(f"pytdx 无可用行情服务器 (已尝试 {len(self._hosts)} 个)")

    def _execute(self, fn, *, retry: bool = True):
        """持锁执行 pytdx 调用; 失败时重连重试一次(应对 TCP 断线/服务器抽风)。"""
        with self._lock:
            if self._api is None:
                self._connect_best()
            # 简单匀速限速
            wait = REQUEST_INTERVAL - (time.monotonic() - self._last_request)
            if wait > 0:
                time.sleep(wait)
            try:
                result = fn(self._api)
                self._last_request = time.monotonic()
                return result
            except Exception:
                self._disconnect()
                if not retry:
                    raise
            # 重连重试一次
            self._connect_best()
            result = fn(self._api)
            self._last_request = time.monotonic()
            return result

    # ---- A 股标的池（TTL 缓存）----

    def _load_universe(self) -> list[tuple[int, str, str]]:
        if self._universe is not None and time.monotonic() - self._universe_at < UNIVERSE_TTL:
            return self._universe
        out = self._fetch_universe_via_tdx()
        # 北交所: gtjas 等券商服务器不下发 BJ 列表(security_count 有数但 list 恒空,
        # quotes/K线正常)。用项目现有维表补 BJ 行, 保证全市场覆盖。
        if not any(m == 2 for m, _, _ in out):
            bj = self._load_bj_from_project_instruments()
            if bj:
                out.extend(bj)
            else:
                logger.warning("pytdx 北交所标的缺失: TDX 服务器不下发 BJ 列表, 且项目维表无 BJ 行")
        if len(out) < 3000:
            # TDX 列表异常(换服务器/协议变动) → 整体回退项目维表, fail-closed
            fallback = self._load_cn_from_project_instruments()
            if len(fallback) > len(out):
                logger.warning("pytdx TDX 标的列表不完整(%d), 回退项目维表(%d)", len(out), len(fallback))
                out = fallback
        if not out:
            raise ConnectionError("pytdx 标的列表拉取为空(TDX 列表与项目维表均无数据)")
        self._universe = out
        self._universe_at = time.monotonic()
        logger.info("pytdx A股标的池: %d 只", len(out))
        return out

    def _fetch_universe_via_tdx(self) -> list[tuple[int, str, str]]:
        """TDX security_list 翻页拉取。

        券商服务器的列表分页有空洞(SH 第 0 页恒空; SZ 6000-8999 恒空, 其后正常),
        因此空页**跳过**而非终止, 连续 5 页全空才认为该市场拉完。
        """
        out: list[tuple[int, str, str]] = []
        for tdx_market in (0, 1, 2):
            exchange = _TDX_TO_EXCHANGE[tdx_market]
            try:
                total = self._execute(lambda api, m=tdx_market: api.get_security_count(m))
            except Exception as e:
                logger.warning("pytdx get_security_count(%s) 失败: %s", exchange, e)
                continue
            start = 0
            empty_streak = 0
            while start < total and empty_streak < 5:
                try:
                    batch = self._execute(lambda api, m=tdx_market, s=start: api.get_security_list(m, s))
                except Exception as e:
                    logger.warning("pytdx get_security_list(%s, %d) 失败: %s", exchange, start, e)
                    empty_streak += 1
                    start += 1000
                    continue
                if not batch:
                    empty_streak += 1
                    start += 1000
                    continue
                empty_streak = 0
                for item in batch:
                    code, name = item.get("code", ""), item.get("name", "")
                    if code and _is_a_stock(exchange, code):
                        out.append((tdx_market, code, name))
                start += len(batch)
        return out

    @staticmethod
    def _project_instruments_path():
        from app.config import settings

        return settings.data_dir / "instruments" / "instruments.parquet"

    def _load_cn_from_project_instruments(self) -> list[tuple[int, str, str]]:
        """项目 instruments.parquet 的全部 A 股行(完整 SH/SZ/BJ, 维表同步自选定 provider)。"""
        path = self._project_instruments_path()
        if not path.exists():
            return []
        try:
            df = pl.read_parquet(path, columns=["symbol", "name"])
        except Exception as e:
            logger.warning("pytdx 读项目维表失败: %s", e)
            return []
        out: list[tuple[int, str, str]] = []
        for symbol, name in df.iter_rows():
            try:
                market, code = _parse_symbol(symbol)
            except ValueError:
                continue
            if _is_a_stock(_TDX_TO_EXCHANGE[market], code):
                out.append((market, code, name or ""))
        return out

    def _load_bj_from_project_instruments(self) -> list[tuple[int, str, str]]:
        return [row for row in self._load_cn_from_project_instruments() if row[0] == 2]

    # ---- Provider 契约 ----

    def get_realtime(self) -> list[dict]:
        """全市场实时快照。无参(quote_service 全市场模式直接调用)。"""
        universe = self._load_universe()
        records: list[dict] = []
        for i in range(0, len(universe), QUOTES_BATCH):
            chunk = universe[i:i + QUOTES_BATCH]
            quotes = self._execute(
                lambda api, c=chunk: api.get_security_quotes([(m, code) for m, code, _ in c])
            ) or []
            name_by_code = {code: name for _, code, name in chunk}
            for q in quotes:
                code = q.get("code", "")
                price = float(q.get("price") or 0)
                prev = float(q.get("last_close") or 0)
                change = price - prev
                rec = {
                    "symbol": f"{code}.{_TDX_TO_EXCHANGE[q['market']]}",
                    "name": name_by_code.get(code, ""),
                    "last_price": price,
                    "prev_close": prev,
                    "open": float(q.get("open") or 0),
                    "high": float(q.get("high") or 0),
                    "low": float(q.get("low") or 0),
                    "volume": float(q.get("vol") or 0),      # 手
                    "amount": float(q.get("amount") or 0),  # 元
                    "change_amount": change,
                    "change_pct": (change / prev) if prev else 0.0,   # 小数制
                    "amplitude": ((float(q.get("high") or 0) - float(q.get("low") or 0)) / prev) if prev else 0.0,
                }
                records.append(rec)
        return records

    def get_daily(self, symbols, start_time, end_time, asset_type="stock", on_chunk_done=None) -> pl.DataFrame:
        """批量日K。返回 canonical 列 [symbol, date, open, high, low, close, volume, amount]。

        volume 单位「手」与项目口径一致。单标的失败只跳过该标的。
        """
        chunk_size = 20
        rows: list[dict] = []
        failed: list[str] = []
        for i, symbol in enumerate(symbols):
            try:
                market, code = _parse_symbol(symbol)
                rows.extend(self._fetch_daily_one(symbol, market, code, start_time, end_time))
            except Exception as e:
                logger.warning("pytdx 日K %s 拉取失败: %s", symbol, e)
                failed.append(symbol)
            if on_chunk_done and ((i + 1) % chunk_size == 0 or i + 1 == len(symbols)):
                on_chunk_done((i + chunk_size) // chunk_size,
                              (len(symbols) + chunk_size - 1) // chunk_size)
        if failed:
            logger.warning("pytdx 日K部分失败: %d/%d (样例: %s)", len(failed), len(symbols), failed[:10])
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def _fetch_daily_one(self, symbol: str, market: int, code: str,
                         start_time: datetime, end_time: datetime) -> list[dict]:
        """单标的日K 翻页(800 根/页, 从最新往前)直到覆盖 start_time。"""
        out: list[dict] = []
        start_date = start_time.date() if isinstance(start_time, datetime) else start_time
        end_date = end_time.date() if isinstance(end_time, datetime) else end_time
        offset = 0
        while True:
            bars = self._execute(
                lambda api, o=offset: api.get_security_bars(9, market, code, o, BARS_PAGE)
            ) or []
            if not bars:
                break
            oldest_in_page = None
            for b in bars:
                d = datetime(b["year"], b["month"], b["day"]).date()
                if oldest_in_page is None or d < oldest_in_page:
                    oldest_in_page = d
                if start_date <= d <= end_date:
                    out.append({
                        "symbol": symbol,
                        "date": d,
                        "open": float(b["open"]),
                        "high": float(b["high"]),
                        "low": float(b["low"]),
                        "close": float(b["close"]),
                        "volume": float(b["vol"]),      # 手
                        "amount": float(b["amount"]),  # 元
                    })
            if len(bars) < BARS_PAGE or (oldest_in_page and oldest_in_page < start_date):
                break
            offset += len(bars)
        return out

    def get_minute(self, symbols, start_time, end_time, asset_type="stock",
                   on_chunk_done=None, freq="1m") -> pl.DataFrame:
        """分钟K。返回 [symbol, datetime, open, high, low, close, volume, amount]。"""
        category = _FREQ_TO_CATEGORY.get(freq, 8)
        rows: list[dict] = []
        for i, symbol in enumerate(symbols):
            try:
                market, code = _parse_symbol(symbol)
                offset = 0
                while True:
                    bars = self._execute(
                        lambda api, o=offset, m=market, c=code:
                        api.get_security_bars(category, m, c, o, BARS_PAGE)
                    ) or []
                    if not bars:
                        break
                    for b in bars:
                        dt = datetime(b["year"], b["month"], b["day"], b["hour"], b["minute"])
                        if start_time and dt < start_time:
                            continue
                        if end_time and dt > end_time:
                            continue
                        rows.append({
                            "symbol": symbol, "datetime": dt,
                            "open": float(b["open"]), "high": float(b["high"]),
                            "low": float(b["low"]), "close": float(b["close"]),
                            "volume": float(b["vol"]), "amount": float(b["amount"]),
                        })
                    if len(bars) < BARS_PAGE:
                        break
                    offset += len(bars)
            except Exception as e:
                logger.warning("pytdx 分钟K %s 失败: %s", symbol, e)
            if on_chunk_done:
                on_chunk_done(i + 1, len(symbols))
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def get_adj_factors(self, symbols, start_time, end_time, asset_type="stock",
                        on_chunk_done=None) -> pl.DataFrame:
        """XDXR → ex_factor（单次事件 pre/post 比值, 非累积）。

        除权基准价 post = (pre - 分红/10 + 配股价×配股/10) / (1 + 送转/10 + 配股/10)
        ex_factor = pre / post（pre 为除权日前一交易日收盘价）。
        """
        rows: list[dict] = []
        for i, symbol in enumerate(symbols):
            try:
                market, code = _parse_symbol(symbol)
                xdxr = self._execute(lambda api, m=market, c=code: api.get_xdxr_info(m, c)) or []
                events = [r for r in xdxr if r.get("category") == 1]
                if not events:
                    continue
                # 需要除权日前收盘价 → 拉日K 建 date→close 映射
                bars = self._execute(
                    lambda api, m=market, c=code: api.get_security_bars(9, m, c, 0, BARS_PAGE)
                ) or []
                close_by_date = {
                    datetime(b["year"], b["month"], b["day"]).date(): float(b["close"]) for b in bars
                }
                trading_days = sorted(close_by_date)
                for r in events:
                    d = datetime(r["year"], r["month"], r["day"]).date()
                    if start_time and d < (start_time.date() if isinstance(start_time, datetime) else start_time):
                        continue
                    if end_time and d > (end_time.date() if isinstance(end_time, datetime) else end_time):
                        continue
                    pre = next((close_by_date[t] for t in reversed(trading_days) if t < d), None)
                    if not pre:
                        continue
                    post = (
                        pre - float(r.get("fenhong") or 0) / 10
                        + float(r.get("peigujia") or 0) * float(r.get("peigu") or 0) / 10
                    ) / (
                        1 + float(r.get("songzhuangu") or 0) / 10 + float(r.get("peigu") or 0) / 10
                    )
                    if post <= 0:
                        continue
                    rows.append({"symbol": symbol, "trade_date": d, "ex_factor": pre / post})
            except Exception as e:
                logger.warning("pytdx 除权因子 %s 失败: %s", symbol, e)
            if on_chunk_done:
                on_chunk_done(i + 1, len(symbols))
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def get_instruments(self, asset_type="stock") -> list[dict]:
        """标的维表: TDX security_list 按号段过滤 A 股 → tickflow Instrument 形状。"""
        universe = self._load_universe()
        rows = []
        for tdx_market, code, name in universe:
            exchange = _TDX_TO_EXCHANGE[tdx_market]
            rows.append({
                "symbol": f"{code}.{exchange}",
                "name": name,
                "code": code,
                "exchange": exchange,
                "region": "cn",
                "type": "stock",
                "ext": {
                    "listing_date": None,
                    # 股本需逐股 get_finance_info, 全量拉取代价高; 置 None 由项目降级处理
                    "total_shares": None,
                    "float_shares": None,
                    "tick_size": 0.01,
                    "limit_up": None,
                    "limit_down": None,
                },
            })
        return rows
