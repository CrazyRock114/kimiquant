"""app.plugins.pytdx 契约测试：字段标准化、号段过滤、除权因子换算、失败隔离。

不访问真实网络——FakeAPI 预置响应替代 pytdx TdxHq_API。
"""
from __future__ import annotations

from datetime import datetime

import pytest

from app.plugins.pytdx import bridge
from app.plugins.pytdx.provider import PytdxProvider, _parse_symbol


def _bar(y=2026, m=9, d=22, hh=15, mm=0, o=10.0, h=10.5, low=9.5, c=10.2,
         vol=100.0, amount=102000.0):
    return {"year": y, "month": m, "day": d, "hour": hh, "minute": mm,
            "open": o, "high": h, "low": low, "close": c, "vol": vol, "amount": amount}


def _quote(market, code, price=10.2, last_close=10.0, o=10.0, h=10.5, low=9.5,
           vol=100.0, amount=102000.0):
    return {"market": market, "code": code, "price": price, "last_close": last_close,
            "open": o, "high": h, "low": low, "vol": vol, "amount": amount}


class FakeAPI:
    """预置响应的 pytdx 替身。fail_on 中的方法名会抛异常。

    sec_list: {tdx_market: [entries]} — get_security_count/list 按市场返回。
    """

    def __init__(self, quotes=None, bars=None, xdxr=None, sec_list=None, fail_on=()):
        self._quotes = quotes or []
        self._bars = bars or []
        self._xdxr = xdxr or []
        self._sec_list = sec_list or {}
        self._fail_on = set(fail_on)

    def _maybe_fail(self, name):
        if name in self._fail_on:
            raise ConnectionError(f"fake failure in {name}")

    def get_security_quotes(self, stocks):
        self._maybe_fail("get_security_quotes")
        return self._quotes

    def get_security_bars(self, category, market, code, start, count):
        self._maybe_fail("get_security_bars")
        return self._bars[start:start + count]

    def get_security_count(self, market):
        return len(self._sec_list.get(market, []))

    def get_security_list(self, market, start):
        return self._sec_list.get(market, [])[start:start + 1000]

    def get_xdxr_info(self, market, code):
        self._maybe_fail("get_xdxr_info")
        return self._xdxr

    def disconnect(self):
        pass


@pytest.fixture
def provider():
    p = PytdxProvider(hosts=["fakehost"])
    yield p
    p.close()


def _attach(provider: PytdxProvider, api: FakeAPI):
    """预连接 FakeAPI, 绕过服务器选择。"""
    provider._api = api
    provider._connected_host = "fakehost"


# ── 基础 ────────────────────────────────────────────────────


def test_parse_symbol():
    assert _parse_symbol("600000.SH") == (1, "600000")
    assert _parse_symbol("000001.SZ") == (0, "000001")
    assert _parse_symbol("920344.BJ") == (2, "920344")
    with pytest.raises(ValueError):
        _parse_symbol("AAPL.US")
    with pytest.raises(ValueError):
        _parse_symbol("BTCUSDT.CRYPTO")


def test_availability_returns_tuple():
    ok, reason = bridge.availability()
    assert isinstance(ok, bool)
    assert isinstance(reason, str)


# ── get_realtime ────────────────────────────────────────────


def test_get_realtime_mapping(provider, monkeypatch):
    monkeypatch.setattr(provider, "_load_universe",
                        lambda: [(1, "600000", "浦发银行"), (0, "000001", "平安银行")])
    _attach(provider, FakeAPI(quotes=[
        _quote(1, "600000", price=10.2, last_close=10.0, vol=532667, amount=480795744.0),
        _quote(0, "000001", price=11.71, last_close=11.73),
    ]))

    records = provider.get_realtime()

    assert len(records) == 2
    r = records[0]
    assert r["symbol"] == "600000.SH"
    assert r["name"] == "浦发银行"
    assert r["last_price"] == 10.2
    assert r["prev_close"] == 10.0
    assert r["volume"] == 532667.0          # 手, 与项目日K口径一致
    assert r["amount"] == 480795744.0       # 元
    assert r["change_pct"] == pytest.approx(0.02)   # 小数制 (10.2-10)/10
    assert r["change_amount"] == pytest.approx(0.2)
    assert r["amplitude"] == pytest.approx(0.1)     # (10.5-9.5)/10
    # 下跌为负
    assert records[1]["change_pct"] == pytest.approx((11.71 - 11.73) / 11.73)


def test_get_realtime_prev_close_zero_no_division_error(provider, monkeypatch):
    monkeypatch.setattr(provider, "_load_universe", lambda: [(1, "600000", "X")])
    _attach(provider, FakeAPI(quotes=[_quote(1, "600000", price=0, last_close=0)]))
    rec = provider.get_realtime()[0]
    assert rec["change_pct"] == 0.0
    assert rec["amplitude"] == 0.0


# ── get_daily ───────────────────────────────────────────────


def test_get_daily_schema_and_units(provider):
    _attach(provider, FakeAPI(bars=[
        _bar(d=20), _bar(d=21, c=10.1), _bar(d=22, c=10.2),
    ]))
    df = provider.get_daily(["600000.SH"], datetime(2026, 9, 20), datetime(2026, 9, 22))

    assert df.columns == ["symbol", "date", "open", "high", "low", "close",
                          "volume", "amount"]
    assert df.height == 3
    assert df["symbol"].to_list() == ["600000.SH"] * 3
    assert df["volume"].to_list() == [100.0, 100.0, 100.0]  # 手


def test_get_daily_filters_range(provider):
    _attach(provider, FakeAPI(bars=[_bar(d=18), _bar(d=21), _bar(d=22)]))
    df = provider.get_daily(["600000.SH"], datetime(2026, 9, 20), datetime(2026, 9, 22))
    assert df.height == 2  # 9-18 被过滤


def test_get_daily_partial_failure_keeps_others(provider):
    api = FakeAPI(bars=[_bar()], fail_on=set())
    _attach(provider, api)
    # 第二只 symbol 非法 → 抛 ValueError 被跳过
    df = provider.get_daily(["600000.SH", "BADCODE"], datetime(2026, 9, 22), datetime(2026, 9, 22))
    assert df["symbol"].to_list() == ["600000.SH"]


def test_get_daily_all_failed_returns_empty(provider):
    _attach(provider, FakeAPI(fail_on={"get_security_bars"}))
    df = provider.get_daily(["600000.SH"], datetime(2026, 9, 22), datetime(2026, 9, 22))
    assert df.is_empty()


# ── get_adj_factors ─────────────────────────────────────────


def test_adj_factor_formula(provider):
    """分红 4.2 元/10股: post = pre - 0.42, ex_factor = pre / post。"""
    _attach(provider, FakeAPI(
        bars=[_bar(d=15, c=20.0), _bar(d=16, c=20.5)],  # 除权日前收盘 20.5
        xdxr=[{"year": 2026, "month": 9, "day": 16, "category": 1, "name": "除权除息",
               "fenhong": 4.2, "peigujia": 0.0, "songzhuangu": 0.0, "peigu": 0.0}],
    ))
    df = provider.get_adj_factors(["600000.SH"], datetime(2026, 9, 1), datetime(2026, 9, 30))

    assert df.height == 1
    row = df.to_dicts()[0]
    assert row["symbol"] == "600000.SH"
    # pre=20.0 (9-15 是 9-16 之前的最后一个交易日), post=20.0-0.42=19.58
    assert row["ex_factor"] == pytest.approx(20.0 / 19.58)


def test_adj_factor_with_bonus_and_rights(provider):
    """送转 3/10 + 配股 2/10 配股价 5 元 + 分红 1 元/10股。

    post = (pre - 0.1 + 5*0.2) / (1 + 0.3 + 0.2)
    """
    _attach(provider, FakeAPI(
        bars=[_bar(d=15, c=15.0)],
        xdxr=[{"year": 2026, "month": 9, "day": 16, "category": 1, "name": "除权除息",
               "fenhong": 1.0, "peigujia": 5.0, "songzhuangu": 3.0, "peigu": 2.0}],
    ))
    df = provider.get_adj_factors(["600000.SH"], datetime(2026, 9, 1), datetime(2026, 9, 30))
    post = (15.0 - 0.1 + 5.0 * 0.2) / 1.5
    assert df.to_dicts()[0]["ex_factor"] == pytest.approx(15.0 / post)


def test_adj_factor_skips_non_ex_events(provider):
    _attach(provider, FakeAPI(
        bars=[_bar(d=15)],
        xdxr=[{"year": 2026, "month": 9, "day": 16, "category": 3, "name": "非除权事件",
               "fenhong": 0.0, "peigujia": 0.0, "songzhuangu": 0.0, "peigu": 0.0}],
    ))
    df = provider.get_adj_factors(["600000.SH"], datetime(2026, 9, 1), datetime(2026, 9, 30))
    assert df.is_empty()


# ── get_minute ──────────────────────────────────────────────


def test_get_minute_schema(provider):
    _attach(provider, FakeAPI(bars=[_bar(d=22, hh=14, mm=56, vol=50.0)]))
    df = provider.get_minute(["600000.SH"], datetime(2026, 9, 22, 14, 0),
                             datetime(2026, 9, 22, 15, 0))
    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close",
                          "volume", "amount"]
    assert df["datetime"][0] == datetime(2026, 9, 22, 14, 56)


# ── get_instruments ─────────────────────────────────────────


def test_get_instruments_filters_a_share_prefixes(provider, monkeypatch):
    monkeypatch.setattr(provider, "_load_cn_from_project_instruments", lambda: [])
    sec_list = {
        1: [  # SH
            {"code": "600000", "name": "浦发银行"},
            {"code": "688981", "name": "中芯国际"},
            {"code": "999999", "name": "上证指数"},   # 指数 → 过滤
        ],
        0: [  # SZ
            {"code": "000001", "name": "平安银行"},
            {"code": "300750", "name": "宁德时代"},
            {"code": "881148", "name": "保健品"},     # 板块指数 → 过滤
            {"code": "150001", "name": "某基金"},     # 基金 → 过滤
        ],
        2: [  # BJ
            {"code": "920344", "name": "北证标的"},
            {"code": "830799", "name": "北证老股"},
        ],
    }
    _attach(provider, FakeAPI(sec_list=sec_list))

    rows = provider.get_instruments()  # 走真实 _load_universe → _is_a_stock 过滤

    symbols = {r["symbol"] for r in rows}
    assert symbols == {"600000.SH", "688981.SH", "000001.SZ", "300750.SZ",
                       "920344.BJ", "830799.BJ"}
    btc = next(r for r in rows if r["symbol"] == "600000.SH")
    assert btc["name"] == "浦发银行"
    assert btc["exchange"] == "SH"
    assert btc["type"] == "stock"


def test_universe_skips_empty_pages(provider, monkeypatch):
    """SZ 型分页空洞(6000-8999 恒空): 空页跳过而非终止, 空洞后的创业板也能拿到。"""
    monkeypatch.setattr(provider, "_load_cn_from_project_instruments", lambda: [])

    class _Gap(FakeAPI):
        def get_security_count(self, market):
            return {0: 10000, 1: 0, 2: 0}[market]

        def get_security_list(self, market, start):
            if market != 0:
                return None
            if start == 0:
                # 真实服务器每页 1000 条
                return [{"code": "000001", "name": "平安银行"}] + [
                    {"code": f"002{i:03d}", "name": f"深{i}"} for i in range(999)
                ]
            if start in (1000, 2000, 3000, 4000, 5000):
                # 债券/基金页(非股票)
                return [{"code": f"134{i:03d}", "name": f"债{i}"} for i in range(1000)]
            if start in (6000, 7000, 8000):
                return None  # 空洞(与真实 SZ 一致: 3 页)
            if start == 9000:
                return [{"code": "300750", "name": "宁德时代"}] + [
                    {"code": f"301{i:03d}", "name": f"创{i}"} for i in range(999)
                ]
            return None

    _attach(provider, _Gap())
    universe = provider._load_universe()
    codes = {c for _, c, _ in universe}
    assert "000001" in codes
    assert "300750" in codes  # 空洞之后的创业板未被丢弃


def test_universe_bj_falls_back_to_project_parquet(provider, monkeypatch):
    """TDX 不下发 BJ 列表 → 用项目维表补 BJ 行。"""
    monkeypatch.setattr(provider, "_load_cn_from_project_instruments", lambda: [
        (2, "920344", "北证标的"), (2, "830799", "北证老股"),
    ])
    _attach(provider, FakeAPI(sec_list={1: [{"code": "600000", "name": "浦发银行"}]}))
    universe = provider._load_universe()
    markets = {m for m, _, _ in universe}
    assert 2 in markets  # BJ 行已由项目维表补齐


def test_universe_falls_back_to_parquet_when_tdx_incomplete(provider, monkeypatch):
    """TDX 列表整体异常(<3000) → 回退项目维表(fail-closed)。"""
    monkeypatch.setattr(provider, "_load_cn_from_project_instruments", lambda: [
        (1, "600000", "浦发银行"), (0, "000001", "平安银行"), (0, "300750", "宁德时代"),
    ] * 1200)  # 3600 行 > 3000 阈值
    _attach(provider, FakeAPI(sec_list={1: [{"code": "600000", "name": "浦发银行"}]}))
    universe = provider._load_universe()
    assert len(universe) == 3600  # 整体回退到项目维表
