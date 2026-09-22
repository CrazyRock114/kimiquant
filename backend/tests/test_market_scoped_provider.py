"""市场专用数据源（selectable: false）不参与全局单选。

binance 插件仅供 crypto 市场内部路由（daily_pipeline.run_market_sync 按市场分流），
若它能被选为全局「日K数据源」，A股/港股/美股的同步会被路由到币安而拿不到数据。
"""
from __future__ import annotations

from app.data_providers import custom as custom_sources
from app.services import preferences


def test_binance_plugin_registered_but_not_selectable():
    status = custom_sources.list_plugins()
    binance = next((p for p in status if p["name"] == "binance"), None)
    assert binance is not None, "binance 插件应已注册"
    assert binance["selectable"] is False
    assert binance["available"] is True  # runtime: none，无额外依赖
    # 已注册（内部路由可用）但不在可选集合里
    assert "binance" in custom_sources.names()
    assert "binance" not in custom_sources.selectable_names()


def test_non_selectable_plugin_falls_back_to_tickflow(monkeypatch):
    """即使用户偏好里残留/被写入了 binance，全局日K源也必须回退 tickflow。"""
    monkeypatch.setattr(preferences, "load", lambda: {"daily_data_provider": "binance"})
    assert preferences.get_daily_data_provider() == "tickflow"


def test_selectable_plugin_still_allowed(monkeypatch):
    """可选插件不受影响：注册进 _PROVIDERS 且未被标记 selectable=false 的源仍可选。"""
    monkeypatch.setattr(
        preferences, "load", lambda: {"daily_data_provider": "some_custom_source"}
    )
    monkeypatch.setattr(
        custom_sources, "selectable_names", lambda: {"some_custom_source"}
    )
    assert preferences.get_daily_data_provider() == "some_custom_source"
