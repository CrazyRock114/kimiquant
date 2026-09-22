"""pytdx 可用性自检（plugin.yaml check 入口）。

不连接服务器、不抛异常——只做依赖检测，连接问题由 provider 运行时处理。
"""
from __future__ import annotations


def availability() -> tuple[bool, str]:
    try:
        import cryptography  # noqa: F401  # pytdx 硬依赖, 新版在 macOS 需锁 <46

        import pytdx  # noqa: F401
        return True, "ok"
    except ImportError as e:
        return False, (
            f"未安装依赖 ({e.name}), 运行: "
            "cd backend && uv sync --extra pytdx"
        )
