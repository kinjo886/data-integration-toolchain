# -*- coding: utf-8 -*-
"""
Python 2 / 3 兼容层。

集中管理所有 py2/py3 差异的导入和类型别名，其他模块统一从此处引用，
避免 IDE 静态分析在业务代码中报 import 错误。
"""

import sys

# ── HTTP 客户端 ──────────────────────────────────────────
if sys.version_info[0] >= 3:
    import http.client as http_lib       # pyright: ignore[reportUnusedImport]
    import urllib.request as urllib_req   # pyright: ignore[reportUnusedImport]
    import urllib.parse as urllib_parse   # pyright: ignore[reportUnusedImport]
else:
    import httplib as http_lib           # type: ignore[import-not-found,unused-ignore]
    import urllib2 as urllib_req         # type: ignore[import-not-found,unused-ignore]
    import urllib as _urllib             # type: ignore[import-not-found,unused-ignore]
    import urlparse as urllib_parse      # type: ignore[import-not-found,unused-ignore]

# ── 文本类型 ──────────────────────────────────────────────
if sys.version_info[0] >= 3:
    text_type = str                       # pyright: ignore[reportUnusedImport]
else:
    text_type = unicode                   # type: ignore[name-defined,unused-ignore] # noqa: F821

# ── Unicode 字面量辅助（py2 非 unicode 字面量文件用） ──────
if sys.version_info[0] >= 3:
    def u(s):
        return s
else:
    def u(s):
        return unicode(s, "unicode_escape")  # type: ignore[name-defined,unused-ignore] # noqa: F821
