# -*- coding: utf-8 -*-
"""
create-kingsoft-prod-all.py

按“源文档库/源文件/源sheet（模糊匹配）”定位源多维表 sheet，
从源 sheet 的“模板下载链接”列取第一条非空 URL，下载模板 xlsx，
取第一个工作表的第二列生成表头字段，然后在目标文档库根目录创建同名 .dbt 文件，
创建同名 sheet，并按表头创建字段（默认 SingleLineText）。
目标文档库由每条迁移记录中的「文档库名称」栏位定位（不再使用「部门名称」拼接后缀）。
字段创建完成后会删除 sheet 内全部占位空记录（仅保留表头），可用环境变量 KINGSOFT_CLEAR_SHEET_RECORDS_AFTER_CREATE=false 关闭。
"""

import ast
import hashlib
import hmac
import json
import os
import re
import sys
import tempfile
import time
import unicodedata
import traceback
from email.utils import formatdate

try:
    # py2
    import urllib2 as _urllib2  # type: ignore
except Exception:
    # py3
    import urllib.request as _urllib2  # type: ignore

try:
    # py2
    import urllib as _urllib  # type: ignore
    import urlparse as _urlparse  # type: ignore
except Exception:
    # py3
    import urllib.parse as _urlparse  # type: ignore
    import urllib.parse as _urllib  # type: ignore

try:
    text_type = unicode  # type: ignore[name-defined]
except Exception:
    text_type = str

import zipfile
import xml.etree.ElementTree as ET

try:
    # py2
    import httplib as _http_lib  # type: ignore
except Exception:
    # py3
    import http.client as _http_lib  # type: ignore


# ==================== 基础配置（可用环境变量覆盖） ====================

API_HOST = os.getenv("KINGSOFT_API_HOST", "<INTERNAL_API_HOST>")
API_PORT = int(os.getenv("KINGSOFT_API_PORT", "5489"))

DEFAULT_CLIENT_ID = os.getenv("KINGSOFT_CLIENT_ID", "<YOUR_APP_ID>")
DEFAULT_CLIENT_SECRET = os.getenv("KINGSOFT_CLIENT_SECRET", "<YOUR_APP_SECRET>")
DEFAULT_APP_ID = os.getenv("KINGSOFT_APP_ID", DEFAULT_CLIENT_ID)
DEFAULT_APP_KEY = os.getenv("KINGSOFT_APP_KEY", DEFAULT_CLIENT_SECRET)

DEFAULT_COMPANY_ID = os.getenv("KINGSOFT_COMPANY_ID", "1")

OAUTH_GRANT_TYPE = "client_credentials"
OAUTH_DEFAULT_TOKEN_TYPE = "Bearer"

SIGNATURE_PREFIX = "KSO-1"
URL_SPLIT_KEYWORD = "openapi"

CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded"
CONTENT_TYPE_OCTET_STREAM = "application/octet-stream"
CONTENT_TYPE_JSON = "application/json"

HTTP_HEADER_ACCEPT = "*/*"
HTTP_HEADER_ACCEPT_ENCODING = "gzip, deflate, br"
HTTP_HEADER_USER_AGENT = "PostmanRuntime-ApipostRuntime/1.1.0"
HTTP_HEADER_CONNECTION = "keep-alive"

# OpenAPI 路径
API_PATH_OAUTH_TOKEN = "/openapi/oauth2/token"
API_PATH_DOCLIB_SEARCH = "/openapi/v7/doclib/search"
API_PATH_FILES_SEARCH = "/openapi/v7/files/search"
API_PATH_FILE_SCHEMA = "/openapi/v7/coop/dbsheet/{file_id}/schema"
API_PATH_FILE_RECORDS_BY_PAGE = (
    "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records/list_by_page"
)
# 批量删除记录（清空新建 sheet 后平台可能预置的多条空行）；若不存在则回退逐条删除
API_PATH_FILE_RECORDS_DELETE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records/delete"
API_PATH_FILE_RECORD_ONE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records/{record_id}"
API_PATH_SHEETS_CREATE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/create"
API_PATH_SHEET_UPDATE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/update"
API_PATH_FILE_SHEET_FIELDS = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/fields"
API_PATH_FILE_SHEET_FIELDS_DELETE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/fields/delete"
API_PATH_FILE_SHEET_FIELDS_UPDATE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/fields/update"

# Graph 路径（创建文件）
GRAPH_PATH_CREATE_FILE = "/graph/v7/drives/{drive_id}/files/{parent_id}/create"


TEMPLATE_URL_FIELD_NAME = "模板下载链接"
MIGRATE_FLAG_FIELD_NAME = "是否迁移"
MIGRATE_FLAG_YES_VALUE = "是"
REPORT_RESOURCE_NAME_FIELD_NAME = "上报资源名"
REPORT_TABLE_NAME_FIELD_NAME = "上报表名称"
# 目标文档库：由配置 sheet「文档库名称」列直接给出（不再使用「部门名称」拼接后缀）
DOCLIB_FIELD_CANDIDATES = [u"文档库名称", u"文档库", u"doc_lib_name", u"doclib"]

# 新建 sheet 时系统可能自动生成的固定字段（避免重复创建）
SYSTEM_DEFAULT_FIELDS = [u"名称", u"数量", u"日期", u"状态"]

# 调试开关：打印 sheet 记录样本，便于定位字段返回结构
DEBUG_DUMP_SHEET = os.getenv("KINGSOFT_DEBUG_DUMP_SHEET", "").strip().lower() in ("1", "true", "yes", "y", "on")
try:
    DEBUG_DUMP_LIMIT = int(os.getenv("KINGSOFT_DEBUG_DUMP_LIMIT", "20"))
except Exception:
    DEBUG_DUMP_LIMIT = 20
if DEBUG_DUMP_LIMIT <= 0:
    DEBUG_DUMP_LIMIT = 20

# 创建字段完成后，删除该 sheet 内全部记录（仅表头，无数据行）；可设环境变量关闭
CLEAR_SHEET_RECORDS_AFTER_CREATE = os.getenv("KINGSOFT_CLEAR_SHEET_RECORDS_AFTER_CREATE", "true").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
try:
    CLEAR_RECORDS_BATCH = int(os.getenv("KINGSOFT_CLEAR_RECORDS_BATCH", "80"))
except Exception:
    CLEAR_RECORDS_BATCH = 80
if CLEAR_RECORDS_BATCH <= 0:
    CLEAR_RECORDS_BATCH = 80


def _to_text(x):
    if x is None:
        return ""
    if isinstance(x, text_type):
        return x
    # py2: 尝试把 utf-8 字节串解码为 unicode，避免出现 '\xe4\xb8\x80...' 这种不可读串
    if sys.version_info[0] < 3:
        try:
            if isinstance(x, str):
                try:
                    return x.decode("utf-8")
                except Exception:
                    return x.decode("utf-8", "replace")
        except Exception:
            pass
    try:
        return text_type(x)
    except Exception:
        try:
            return text_type(repr(x))
        except Exception:
            return ""


def _to_bytes(s):
    """
    将输入转换为可用于 HTTP body 的 bytes/str：
    - py2: 返回 str（字节串）
    - py3: 返回 bytes
    """
    if s is None:
        return b"" if sys.version_info[0] >= 3 else ""

    # py3 bytes
    if sys.version_info[0] >= 3:
        if isinstance(s, bytes):
            return s
        if isinstance(s, str):
            return s.encode("utf-8")
        return _to_text(s).encode("utf-8")

    # py2: str / unicode
    try:
        if isinstance(s, str):
            return s
    except Exception:
        pass
    try:
        if isinstance(s, unicode):  # type: ignore[name-defined]
            return s.encode("utf-8")
    except Exception:
        pass
    return _to_text(s).encode("utf-8")


def _url_quote(s, safe=""):
    """
    URL 编码（Py2/Py3 兼容）。
    """
    try:
        # py3: urllib.parse.quote
        return _urlparse.quote(_to_text(s), safe=safe)
    except Exception:
        try:
            # py2: urllib.quote expects bytes
            return _urllib.quote(_to_bytes(s), safe=safe)  # type: ignore[attr-defined]
        except Exception:
            return _to_text(s)


def _urlencode(query_items, doseq=False):
    """
    urlencode（Py2/Py3 兼容）。
    query_items: list of (k,v)
    """
    try:
        return _urlparse.urlencode(query_items, doseq=doseq)
    except Exception:
        try:
            return _urllib.urlencode(query_items, doseq=doseq)  # type: ignore[attr-defined]
        except Exception:
            # 最差兜底：手工拼接
            parts = []
            for k, v in (query_items or []):
                parts.append("{0}={1}".format(_url_quote(k), _url_quote(v)))
            return "&".join(parts)


def _print_err(msg):
    try:
        sys.stderr.write(_to_text(msg) + "\n")
        try:
            sys.stderr.flush()
        except Exception:
            pass
    except Exception:
        # fallback
        try:
            print(msg)
        except Exception:
            pass


def _die(msg, exit_code=1):
    _print_err(msg)
    raise SystemExit(exit_code)


def _sanitize_for_json(obj):
    """
    Python2 下 json.dumps(ensure_ascii=False) 要求对象内部字符串尽量是 unicode。
    这里递归把 dict key/value、list 元素中的字节串解码为 unicode（优先 utf-8）。
    """
    # dict
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            out[_to_text(k)] = _sanitize_for_json(v)
        return out

    # list/tuple
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(x) for x in obj]

    # py2 bytes(str) / py3 bytes
    if sys.version_info[0] >= 3:
        if isinstance(obj, bytes):
            try:
                return obj.decode("utf-8")
            except Exception:
                return obj.decode("utf-8", "replace")
        if isinstance(obj, str):
            return obj
        return obj

    # py2
    try:
        if isinstance(obj, unicode):  # type: ignore[name-defined]
            return obj
    except Exception:
        pass
    try:
        if isinstance(obj, str):
            try:
                return obj.decode("utf-8")
            except Exception:
                try:
                    return obj.decode("utf-8", "replace")
                except Exception:
                    return _to_text(obj)
    except Exception:
        pass
    return obj


def _print_json(obj, indent=2):
    safe_obj = _sanitize_for_json(obj)
    txt = json.dumps(safe_obj, ensure_ascii=False, indent=indent)
    # py2: 统一输出 utf-8
    if sys.version_info[0] < 3:
        try:
            if isinstance(txt, unicode):  # type: ignore[name-defined]
                sys.stdout.write(txt.encode("utf-8") + "\n")
                try:
                    sys.stdout.flush()
                except Exception:
                    pass
                return
        except Exception:
            pass
    try:
        sys.stdout.write(_to_text(txt) + "\n")
        try:
            sys.stdout.flush()
        except Exception:
            pass
    except Exception:
        print(txt)


def dump_source_sheet_records(auth, file_id, sheet_id, limit=20):
    """
    打印源 sheet 的记录数据（用于核对前三个入参匹配到的表是否正确）。
    输出字段：
    - record_id
    - fields（原样 dict；会做 JSON unicode 清洗）
    """
    rows = collect_source_sheet_records(auth, file_id=file_id, sheet_id=sheet_id, limit=limit)
    _print_json(rows)


def collect_source_sheet_records(auth, file_id, sheet_id, limit=20):
    """
    收集源 sheet 的记录数据（用于在最终输出里携带，避免某些执行平台只展示最后一段 JSON）。
    """
    rows = []
    i = 0
    for rec in iter_sheet_records(auth, file_id=file_id, sheet_id=sheet_id):
        if not isinstance(rec, dict):
            continue
        i += 1
        fields = rec.get("fields")
        if not isinstance(fields, dict):
            if _is_string(fields):
                parsed = _json_loads_loose(fields)
                if isinstance(parsed, dict):
                    fields = parsed
        if not isinstance(fields, dict):
            fields = {}
        rows.append({"row_index": i, "record_id": rec.get("id"), "fields": fields})
        if i >= int(limit or 0):
            break
    return {
        "debug": "source_sheet_records",
        "file_id": _to_text(file_id),
        "sheet_id": _to_text(sheet_id),
        "limit": int(limit or 0),
        "rows": rows,
    }


def collect_migrate_field_samples(auth, file_id, sheet_id, field_name=MIGRATE_FLAG_FIELD_NAME, yes_value=MIGRATE_FLAG_YES_VALUE, limit=10):
    """
    当 migrate_yes_count=0 时，收集少量“是否迁移”字段值样本，便于定位：
    - 字段是否存在
    - 实际返回值结构（str/dict/list）
    - 抽取后的文本/规范化后的比较值
    """
    yes_cmp = _normalize_compare_text(yes_value)
    yes_norm = yes_cmp.lower()
    rows = []
    i = 0
    for fields in iter_sheet_records_fields(auth, file_id=file_id, sheet_id=sheet_id):
        if not isinstance(fields, dict):
            continue
        i += 1
        found, raw = _get_field_value_by_name(fields, field_name)
        extracted = _extract_cell_text(raw) if found else ""
        extracted_cmp = _normalize_compare_text(extracted)
        try:
            keys_sample = [_to_text(k) for k in list(fields.keys())[:30]]
        except Exception:
            keys_sample = []
        raw_short = _to_text(raw)
        if len(raw_short) > 200:
            raw_short = raw_short[:200] + "..."
        rows.append(
            {
                "row_index": i,
                "found_field": bool(found),
                "field_name": _to_text(field_name),
                "raw_value_short": raw_short,
                "extracted_text": _to_text(extracted),
                "extracted_compare": _to_text(extracted_cmp),
                "yes_compare": _to_text(yes_cmp),
                "yes_norm": _to_text(yes_norm),
                "keys_sample": keys_sample,
            }
        )
        if i >= int(limit or 0):
            break
    return {
        "debug": "migrate_field_samples",
        "file_id": _to_text(file_id),
        "sheet_id": _to_text(sheet_id),
        "field_name": _to_text(field_name),
        "yes_value": _to_text(yes_value),
        "limit": int(limit or 0),
        "rows": rows,
    }


def _is_string(obj):
    # py2: str/unicode; py3: str
    if sys.version_info[0] >= 3:
        return isinstance(obj, str)
    try:
        return isinstance(obj, basestring)  # type: ignore[name-defined]
    except Exception:
        return isinstance(obj, (str, text_type))


def _extract_cell_text(value):
    """
    将多维表单元格值尽量提取为可比较的文本：
    - str/unicode：直接返回（如是 JSON 字符串则尝试解析）
    - dict：优先取常见字段 text/name/value/label/title，再递归兜底遍历 values
    - list/tuple：取第一个非空的递归结果
    - 其它：转文本
    """
    if value is None:
        return ""

    # 字符串：可能是 JSON 字符串
    if _is_string(value):
        s = _to_text(value).strip()
        if not s:
            return ""
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            parsed = _json_loads_loose(s)
            if parsed is not None:
                return _extract_cell_text(parsed)
        return s

    # dict：单选/人员等复杂类型经常是 dict
    if isinstance(value, dict):
        for k in ("text", "name", "value", "label", "title", "display_name"):
            if k in value:
                t = _extract_cell_text(value.get(k))
                if t:
                    return t
        # 兜底遍历所有 value
        for v in value.values():
            t = _extract_cell_text(v)
            if t:
                return t
        return ""

    # list/tuple：多选通常是数组
    if isinstance(value, (list, tuple)):
        for it in value:
            t = _extract_cell_text(it)
            if t:
                return t
        return ""

    return _to_text(value).strip()


def _normalize_field_key(name):
    """
    规范化字段名，用于与接口返回的 fields key 做容错匹配：
    - 去除首尾空白（含全角空格）
    - 合并中间多空格为单空格
    """
    s = _to_text(name)
    if not s:
        return ""
    # 去掉全角空格
    s = s.replace(u"\u3000", u" ").strip()
    # 合并多空格
    try:
        s = re.sub(r"\s+", " ", s)
    except Exception:
        # py2 fallback
        s = " ".join(s.split())
    return s


def _normalize_compare_text(s):
    """
    用于“是否迁移=是”这类短文本的容错比较：
    - 统一为 unicode 文本
    - 去除首尾空白（含全角空格）
    - 去除 Unicode 格式类/控制类字符（常见：零宽空格 U+200B、BOM U+FEFF 等）
    """
    t = _to_text(s)
    if not t:
        return ""
    t = t.replace(u"\u3000", u" ").strip()
    out = []
    for ch in t:
        try:
            cat = unicodedata.category(ch)
        except Exception:
            cat = ""
        # Zs: space separator；Cc/Cf: control/format
        if cat in ("Cc", "Cf"):
            continue
        if cat == "Zs" and ch.strip() == "":
            continue
        out.append(ch)
    return u"".join(out)


def _get_field_value_by_name(fields_dict, desired_name):
    """
    从一条记录的 fields 字典中按“字段展示名”取值，支持规范化匹配。
    返回 (found:bool, value:any)。
    """
    if not isinstance(fields_dict, dict):
        return False, None
    if desired_name in fields_dict:
        return True, fields_dict.get(desired_name)
    dn = _normalize_field_key(desired_name)
    if not dn:
        return False, None
    for k in fields_dict.keys():
        if _normalize_field_key(k) == dn:
            return True, fields_dict.get(k)
    return False, None

def _match_name(query, target):
    if not query or not target:
        return False
    return _to_text(query).strip().lower() in _to_text(target).strip().lower()


def _match_name_either_contains(query, target):
    """
    双向子串匹配：用于文档库名等“用户关键字”与接口返回全称长短不一致时。
    例：query「大数据三期迁移测试」可匹配 target「大数据三期迁移测试-专用库」；
    或 query 带后缀而接口名为较短前缀时。
    """
    if not query or not target:
        return False
    q = _to_text(query).strip().lower()
    t = _to_text(target).strip().lower()
    if not q or not t:
        return False
    return q in t or t in q


def _is_equal_ignore_case(a, b):
    return _to_text(a).strip().lower() == _to_text(b).strip().lower()


def _pick_best(
    query,
    candidates,
    get_name,
    max_echo=20,
):
    if not candidates:
        raise RuntimeError(
            "文档库/文件候选列表为空，无法匹配：query={0!r}。"
            "若为文档库，请确认 /openapi/v7/doclib/search 是否有数据（company_id、权限、返回 JSON 结构）。".format(
                _to_text(query)
            )
        )

    # 先构造 (name, obj)
    named = []
    for c in candidates:
        try:
            n = get_name(c) or ""
        except Exception:
            n = ""
        named.append((_to_text(n), c))

    # 完全相等优先
    q = _to_text(query)
    exact = [(n, c) for (n, c) in named if _is_equal_ignore_case(q, n)]
    pool = exact if exact else [(n, c) for (n, c) in named if _match_name(q, n)]
    if not pool:
        pool = [(n, c) for (n, c) in named if _match_name_either_contains(q, n)]
    if not pool:
        echo = [n for (n, _) in named if n][:max_echo]
        raise RuntimeError("未找到匹配项：query={0!r} candidates(sample)={1!r}".format(q, echo))

    # 名称短优先
    pool.sort(key=lambda x: (len(x[0] or ""), (x[0] or "").lower()))
    if len(pool) > 1:
        echo = [n for (n, _) in pool[:max_echo]]
        print("匹配到多个候选，已按规则择一：query={0!r} candidates(sample)={1!r}".format(q, echo))
    return pool[0][1]


def _json_loads_loose(text):
    try:
        return json.loads(text)
    except Exception:
        try:
            return ast.literal_eval(text)
        except Exception:
            return None


def get_request_headers(
    method,
    url_for_sign,
    body="",
    content_type=CONTENT_TYPE_FORM_URLENCODED,
    app_id=None,
    app_key=None,
    sign_path_override=None,
):
    """
    生成 KSO-1 签名头。

    - 默认行为：对齐仓库现有逻辑：按 url.split("openapi")[1] 取 path 参与签名
    - 如传入 sign_path_override，则直接使用它参与签名（用于 /graph 等特殊前缀）
    """
    app_id = app_id or DEFAULT_APP_ID
    app_key = app_key or DEFAULT_APP_KEY
    if not app_id or not app_key:
        raise RuntimeError("未配置 appId/appKey，请设置环境变量 KINGSOFT_APP_ID/KINGSOFT_APP_KEY")

    method = method.upper()
    date_string = formatdate(usegmt=True)

    if sign_path_override is not None:
        path = sign_path_override
    else:
        parts = url_for_sign.split(URL_SPLIT_KEYWORD, 1)
        path = parts[1] if len(parts) == 2 else url_for_sign

    body = body or ""
    if body == "":
        base_string = "{0}{1}{2}{3}{4}".format(SIGNATURE_PREFIX, method, path, content_type, date_string)
    else:
        sha256_hex = hashlib.sha256(body.encode("utf-8")).hexdigest()
        base_string = "{0}{1}{2}{3}{4}{5}".format(
            SIGNATURE_PREFIX, method, path, content_type, date_string, sha256_hex
        )

    signature = hmac.new(app_key.encode("utf-8"), base_string.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = "{0} {1}:{2}".format(SIGNATURE_PREFIX, app_id, signature)
    return {"X-Kso-Date": date_string, "Content-Type": content_type, "X-Kso-Authorization": authorization}


def _http_request(
    method,
    path,
    headers,
    body=None,
    host=API_HOST,
    port=API_PORT,
    timeout=60,
):
    conn = _http_lib.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request(method, path, body=_to_bytes(body) if body is not None else _to_bytes(""), headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        # py2: data is str；py3: bytes
        if sys.version_info[0] >= 3:
            try:
                text = data.decode("utf-8")
            except Exception:
                text = data.decode("utf-8", "replace")
        else:
            # py2：尽量按 utf-8 解码为 unicode 再转 text
            try:
                text = data.decode("utf-8")
            except Exception:
                try:
                    text = data.decode("utf-8", "replace")
                except Exception:
                    text = _to_text(data)
        return resp.status, text
    finally:
        conn.close()


def app_authorize(client_id, client_secret):
    method = "POST"
    path = API_PATH_OAUTH_TOKEN
    payload = "grant_type={0}&client_id={1}&client_secret={2}".format(OAUTH_GRANT_TYPE, client_id, client_secret)
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed = get_request_headers(
        method=method,
        url_for_sign=url_for_sign,
        body=payload,
        content_type=CONTENT_TYPE_FORM_URLENCODED,
        app_id=client_id,
        app_key=client_secret,
    )
    headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
    }
    headers.update(signed)
    status, text = _http_request(method, path, headers=headers, body=payload.encode("utf-8"))
    if status != 200:
        raise RuntimeError("应用授权失败 HTTP {0}: {1}".format(status, text[:2000]))
    resp = json.loads(text)
    token = resp.get("access_token") or ""
    token_type = resp.get("token_type") or OAUTH_DEFAULT_TOKEN_TYPE
    if not token:
        raise RuntimeError("应用授权失败：未返回 access_token: {0}".format(text[:2000]))
    return {"access_token": token, "token_type": token_type}


def _auth_headers(auth):
    return {"Authorization": "{0} {1}".format(auth.get("token_type") or OAUTH_DEFAULT_TOKEN_TYPE, auth.get("access_token"))}


def _doclib_items_from_response(resp):
    """
    从 /doclib/search 的 JSON 中抽取文档库条目列表（兼容多种网关包装）。
    对齐 oracle-data-insert-kingsoft-prod-all.get_doc_lib_list：除 data.items 外可回落到顶层 items。
    """
    if not isinstance(resp, dict):
        return []
    items = []
    data = resp.get("data")
    if isinstance(data, dict):
        raw = data.get("items") or data.get("list") or data.get("records")
        if isinstance(raw, list):
            items = raw
    if not items and isinstance(resp.get("items"), list):
        items = resp.get("items") or []
    if not items and isinstance(data, list):
        items = data
    out = []
    for x in items or []:
        if isinstance(x, dict):
            out.append(x)
    return out


def doclib_search(auth, keyword, company_id=DEFAULT_COMPANY_ID, page_size=200):
    encoded_keyword = _url_quote(keyword or "", safe="")
    encoded_company_id = _url_quote(company_id or "", safe="")
    path = "{0}?page_size={1}&company_id={2}&keyword={3}".format(
        API_PATH_DOCLIB_SEARCH, page_size, encoded_company_id, encoded_keyword
    )
    method = "GET"
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed = get_request_headers(method=method, url_for_sign=url_for_sign, body="", content_type=CONTENT_TYPE_OCTET_STREAM)
    headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
        "X-Kso-Id-Type": "internal",
    }
    headers.update(_auth_headers(auth))
    headers.update(signed)
    headers["Content-Type"] = CONTENT_TYPE_OCTET_STREAM
    status, text = _http_request(method, path, headers=headers)
    if status != 200:
        raise RuntimeError("搜索文档库失败 HTTP {0}: {1}".format(status, text[:2000]))
    try:
        resp = json.loads(text)
    except Exception:
        raise RuntimeError("搜索文档库返回非 JSON：{0}".format(_to_text(text[:2000])))
    out = _doclib_items_from_response(resp)
    if not out:
        print(
            "警告：doclib_search 解析后候选为空。keyword={0!r} company_id={1!r} page_size={2} resp(sample)={3}".format(
                _to_text(keyword), _to_text(company_id), page_size, _to_text(text[:800])
            )
        )
    return out


def _get_doclib_name(item):
    base = item
    if isinstance(item.get("doclib"), dict):
        base = item.get("doclib")
    drive = base.get("drive") if isinstance(base.get("drive"), dict) else {}
    for k in ("name", "display_name", "title"):
        v = base.get(k)
        vt = _to_text(v).strip()
        if vt:
            return vt
        v2 = drive.get(k) if isinstance(drive, dict) else None
        v2t = _to_text(v2).strip()
        if v2t:
            return v2t
    return ""


def _get_drive_id(item):
    base = item
    if isinstance(item.get("doclib"), dict):
        base = item.get("doclib")
    drive = base.get("drive") if isinstance(base.get("drive"), dict) else {}
    v = drive.get("id")
    vt = _to_text(v).strip()
    return vt


def files_search(
    auth,
    keyword,
    drive_ids,
    search_type="file_name",
    page_size=100,
):
    query_items = [
        ("keyword", _to_text(keyword or "")),
        ("type", search_type),
        ("page_size", str(page_size)),
    ]
    # drive_ids 兼容重复参数
    for did in drive_ids or []:
        if did:
            query_items.append(("drive_ids", _to_text(did)))
    encoded_query = _urlencode(query_items, doseq=True)
    path = "{0}?{1}".format(API_PATH_FILES_SEARCH, encoded_query)
    method = "GET"
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed = get_request_headers(method=method, url_for_sign=url_for_sign, body="", content_type=CONTENT_TYPE_OCTET_STREAM)
    headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
    }
    headers.update(_auth_headers(auth))
    headers.update(signed)
    headers["Content-Type"] = CONTENT_TYPE_OCTET_STREAM
    status, text = _http_request(method, path, headers=headers)
    if status != 200:
        raise RuntimeError("搜索文件失败 HTTP {0}: {1}".format(status, text[:2000]))
    resp = json.loads(text)
    code = resp.get("code", 0)
    if code not in (0, "0", None):
        raise RuntimeError("搜索文件失败：code={0} resp={1}".format(code, text[:2000]))
    items = (resp.get("data") or {}).get("items") or []
    out = []
    if isinstance(items, list):
        for it in items:
            if not isinstance(it, dict):
                continue
            fobj = it.get("file")
            if isinstance(fobj, dict):
                out.append(fobj)
            else:
                out.append(it)
    return out


def _get_file_name(item):
    for k in ("name", "display_name", "title", "file_name"):
        v = item.get(k)
        vt = _to_text(v).strip()
        if vt:
            return vt
    return ""


def _get_file_id(item):
    v = item.get("id")
    return _to_text(v).strip() if v is not None else ""


def _find_existing_file_by_exact_name(auth, drive_id, exact_name):
    """
    在指定 drive 下按文件名精确匹配查找文件。
    files_search 是模糊搜索，这里做二次精确过滤。
    返回 file dict 或 None。
    """
    exact = _to_text(exact_name).strip()
    if not exact:
        return None
    # 优先用包含后缀的 keyword 搜索；若无结果再用去后缀搜索
    keywords = [exact]
    if exact.lower().endswith(".dbt"):
        keywords.append(exact[:-4])
    for kw in keywords:
        try:
            items = files_search(auth, keyword=kw, drive_ids=[drive_id])
        except Exception:
            items = []
        for it in items or []:
            if not isinstance(it, dict):
                continue
            nm = _get_file_name(it)
            if _is_equal_ignore_case(nm, exact):
                return it
    return None


def get_file_schema(auth, file_id):
    path = API_PATH_FILE_SCHEMA.format(file_id=file_id)
    method = "GET"
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed = get_request_headers(method=method, url_for_sign=url_for_sign, body="", content_type=CONTENT_TYPE_OCTET_STREAM)
    headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
    }
    headers.update(_auth_headers(auth))
    headers.update(signed)
    headers["Content-Type"] = CONTENT_TYPE_OCTET_STREAM
    status, text = _http_request(method, path, headers=headers)
    if status != 200:
        raise RuntimeError("获取 schema 失败 HTTP {0}: {1}".format(status, text[:2000]))
    return json.loads(text)


def _get_sheet_name(sheet):
    for k in ("name", "display_name", "title"):
        v = sheet.get(k)
        vt = _to_text(v).strip()
        if vt:
            return vt
    return ""


def _get_sheet_id(sheet):
    v = sheet.get("id")
    return _to_text(v).strip() if v is not None else ""


def iter_sheet_records_fields(
    auth,
    file_id,
    sheet_id,
    page_size=100,
):
    page_num = 1
    method = "POST"
    while True:
        # 显式 prefer_id=false，尽量让 fields key 使用“展示名”而非字段 id
        body_dict = {"page_num": page_num, "page_size": page_size, "prefer_id": False}
        body = json.dumps(body_dict, ensure_ascii=False)
        path = API_PATH_FILE_RECORDS_BY_PAGE.format(file_id=file_id, sheet_id=sheet_id)
        url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
        signed = get_request_headers(method=method, url_for_sign=url_for_sign, body=body, content_type=CONTENT_TYPE_JSON)
        headers = {
            "Accept": HTTP_HEADER_ACCEPT,
            "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
            "User-Agent": HTTP_HEADER_USER_AGENT,
            "Connection": HTTP_HEADER_CONNECTION,
        }
        headers.update(_auth_headers(auth))
        headers.update(signed)
        headers["Content-Type"] = CONTENT_TYPE_JSON
        status, text = _http_request(method, path, headers=headers, body=body.encode("utf-8"))
        if status != 200:
            raise RuntimeError("分页获取 records 失败 HTTP {0}: {1}".format(status, text[:2000]))
        resp = json.loads(text)
        records = (resp.get("data") or {}).get("records") or []
        if not isinstance(records, list) or not records:
            break

        for rec in records:
            if not isinstance(rec, dict):
                continue
            fields_val = rec.get("fields")
            if isinstance(fields_val, dict):
                yield fields_val
                continue
            if _is_string(fields_val):
                parsed = _json_loads_loose(fields_val)
                if isinstance(parsed, dict):
                    yield parsed
                continue

        if len(records) < page_size:
            break
        page_num += 1


def iter_sheet_records(
    auth,
    file_id,
    sheet_id,
    page_size=100,
):
    """
    与 iter_sheet_records_fields 相同的分页拉取，但返回完整 record（包含 id/fields 等）。
    """
    page_num = 1
    method = "POST"
    while True:
        body_dict = {"page_num": page_num, "page_size": page_size, "prefer_id": False}
        body = json.dumps(body_dict, ensure_ascii=False)
        path = API_PATH_FILE_RECORDS_BY_PAGE.format(file_id=file_id, sheet_id=sheet_id)
        url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
        signed = get_request_headers(method=method, url_for_sign=url_for_sign, body=body, content_type=CONTENT_TYPE_JSON)
        headers = {
            "Accept": HTTP_HEADER_ACCEPT,
            "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
            "User-Agent": HTTP_HEADER_USER_AGENT,
            "Connection": HTTP_HEADER_CONNECTION,
        }
        headers.update(_auth_headers(auth))
        headers.update(signed)
        headers["Content-Type"] = CONTENT_TYPE_JSON
        status, text = _http_request(method, path, headers=headers, body=body.encode("utf-8"))
        if status != 200:
            raise RuntimeError("分页获取 records 失败 HTTP {0}: {1}".format(status, text[:2000]))
        resp = json.loads(text)
        records = (resp.get("data") or {}).get("records") or []
        if not isinstance(records, list) or not records:
            break
        for rec in records:
            if isinstance(rec, dict):
                yield rec
        if len(records) < page_size:
            break
        page_num += 1


def collect_sheet_record_ids(auth, file_id, sheet_id):
    """分页拉取 sheet 全部记录 id（用于清空占位行）。"""
    ids = []
    for rec in iter_sheet_records(auth, file_id=file_id, sheet_id=sheet_id):
        if not isinstance(rec, dict):
            continue
        rid = rec.get("id")
        if rid is None:
            continue
        t = _to_text(rid).strip()
        if t:
            ids.append(t)
    return ids


def _post_json_signed(auth, path, body_dict):
    """OpenAPI JSON POST（与 create_fields 相同签名方式）。"""
    body = json.dumps(body_dict, ensure_ascii=False)
    method = "POST"
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed = get_request_headers(method=method, url_for_sign=url_for_sign, body=body, content_type=CONTENT_TYPE_JSON)
    headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
    }
    headers.update(_auth_headers(auth))
    headers.update(signed)
    headers["Content-Type"] = CONTENT_TYPE_JSON
    return _http_request(method, path, headers=headers, body=body.encode("utf-8"))


def _delete_one_record_rest(auth, file_id, sheet_id, record_id):
    """单条删除：DELETE .../records/{record_id}（兼容无批量删除的环境）。"""
    rid = _url_quote(_to_text(record_id), safe="")
    path = API_PATH_FILE_RECORD_ONE.format(file_id=_to_text(file_id), sheet_id=_to_text(sheet_id), record_id=rid)
    method = "DELETE"
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed = get_request_headers(method=method, url_for_sign=url_for_sign, body="", content_type=CONTENT_TYPE_OCTET_STREAM)
    headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
    }
    headers.update(_auth_headers(auth))
    headers.update(signed)
    headers["Content-Type"] = CONTENT_TYPE_OCTET_STREAM
    return _http_request(method, path, headers=headers, body=None)


def clear_sheet_placeholder_records(auth, file_id, sheet_id):
    """
    新建 sheet 并完成建字段后，平台常会预置多条「空记录」；本函数删除当前 sheet 全部记录，仅保留字段（表头）。
    先 POST batch delete，失败再逐条 DELETE。
    """
    ids = collect_sheet_record_ids(auth, file_id=file_id, sheet_id=sheet_id)
    if not ids:
        return {"deleted_reported": 0, "attempted": 0, "remaining_ids_count": 0, "note": "no_records"}

    path_batch = API_PATH_FILE_RECORDS_DELETE.format(file_id=_to_text(file_id), sheet_id=_to_text(sheet_id))
    alt_batch = "/openapi/v7/coop/dbsheet/{0}/sheets/{1}/records/batch_delete".format(
        _to_text(file_id), _to_text(sheet_id)
    )
    deleted = 0
    errors = []

    for i in range(0, len(ids), CLEAR_RECORDS_BATCH):
        chunk = ids[i : i + CLEAR_RECORDS_BATCH]
        batch_ok = False
        for p in (path_batch, alt_batch):
            for body_key in ("records", "record_ids"):
                status, text = _post_json_signed(auth, p, {body_key: chunk})
                if status != 200:
                    continue
                try:
                    resp = json.loads(text)
                except Exception:
                    continue
                code = resp.get("code", 0)
                if code in (0, "0", None):
                    deleted += len(chunk)
                    batch_ok = True
                    break
            if batch_ok:
                break
        if batch_ok:
            continue
        for rid in chunk:
            st, tx = _delete_one_record_rest(auth, file_id, sheet_id, rid)
            if st == 200:
                try:
                    rj = json.loads(tx)
                    c = rj.get("code", 0)
                    if c in (0, "0", None):
                        deleted += 1
                        continue
                except Exception:
                    deleted += 1
                    continue
            errors.append("del {0} http={1}".format(rid, st))

    left = collect_sheet_record_ids(auth, file_id=file_id, sheet_id=sheet_id)
    return {
        "deleted_reported": deleted,
        "attempted": len(ids),
        "remaining_ids_count": len(left),
        "errors_sample": errors[:8],
    }


def debug_dump_sheet_migrate_field(auth, file_id, sheet_id, field_name, limit=20):
    """
    打印 sheet 的记录样本（仅前 limit 条），重点输出：
    - fields keys（前30个）
    - 是否命中 field_name（支持规范化匹配）
    - 原始单元格值 raw（截断）
    - 提取后的文本 extracted（用于判断是否为“是”）
    """
    rows = []
    idx = 0
    for rec in iter_sheet_records(auth, file_id=file_id, sheet_id=sheet_id):
        idx += 1
        fields = rec.get("fields")
        if isinstance(fields, dict):
            found, raw = _get_field_value_by_name(fields, field_name)
            extracted = _extract_cell_text(raw) if found else ""
            try:
                keys_sample = [_to_text(k) for k in list(fields.keys())[:30]]
            except Exception:
                keys_sample = []
        else:
            found, raw, extracted, keys_sample = False, None, "", []

        raw_short = _to_text(raw)
        if len(raw_short) > 200:
            raw_short = raw_short[:200] + "..."
        rows.append(
            {
                "row_index": idx,
                "record_id": rec.get("id"),
                "found_field": found,
                "field_name": _to_text(field_name),
                "field_name_normalized": _normalize_field_key(field_name),
                "raw_value_short": raw_short,
                "extracted_text": extracted,
                "keys_sample": keys_sample,
            }
        )
        if idx >= limit:
            break

    _print_json(
        {
            "debug": "sheet_migrate_field_sample",
            "file_id": _to_text(file_id),
            "sheet_id": _to_text(sheet_id),
            "field_name": _to_text(field_name),
            "limit": limit,
            "rows": rows,
        }
    )


def pick_first_non_empty_template_url(
    auth,
    file_id,
    sheet_id,
    field_name=TEMPLATE_URL_FIELD_NAME,
):
    for fields in iter_sheet_records_fields(auth, file_id=file_id, sheet_id=sheet_id):
        if not isinstance(fields, dict):
            continue
        found, v = _get_field_value_by_name(fields, field_name)
        if not found:
            continue
        s = _extract_cell_text(v)
        if s:
            return s
    raise RuntimeError("未找到列 {0!r} 的第一条非空值，请确认源 sheet 中该列存在且有值".format(field_name))


def pick_first_non_empty_field_text(auth, file_id, sheet_id, field_name):
    """
    从源 sheet 的指定列中取第一条非空文本（用于上报资源名/上报表名称等）。
    """
    for fields in iter_sheet_records_fields(auth, file_id=file_id, sheet_id=sheet_id):
        if not isinstance(fields, dict):
            continue
        found, v = _get_field_value_by_name(fields, field_name)
        if not found:
            continue
        s = _extract_cell_text(v)
        if s:
            return s
    return ""


def iter_migrate_yes_items(
    auth,
    file_id,
    sheet_id,
    migrate_field_name=MIGRATE_FLAG_FIELD_NAME,
    yes_value=MIGRATE_FLAG_YES_VALUE,
    resource_name_field=REPORT_RESOURCE_NAME_FIELD_NAME,
    table_name_field=REPORT_TABLE_NAME_FIELD_NAME,
    template_url_field=TEMPLATE_URL_FIELD_NAME,
    doclib_field_candidates=DOCLIB_FIELD_CANDIDATES,
    limit=None,
):
    """
    遍历源 sheet 中 “是否迁移=是” 的记录，抽取每条记录用于创建目标文档所需的信息：
    - 上报资源名 -> 目标文件名
    - 上报表名称 -> 目标 sheet 名
    - 模板下载链接 -> 下载模板，解析表头
    - 文档库名称（文档库/…）-> 用于定位目标文档库（表空间）
    """
    yes_norm = _normalize_compare_text(yes_value).lower()
    n = 0
    for rec in iter_sheet_records(auth, file_id=file_id, sheet_id=sheet_id):
        if limit is not None and n >= int(limit):
            break
        fields = rec.get("fields") if isinstance(rec, dict) else None
        if not isinstance(fields, dict):
            # 兼容 fields 为 JSON 字符串
            if _is_string(fields):
                parsed = _json_loads_loose(fields)
                if isinstance(parsed, dict):
                    fields = parsed
            if not isinstance(fields, dict):
                continue

        found, mv = _get_field_value_by_name(fields, migrate_field_name)
        if not found:
            continue
        mtxt = _normalize_compare_text(_extract_cell_text(mv)).lower()
        if not mtxt or mtxt != yes_norm:
            continue

        # 命中迁移=是
        n += 1

        def _pick(field):
            f_found, fv = _get_field_value_by_name(fields, field)
            if not f_found:
                return ""
            return _extract_cell_text(fv)

        def _pick_doclib():
            for nm in doclib_field_candidates or []:
                t = _pick(nm)
                if t:
                    return t, nm
            return "", ""

        doclib_val, doclib_key = _pick_doclib()

        yield {
            "record_id": rec.get("id") if isinstance(rec, dict) else None,
            "report_resource_name": _pick(resource_name_field),
            "report_table_name": _pick(table_name_field),
            "template_url": _pick(template_url_field),
            "doclib_name": doclib_val,
            "doclib_field": _to_text(doclib_key),
        }


def count_migrate_yes_rows(
    auth,
    file_id,
    sheet_id,
    field_name=MIGRATE_FLAG_FIELD_NAME,
    yes_value=MIGRATE_FLAG_YES_VALUE,
):
    """
    统计源 sheet 中“是否迁移”=“是”的记录数量。
    若列不存在，则返回 0（视为不迁移）。
    """
    yes_cmp = _normalize_compare_text(yes_value)
    yes_norm = yes_cmp.lower()
    cnt = 0
    saw_field = False
    debug_samples = []
    debug_rows = []
    for fields in iter_sheet_records_fields(auth, file_id=file_id, sheet_id=sheet_id):
        if not isinstance(fields, dict):
            continue
        found, v = _get_field_value_by_name(fields, field_name)
        if not found:
            # 收集少量样本 key，便于定位“是否迁移”列在响应中到底叫什么/是否用 id 返回
            if len(debug_samples) < 3:
                try:
                    debug_samples.append([_to_text(k) for k in list(fields.keys())[:30]])
                except Exception:
                    pass
            continue
        saw_field = True
        s = _extract_cell_text(v)
        if len(debug_rows) < 5:
            raw_short = _to_text(v)
            if len(raw_short) > 200:
                raw_short = raw_short[:200] + "..."
            try:
                keys_sample = [_to_text(k) for k in list(fields.keys())[:30]]
            except Exception:
                keys_sample = []
            debug_rows.append(
                {
                    "raw_value_short": raw_short,
                    "extracted_text": _to_text(s),
                    "extracted_compare": _to_text(_normalize_compare_text(s)),
                    "keys_sample": keys_sample,
                }
            )
        s_cmp = _normalize_compare_text(s)
        if s_cmp and s_cmp.lower() == yes_norm:
            cnt += 1
    if cnt <= 0:
        # 仅在没命中时输出诊断信息（避免刷屏）
        try:
            print(
                "诊断：migrate_yes_count=0。saw_field={0} field_name={1!r} normalized={2!r} samples_keys={3}".format(
                    saw_field,
                    _to_text(field_name),
                    _normalize_field_key(field_name),
                    _to_text(json.dumps(debug_samples, ensure_ascii=False)[:1500]),
                )
            )
        except Exception:
            pass

        # 默认也输出少量“命中字段的样本值”，便于确认是否真的等于“是”
        try:
            _print_json(
                {
                    "debug": "migrate_field_value_sample",
                    "file_id": _to_text(file_id),
                    "sheet_id": _to_text(sheet_id),
                    "field_name": _to_text(field_name),
                    "yes_value": _to_text(yes_value),
                    "yes_compare": _to_text(yes_cmp),
                    "yes_norm": _to_text(yes_norm),
                    "rows": debug_rows,
                }
            )
        except Exception:
            pass

        # 可选：打印更多 sheet 记录样本（可开启环境变量）
        if DEBUG_DUMP_SHEET:
            try:
                debug_dump_sheet_migrate_field(
                    auth,
                    file_id=file_id,
                    sheet_id=sheet_id,
                    field_name=field_name,
                    limit=DEBUG_DUMP_LIMIT,
                )
            except Exception:
                pass
    return cnt


def _template_download_env_int(name, default):
    try:
        v = int(os.getenv(name, "").strip())
        return v if v > 0 else default
    except Exception:
        return default


def _template_download_env_float(name, default):
    try:
        v = float(os.getenv(name, "").strip())
        return v if v >= 0 else default
    except Exception:
        return default


def _urllib_url_errors():
    """
    Py2: urllib2.URLError / HTTPError
    Py3: urllib.error.URLError / HTTPError（urllib.request 无 URLError 属性）
    """
    try:
        URLError = _urllib2.URLError  # type: ignore[attr-defined]
        HTTPError = _urllib2.HTTPError  # type: ignore[attr-defined]
        return URLError, HTTPError
    except Exception:
        import urllib.error as _urllib_error  # type: ignore

        return _urllib_error.URLError, _urllib_error.HTTPError


def _download_transient_error(err):
    """
    判断是否为可重试的网络/服务端瞬时错误（超时、连接失败、502/503 等）。
    """
    URLError, HTTPError = _urllib_url_errors()
    if isinstance(err, HTTPError):
        try:
            code = int(getattr(err, "code", 0) or 0)
        except Exception:
            code = 0
        return code in (408, 429, 500, 502, 503, 504)
    if isinstance(err, URLError):
        reason = getattr(err, "reason", None)
        if reason is not None:
            en = getattr(reason, "errno", None)
            try:
                if en is not None and int(en) in (110, 111, 11, 113, 101, 32, 10060, 10061):
                    return True
            except Exception:
                pass
            rtxt = _to_text(reason).lower()
            if "timed out" in rtxt or "temporarily unavailable" in rtxt:
                return True
        etxt = _to_text(err).lower()
        if "timed out" in etxt or "connection timed out" in etxt:
            return True
    try:
        import socket

        if isinstance(err, socket.timeout):
            return True
    except Exception:
        pass
    return False


def _url_encode_non_ascii_path(url):
    """
    urllib2 在 py2 下对包含中文的 unicode URL 处理很差，常见报错：
    "'ascii' codec can't encode characters ...".
    这里将 URL 的 path 部分做 percent-encode，避免中文路径导致请求构造失败。
    """
    u = _to_text(url).strip()
    if not u:
        return u
    try:
        parts = _urlparse.urlsplit(u)
    except Exception:
        # urlparse 失败则原样返回
        return u
    try:
        scheme = parts.scheme
        netloc = parts.netloc
        path = parts.path or ""
        query = parts.query or ""
        fragment = parts.fragment or ""
    except Exception:
        return u

    # 已经包含百分号编码时保留 '%'
    try:
        enc_path = _url_quote(path, safe="/%")
    except Exception:
        enc_path = path
    try:
        return _urlparse.urlunsplit((scheme, netloc, enc_path, query, fragment))
    except Exception:
        return u


def download_to_tempfile(url, timeout=None, max_attempts=None):
    """
    下载模板 xlsx 到临时文件。

    连接超时等可通过环境变量放宽（执行环境需能访问 URL 所在网络）：
    - KINGSOFT_TEMPLATE_DOWNLOAD_TIMEOUT：单次 socket 超时秒数，默认 120
    - KINGSOFT_TEMPLATE_DOWNLOAD_MAX_ATTEMPTS：最大尝试次数，默认 4
    - KINGSOFT_TEMPLATE_DOWNLOAD_BACKOFF_SEC：重试间隔基数秒，默认 2（第 n 次重试前 sleep 约 n*基数）
    """
    url = _to_text(url).strip()
    if not url:
        raise RuntimeError("模板下载 URL 为空")
    url_req = _url_encode_non_ascii_path(url)

    if timeout is None:
        timeout = _template_download_env_int("KINGSOFT_TEMPLATE_DOWNLOAD_TIMEOUT", 120)
    if max_attempts is None:
        max_attempts = _template_download_env_int("KINGSOFT_TEMPLATE_DOWNLOAD_MAX_ATTEMPTS", 4)
    backoff_sec = _template_download_env_float("KINGSOFT_TEMPLATE_DOWNLOAD_BACKOFF_SEC", 2.0)

    if max_attempts < 1:
        max_attempts = 1

    URLError, HTTPError = _urllib_url_errors()
    last_err = None
    url_preview = url if len(url) <= 160 else url[:160] + "..."

    for attempt in range(1, max_attempts + 1):
        req = _urllib2.Request(url_req, headers={"User-Agent": "Mozilla/5.0"})
        resp = None
        try:
            resp = _urllib2.urlopen(req, timeout=timeout)
        except (URLError, HTTPError) as e:
            last_err = e
            if attempt < max_attempts and _download_transient_error(e):
                try:
                    time.sleep(backoff_sec * attempt)
                except Exception:
                    pass
                continue
            break

        try:
            status = getattr(resp, "status", 200)
            if status != 200:
                raise RuntimeError("下载模板失败 HTTP {0}: url={1!r}".format(status, url))
            data = resp.read()
        finally:
            try:
                if resp is not None:
                    resp.close()
            except Exception:
                pass

        fd, path = tempfile.mkstemp(suffix=".xlsx", prefix="kingsoft_template_")
        os.close(fd)
        with open(path, "wb") as f:
            f.write(data)
        return path

    msg = _to_text(last_err) if last_err is not None else "未知错误"
    raise RuntimeError(
        "下载模板失败（尝试 {0} 次，单次超时 {1}s）：{2}\n"
        "URL 预览: {3}\n"
        "若为连接超时，请确认执行机能否访问该地址（防火墙/无外网/需代理等）。"
        "可调环境变量：KINGSOFT_TEMPLATE_DOWNLOAD_TIMEOUT、KINGSOFT_TEMPLATE_DOWNLOAD_MAX_ATTEMPTS、"
        "KINGSOFT_TEMPLATE_DOWNLOAD_BACKOFF_SEC。".format(max_attempts, timeout, msg, url_preview)
    )


def parse_headers_from_xlsx_second_col(xlsx_path, min_row=2):
    """
    解析 xlsx 第一个工作表的第二列（B列）作为表头。
    - 若环境已安装 openpyxl：优先使用
    - 否则使用标准库 zip+xml 解析（兼容本地网络无法 pip 安装）
    """
    try:
        min_row = int(min_row)
    except Exception:
        min_row = 2
    if min_row <= 0:
        min_row = 1
    # 1) 优先 openpyxl（如果环境自带）
    try:
        import openpyxl  # type: ignore

        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
        try:
            ws = wb.worksheets[0]
            vals = []
            for row in ws.iter_rows(min_row=min_row, min_col=2, max_col=2, values_only=True):
                vals.append(row[0] if row else None)
        finally:
            try:
                wb.close()
            except Exception:
                pass
        return _normalize_headers(vals)
    except Exception:
        # 2) 标准库解析
        vals = _xlsx_read_first_sheet_col_values(xlsx_path, target_col_letters="B", min_row=min_row)
        return _normalize_headers(vals)


def parse_headers_from_xlsx_header_row(xlsx_path, header_row=2, min_col=1, max_col=200):
    """
    解析 xlsx 第一个工作表的某一行作为表头（默认第 2 行）。
    适配你的模板：第 1 行是英文 key，第 2 行是中文展示名（如：姓名、性别）。
    - 若环境已安装 openpyxl：优先使用
    - 否则使用标准库 zip+xml 解析
    """
    try:
        header_row = int(header_row)
    except Exception:
        header_row = 2
    if header_row <= 0:
        header_row = 1
    try:
        min_col = int(min_col)
    except Exception:
        min_col = 1
    if min_col <= 0:
        min_col = 1
    try:
        max_col = int(max_col)
    except Exception:
        max_col = 200
    if max_col < min_col:
        max_col = min_col

    # 1) 优先 openpyxl
    try:
        import openpyxl  # type: ignore

        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
        try:
            ws = wb.worksheets[0]
            vals = []
            for row in ws.iter_rows(
                min_row=header_row,
                max_row=header_row,
                min_col=min_col,
                max_col=max_col,
                values_only=True,
            ):
                # row 是一个 tuple
                for v in (row or []):
                    vals.append(v)
                break
        finally:
            try:
                wb.close()
            except Exception:
                pass
        return _normalize_headers(vals)
    except Exception:
        # 2) 标准库解析：读 header_row 这一行的所有 cell
        vals = _xlsx_read_first_sheet_row_values(xlsx_path, row_num=header_row)
        return _normalize_headers(vals)


def _xlsx_read_first_sheet_row_values(xlsx_path, row_num=2):
    """
    纯标准库读取 xlsx：读取第一个工作表的指定行，按列序返回值列表。
    """
    col_to_val = _xlsx_read_first_sheet_row_map(xlsx_path, row_num=row_num)
    cols = sorted(col_to_val.keys(), key=lambda x: _excel_col_letters_to_index(x))
    return [col_to_val[c] for c in cols]


def parse_headers_from_xlsx_combo_rows(xlsx_path, code_row=1, label_row=2, max_col=200):
    """
    解析模板表头：第 code_row 行为英文 code，第 label_row 行为中文 label，
    最终字段名：label(code)。例如：姓名(xm)
    """
    try:
        code_row = int(code_row)
    except Exception:
        code_row = 1
    try:
        label_row = int(label_row)
    except Exception:
        label_row = 2
    if code_row <= 0:
        code_row = 1
    if label_row <= 0:
        label_row = 2
    try:
        max_col = int(max_col)
    except Exception:
        max_col = 200
    if max_col <= 0:
        max_col = 200

    # 1) 优先 openpyxl
    try:
        import openpyxl  # type: ignore

        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
        try:
            ws = wb.worksheets[0]
            codes = []
            labels = []
            for row in ws.iter_rows(
                min_row=code_row,
                max_row=code_row,
                min_col=1,
                max_col=max_col,
                values_only=True,
            ):
                codes = list(row or [])
                break
            for row in ws.iter_rows(
                min_row=label_row,
                max_row=label_row,
                min_col=1,
                max_col=max_col,
                values_only=True,
            ):
                labels = list(row or [])
                break
        finally:
            try:
                wb.close()
            except Exception:
                pass

        n = max(len(codes), len(labels))
        merged = []
        for i in range(n):
            code = _to_text(codes[i]).strip() if i < len(codes) else ""
            lab = _to_text(labels[i]).strip() if i < len(labels) else ""
            if lab and code:
                merged.append(u"{0}({1})".format(lab, code))
            elif lab:
                merged.append(lab)
            elif code:
                merged.append(code)
        return _normalize_headers(merged)
    except Exception:
        cmap = _xlsx_read_first_sheet_row_map(xlsx_path, row_num=code_row)
        lmap = _xlsx_read_first_sheet_row_map(xlsx_path, row_num=label_row)
        cols = set(list(cmap.keys()) + list(lmap.keys()))
        cols_sorted = sorted(cols, key=lambda x: _excel_col_letters_to_index(x))
        merged = []
        for col in cols_sorted:
            code = _to_text(cmap.get(col)).strip()
            lab = _to_text(lmap.get(col)).strip()
            if lab and code:
                merged.append(u"{0}({1})".format(lab, code))
            elif lab:
                merged.append(lab)
            elif code:
                merged.append(code)
        return _normalize_headers(merged)

def _normalize_headers(values):
    seen = set()
    headers = []
    for v in values or []:
        s = _to_text(v).strip() if v is not None else ""
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        headers.append(s)
    return headers


def _strip_system_default_headers(headers):
    """
    去掉系统自动生成的固定字段（名称/数量/日期/状态）。
    注意：这里只影响“我们要创建的字段”，不会删除已存在字段。
    """
    if not headers:
        return []
    sys_norm = set([_normalize_compare_text(x).lower() for x in SYSTEM_DEFAULT_FIELDS])
    out = []
    for h in headers:
        hn = _normalize_compare_text(h).lower()
        if hn in sys_norm:
            continue
        out.append(h)
    return out


def _extract_sheet_fields_from_schema(schema, sheet_id=None, sheet_name=None):
    """
    从 schema.data.sheets 中取某个 sheet 的 fields 列表（若 schema 包含）。
    返回 list[dict]，每个 dict 至少包含 id/name/display_name 等字段。
    """
    try:
        sheets = (schema.get("data") or {}).get("sheets") or []
    except Exception:
        sheets = []
    if not isinstance(sheets, list):
        return []
    target = None
    if sheet_id:
        sid = _to_text(sheet_id).strip()
        for s in sheets:
            if isinstance(s, dict) and _to_text(s.get("id")).strip() == sid:
                target = s
                break
    if target is None and sheet_name:
        for s in sheets:
            if isinstance(s, dict) and _is_equal_ignore_case(_get_sheet_name(s), sheet_name):
                target = s
                break
    if not isinstance(target, dict):
        return []
    fields = target.get("fields") or target.get("field") or []
    if isinstance(fields, list):
        return [x for x in fields if isinstance(x, dict)]
    return []


def delete_sheet_fields(auth, file_id, sheet_id, field_ids):
    """
    删除字段（参考开放平台：POST /openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/fields/delete）
    body: {"fields": ["field_id", ...]}
    """
    ids = [ _to_text(x).strip() for x in (field_ids or []) if _to_text(x).strip() ]
    if not ids:
        return {"skipped": True, "reason": "no_field_ids"}
    path = API_PATH_FILE_SHEET_FIELDS_DELETE.format(file_id=_to_text(file_id), sheet_id=_to_text(sheet_id))
    method = "POST"
    body = json.dumps({"fields": ids}, ensure_ascii=False)
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed = get_request_headers(method=method, url_for_sign=url_for_sign, body=body, content_type=CONTENT_TYPE_JSON)
    headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
    }
    headers.update(_auth_headers(auth))
    headers.update(signed)
    headers["Content-Type"] = CONTENT_TYPE_JSON
    status, text = _http_request(method, path, headers=headers, body=body.encode("utf-8"))
    if status != 200:
        raise RuntimeError("删除字段失败 HTTP {0}: {1}".format(status, text[:2000]))
    try:
        return json.loads(text)
    except Exception:
        return {"raw": text}


def update_sheet_fields(auth, file_id, sheet_id, fields_to_update):
    """
    更新字段（参考开放平台：POST /openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/fields/update）
    body: {"fields": [{"id": "...", "name": "新名字", ...}, ...]}
    """
    items = []
    for f in fields_to_update or []:
        if not isinstance(f, dict):
            continue
        fid = _to_text(f.get("id") or "").strip()
        if not fid:
            continue
        # 只传我们需要更新的字段，最小化风险
        item = {"id": fid}
        if f.get("name") is not None:
            item["name"] = _to_text(f.get("name"))
        if len(item.keys()) > 1:
            items.append(item)
    if not items:
        return {"skipped": True, "reason": "no_fields_to_update"}

    path = API_PATH_FILE_SHEET_FIELDS_UPDATE.format(file_id=_to_text(file_id), sheet_id=_to_text(sheet_id))
    method = "POST"
    body = json.dumps({"fields": items}, ensure_ascii=False)
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed = get_request_headers(method=method, url_for_sign=url_for_sign, body=body, content_type=CONTENT_TYPE_JSON)
    headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
    }
    headers.update(_auth_headers(auth))
    headers.update(signed)
    headers["Content-Type"] = CONTENT_TYPE_JSON
    status, text = _http_request(method, path, headers=headers, body=body.encode("utf-8"))
    if status != 200:
        raise RuntimeError("更新字段失败 HTTP {0}: {1}".format(status, text[:2000]))
    try:
        return json.loads(text)
    except Exception:
        return {"raw": text}


def delete_system_default_fields_if_present(auth, file_id, sheet_id, sheet_name=None):
    """
    在创建 sheet 后删除系统默认字段：名称/数量/日期/状态。
    仅在能获取到字段列表并定位到字段 id 时才会删除。
    """
    try:
        schema = get_file_schema(auth, file_id=file_id)
    except Exception:
        schema = {}
    fields = _extract_sheet_fields_from_schema(schema, sheet_id=sheet_id, sheet_name=sheet_name)
    if not fields:
        return {"skipped": True, "reason": "schema_has_no_fields"}
    sys_norm = set([_normalize_compare_text(x).lower() for x in SYSTEM_DEFAULT_FIELDS])
    del_ids = []
    del_names = []
    for f in fields:
        # 兼容字段名键
        nm = ""
        for k in ("name", "display_name", "title"):
            if f.get(k):
                nm = _to_text(f.get(k)).strip()
                if nm:
                    break
        if not nm:
            continue
        if _normalize_compare_text(nm).lower() in sys_norm:
            fid = _to_text(f.get("id") or "").strip()
            if fid:
                del_ids.append(fid)
                del_names.append(nm)
    if not del_ids:
        return {"skipped": True, "reason": "no_system_fields_found"}
    resp = delete_sheet_fields(auth, file_id=file_id, sheet_id=sheet_id, field_ids=del_ids)
    resp["_deleted_field_names"] = del_names
    resp["_deleted_field_ids"] = del_ids
    return resp


def rename_name_field_if_present(auth, file_id, sheet_id, new_name):
    """
    系统默认的“名称”字段在部分表中是主字段，可能不允许删除。
    若存在则尝试把它重命名为模板第一个表头（如“姓名”），以达到“没有名称列”的效果。
    返回 (did_rename:bool, details:dict)。
    """
    new_name = _to_text(new_name).strip()
    if not new_name:
        return False, {"skipped": True, "reason": "empty_new_name"}
    try:
        schema = get_file_schema(auth, file_id=file_id)
    except Exception:
        schema = {}
    fields = _extract_sheet_fields_from_schema(schema, sheet_id=sheet_id)
    if not fields:
        return False, {"skipped": True, "reason": "schema_has_no_fields"}
    target_id = ""
    for f in fields:
        nm = ""
        for k in ("name", "display_name", "title"):
            if f.get(k):
                nm = _to_text(f.get(k)).strip()
                if nm:
                    break
        if _normalize_compare_text(nm).lower() == _normalize_compare_text(u"名称").lower():
            target_id = _to_text(f.get("id") or "").strip()
            break
    if not target_id:
        return False, {"skipped": True, "reason": "name_field_not_found"}
    resp = update_sheet_fields(auth, file_id=file_id, sheet_id=sheet_id, fields_to_update=[{"id": target_id, "name": new_name}])
    return True, {"updated_field_id": target_id, "new_name": new_name, "resp": resp}


_CELL_REF_RE = re.compile(r"^([A-Z]+)(\d+)$")


def _excel_col_letters_to_index(letters):
    """
    Excel 列字母 -> 1-based 列索引（A=1, Z=26, AA=27）。
    """
    s = _to_text(letters).upper().strip()
    if not s:
        return 0
    n = 0
    for ch in s:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
        else:
            return 0
    return n


def _excel_col_index_to_letters(idx):
    """
    1-based 列索引 -> Excel 列字母（1->A, 27->AA）。
    """
    try:
        n = int(idx)
    except Exception:
        return ""
    if n <= 0:
        return ""
    out = []
    while n > 0:
        n, r = divmod(n - 1, 26)
        out.append(chr(ord("A") + r))
    return "".join(reversed(out))


def _xlsx_read_first_sheet_row_map(xlsx_path, row_num=2):
    """
    纯标准库读取 xlsx：读取第一个工作表的指定行，返回 {colLetters: value}。
    colLetters 使用 Excel 列字母（A/AA...）。
    """
    try:
        row_num = int(row_num)
    except Exception:
        row_num = 2
    if row_num <= 0:
        row_num = 1

    with zipfile.ZipFile(xlsx_path, "r") as zf:
        shared = _xlsx_load_shared_strings(zf)
        sheet_xml_path = _xlsx_pick_first_sheet_xml_path(zf)
        if not sheet_xml_path:
            raise RuntimeError("xlsx 中未找到工作表 XML（期望 xl/worksheets/sheet1.xml）")

        xml_bytes = zf.read(sheet_xml_path)
        try:
            root = ET.fromstring(xml_bytes)
        except Exception as e:
            raise RuntimeError("解析 sheet xml 失败：{0}".format(e))

        col_to_val = {}
        for c in root.iter():
            if not (c.tag.endswith("}c") or c.tag == "c"):
                continue
            r = c.get("r") or ""
            m = _CELL_REF_RE.match(_to_text(r).upper())
            if not m:
                continue
            col_letters, row_num_s = m.group(1), m.group(2)
            try:
                rn = int(row_num_s)
            except Exception:
                continue
            if rn != row_num:
                continue
            t = c.get("t") or ""
            val = _xlsx_cell_value(c, t=t, shared_strings=shared)
            col_to_val[col_letters] = val
        return col_to_val


def _xlsx_read_first_sheet_col_values(xlsx_path, target_col_letters="B", min_row=1):
    """
    纯标准库读取 xlsx：
    - 默认读取 xl/worksheets/sheet1.xml
    - 支持 sharedStrings (xl/sharedStrings.xml)
    - 只抽取指定列（如 'B'）的单元格值，按行号升序返回列表
    """
    target_col_letters = _to_text(target_col_letters).upper().strip()
    if not target_col_letters:
        target_col_letters = "B"
    try:
        min_row = int(min_row)
    except Exception:
        min_row = 1
    if min_row <= 0:
        min_row = 1

    with zipfile.ZipFile(xlsx_path, "r") as zf:
        shared = _xlsx_load_shared_strings(zf)
        sheet_xml_path = _xlsx_pick_first_sheet_xml_path(zf)
        if not sheet_xml_path:
            raise RuntimeError("xlsx 中未找到工作表 XML（期望 xl/worksheets/sheet1.xml）")

        xml_bytes = zf.read(sheet_xml_path)
        # ElementTree 处理 namespace：用通配符匹配标签末尾
        try:
            root = ET.fromstring(xml_bytes)
        except Exception as e:
            raise RuntimeError("解析 sheet xml 失败：{0}".format(e))

        # 收集 (row_num -> value)
        row_to_val = {}
        # 遍历所有 cell：<c r="B2" t="s"><v>0</v></c>
        for c in root.iter():
            if not (c.tag.endswith("}c") or c.tag == "c"):
                continue
            r = c.get("r") or ""
            m = _CELL_REF_RE.match(_to_text(r).upper())
            if not m:
                continue
            col_letters, row_num_s = m.group(1), m.group(2)
            if col_letters != target_col_letters:
                continue
            try:
                row_num = int(row_num_s)
            except Exception:
                continue
            if row_num < min_row:
                continue

            t = c.get("t") or ""
            val = _xlsx_cell_value(c, t=t, shared_strings=shared)
            row_to_val[row_num] = val

        # 按行号输出
        out = []
        for rn in sorted(row_to_val.keys()):
            out.append(row_to_val[rn])
        return out


def _xlsx_pick_first_sheet_xml_path(zf):
    # 最常见：xl/worksheets/sheet1.xml
    if "xl/worksheets/sheet1.xml" in zf.namelist():
        return "xl/worksheets/sheet1.xml"
    # 兜底：找第一个 xl/worksheets/sheet*.xml
    candidates = [n for n in zf.namelist() if n.startswith("xl/worksheets/") and n.endswith(".xml")]
    candidates.sort()
    return candidates[0] if candidates else None


def _xlsx_load_shared_strings(zf):
    """
    读取 sharedStrings.xml，返回 list[str]
    """
    path = "xl/sharedStrings.xml"
    if path not in zf.namelist():
        return []
    try:
        xml_bytes = zf.read(path)
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []

    out = []
    # <sst><si><t>文本</t></si> 或富文本 <si><r><t>..</t></r></si>
    for si in root.iter():
        if not (si.tag.endswith("}si") or si.tag == "si"):
            continue
        parts = []
        for t in si.iter():
            if not (t.tag.endswith("}t") or t.tag == "t"):
                continue
            if t.text:
                parts.append(t.text)
        out.append("".join(parts))
    return out


def _xlsx_cell_value(c_elem, t, shared_strings):
    """
    根据 cell 类型返回值：
    - t="s": sharedStrings 索引
    - t="inlineStr": is/t
    - 其他：v 文本/数字
    """
    t = _to_text(t)
    # 取 <v> 或 <is><t>
    v_text = None
    if t == "inlineStr":
        for ch in c_elem.iter():
            if ch.tag.endswith("}t") or ch.tag == "t":
                v_text = ch.text
                break
        return v_text

    for ch in c_elem:
        if ch.tag.endswith("}v") or ch.tag == "v":
            v_text = ch.text
            break

    if v_text is None:
        return None

    if t == "s":
        try:
            idx = int(_to_text(v_text).strip())
            if 0 <= idx < len(shared_strings):
                return shared_strings[idx]
        except Exception:
            return None
        return None

    return v_text


def create_graph_file(
    auth,
    target_drive_id,
    name,
    parent_id="0",
):
    """
    创建文件（.dbt）。
    签名策略不确定时，做两次尝试：
    - 尝试1：按现有 openapi 规则，sign_path_override 使用 "/v7/..."（去掉 /graph 前缀）
    - 尝试2：不覆盖 sign_path_override（使用完整 path）
    """
    # py2: 避免 str.format 拼接 unicode 触发 ascii 编码错误
    name = _to_text(name or "").strip()
    if not name.lower().endswith(".dbt"):
        name = name + u".dbt"

    path = GRAPH_PATH_CREATE_FILE.format(drive_id=target_drive_id, parent_id=str(parent_id))
    body_dict = {"file_type": "file", "name": name, "on_name_conflict": "rename"}
    body = json.dumps(body_dict, ensure_ascii=False)
    method = "POST"

    attempts = []
    # 兜底B：/graph 不参与签名 → 只签 /v7/...
    if path.startswith("/graph/"):
        attempts.append(("sign_without_graph_prefix", path[len("/graph") :]))
    attempts.append(("sign_full_path", None))

    last_err = ""
    for tag, sign_override in attempts:
        url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
        signed = get_request_headers(
            method=method,
            url_for_sign=url_for_sign,
            body=body,
            content_type=CONTENT_TYPE_JSON,
            sign_path_override=sign_override,
        )
        headers = {
            "Accept": HTTP_HEADER_ACCEPT,
            "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
            "User-Agent": HTTP_HEADER_USER_AGENT,
            "Connection": HTTP_HEADER_CONNECTION,
        }
        headers.update(_auth_headers(auth))
        headers.update(signed)
        headers["Content-Type"] = CONTENT_TYPE_JSON

        status, text = _http_request(method, path, headers=headers, body=body.encode("utf-8"))
        if status == 200:
            resp = json.loads(text)
            data = resp.get("data") if isinstance(resp, dict) else None
            if isinstance(data, dict):
                data["_sign_attempt"] = tag
                return data
            return resp if isinstance(resp, dict) else {"raw": resp, "_sign_attempt": tag}

        last_err = "{0}: HTTP {1}: {2}".format(tag, status, text[:2000])

    raise RuntimeError("创建目标 .dbt 文件失败：{0}".format(last_err))


def create_sheet(auth, file_id, sheet_name):
    """
    优先创建工作表；失败则兜底重命名默认第一张表。
    """
    # 优先：sheets/create
    path = API_PATH_SHEETS_CREATE.format(file_id=file_id)
    method = "POST"
    body_dict = {"name": sheet_name, "fields": []}
    body = json.dumps(body_dict, ensure_ascii=False)
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed = get_request_headers(method=method, url_for_sign=url_for_sign, body=body, content_type=CONTENT_TYPE_JSON)
    headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
    }
    headers.update(_auth_headers(auth))
    headers.update(signed)
    headers["Content-Type"] = CONTENT_TYPE_JSON
    status, text = _http_request(method, path, headers=headers, body=body.encode("utf-8"))
    if status == 200:
        return json.loads(text)

    # 兜底：重命名 schema 第一张表
    schema = get_file_schema(auth, file_id=file_id)
    sheets = (schema.get("data") or {}).get("sheets") or []
    if not isinstance(sheets, list) or not sheets:
        raise RuntimeError("创建 sheet 失败且 schema 无 sheets。create_http={0} resp={1}".format(status, text[:2000]))
    first = sheets[0] if isinstance(sheets[0], dict) else None
    if not first:
        raise RuntimeError("创建 sheet 失败且 schema sheets 格式异常。create_http={0} resp={1}".format(status, text[:2000]))
    sid = _get_sheet_id(first)
    if not sid:
        raise RuntimeError("创建 sheet 失败且无法获取默认 sheet_id。create_http={0} resp={1}".format(status, text[:2000]))

    up_path = API_PATH_SHEET_UPDATE.format(file_id=file_id, sheet_id=sid)
    up_body = json.dumps({"name": sheet_name}, ensure_ascii=False)
    up_url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, up_path)
    up_signed = get_request_headers(method="POST", url_for_sign=up_url_for_sign, body=up_body, content_type=CONTENT_TYPE_JSON)
    up_headers = {
        "Accept": HTTP_HEADER_ACCEPT,
        "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
        "User-Agent": HTTP_HEADER_USER_AGENT,
        "Connection": HTTP_HEADER_CONNECTION,
    }
    up_headers.update(_auth_headers(auth))
    up_headers.update(up_signed)
    up_headers["Content-Type"] = CONTENT_TYPE_JSON
    up_status, up_text = _http_request("POST", up_path, headers=up_headers, body=up_body.encode("utf-8"))
    if up_status != 200:
        raise RuntimeError(
            "创建 sheet 失败（HTTP {0}）且重命名兜底失败（HTTP {1}）。create_resp={2} update_resp={3}".format(
                status, up_status, text[:1000], up_text[:1000]
            )
        )
    return json.loads(up_text)


def _extract_sheet_from_create_or_update_resp(resp):
    data = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(data, dict) and isinstance(data.get("sheet"), dict):
        return data["sheet"]
    if isinstance(data, dict) and isinstance(data.get("sheets"), list) and data["sheets"]:
        s0 = data["sheets"][0]
        return s0 if isinstance(s0, dict) else None
    return None


def create_fields(
    auth,
    file_id,
    sheet_id,
    headers,
    batch_size=50,
):
    path = API_PATH_FILE_SHEET_FIELDS.format(file_id=file_id, sheet_id=sheet_id)
    method = "POST"
    for i in range(0, len(headers), batch_size):
        chunk = headers[i : i + batch_size]
        fields_payload = [{"name": h, "type": "SingleLineText"} for h in chunk]
        body_dict = {"fields": fields_payload, "prefer_id": False}
        body = json.dumps(body_dict, ensure_ascii=False)
        url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
        signed = get_request_headers(method=method, url_for_sign=url_for_sign, body=body, content_type=CONTENT_TYPE_JSON)
        headers_req = {
            "Accept": HTTP_HEADER_ACCEPT,
            "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
            "User-Agent": HTTP_HEADER_USER_AGENT,
            "Connection": HTTP_HEADER_CONNECTION,
        }
        headers_req.update(_auth_headers(auth))
        headers_req.update(signed)
        headers_req["Content-Type"] = CONTENT_TYPE_JSON
        status, text = _http_request(method, path, headers=headers_req, body=body.encode("utf-8"))
        if status != 200:
            raise RuntimeError("创建字段失败 HTTP {0}: {1}".format(status, text[:2000]))
        resp = json.loads(text)
        code = resp.get("code", 0) if isinstance(resp, dict) else 0
        if code not in (0, "0", None):
            raise RuntimeError("创建字段失败：code={0} resp={1}".format(code, text[:2000]))


def _ensure_dbt_name(name):
    # py2: 统一转 unicode，避免后续拼接触发 ascii 编码错误
    n = _to_text(name or "").strip()
    if not n:
        return "new.dbt"
    if n.lower().endswith(".dbt"):
        return n
    return n + u".dbt"


def _find_sheet_in_schema(schema, sheet_name):
    """
    在 schema.data.sheets 中按名称查找 sheet dict（忽略大小写）。
    找不到返回 None。
    """
    try:
        sheets = (schema.get("data") or {}).get("sheets") or []
    except Exception:
        sheets = []
    if not isinstance(sheets, list):
        sheets = []
    want = _to_text(sheet_name).strip()
    if not want:
        return None
    for s in sheets:
        if not isinstance(s, dict):
            continue
        if _is_equal_ignore_case(_get_sheet_name(s), want):
            return s
    return None


def main(argv):
    if len(argv) >= 2 and argv[1] in ("-h", "--help"):
        print(
            "用法：python create-kingsoft-prod-all.py <doc_lib_name> <file_name> <sheet_name> [--dry-run]\n"
            "示例：python create-kingsoft-prod-all.py \"上报服务\" \"QWERTYUIO\" \"本市街道\" --dry-run\n"
            "（不再支持打印源 sheet 行数据；汇总见输出 JSON 中 run_summary。）\n"
        )
        return 0

    dry_run = "--dry-run" in argv
    args = [a for a in argv[1:] if a != "--dry-run"]
    if len(args) != 3:
        _die("参数不足。需要3个入参：doc_lib_name file_name sheet_name（可加 --dry-run）", exit_code=2)

    src_doc_lib_name, src_file_name, src_sheet_name = args

    auth = app_authorize(DEFAULT_CLIENT_ID, DEFAULT_CLIENT_SECRET)

    # 1) 源文档库
    src_doclibs = doclib_search(auth, k<INTERNAL_B64>)
    if not src_doclibs:
        # 搜索接口可能对 keyword 匹配较严格；兜底拉取更多候选再本地模糊匹配
        src_doclibs = doclib_search(auth, keyword="")
    # 诊断：若候选存在但解析不出名称，输出样例结构，便于快速适配网关返回结构
    try:
        names_probe = [_get_doclib_name(x) for x in (src_doclibs[:10] if isinstance(src_doclibs, list) else [])]
        if src_doclibs and not [n for n in names_probe if n]:
            sample = src_doclibs[0]
            print(
                "警告：doclib_search 有返回但无法解析名称字段。sample_keys={0} sample={1}".format(
                    sorted(list(sample.keys())) if isinstance(sample, dict) else type(sample).__name__,
                    _to_text(json.dumps(sample, ensure_ascii=False)[:800]) if isinstance(sample, dict) else _to_text(sample),
                )
            )
    except Exception:
        pass
    src_doclib = _pick_best(src_doc_lib_name, src_doclibs, _get_doclib_name)
    source_drive_id = _get_drive_id(src_doclib)
    if not source_drive_id:
        raise RuntimeError("源文档库 drive_id 为空")

    # 2) 源文件
    file_items = files_search(auth, keyword=src_file_name, drive_ids=[source_drive_id])
    file_items_filtered = [x for x in file_items if _match_name(src_file_name, _get_file_name(x))]
    try:
        if file_items and not [ _get_file_name(x) for x in file_items[:10] if _get_file_name(x) ]:
            sample = file_items[0]
            print(
                "警告：files_search 有返回但无法解析文件名字段。sample_keys={0} sample={1}".format(
                    sorted(list(sample.keys())) if isinstance(sample, dict) else type(sample).__name__,
                    _to_text(json.dumps(sample, ensure_ascii=False)[:800]) if isinstance(sample, dict) else _to_text(sample),
                )
            )
    except Exception:
        pass
    src_file = _pick_best(src_file_name, file_items_filtered or file_items, _get_file_name)
    source_file_id = _get_file_id(src_file)
    source_file_name = _get_file_name(src_file) or src_file_name
    if not source_file_id:
        raise RuntimeError("源文件 file_id 为空")

    # 3) 源 sheet
    schema = get_file_schema(auth, file_id=source_file_id)
    sheets = (schema.get("data") or {}).get("sheets") or []
    if not isinstance(sheets, list) or not sheets:
        raise RuntimeError("源文件 schema 无 sheets：file_id={0}".format(source_file_id))
    try:
        sheet_names_probe = [_get_sheet_name(s) for s in sheets[:10] if isinstance(s, dict)]
        if sheets and not [n for n in sheet_names_probe if n]:
            sample = sheets[0] if sheets else None
            print(
                "警告：schema.sheets 有返回但无法解析 sheet 名称字段。sample_keys={0} sample={1}".format(
                    sorted(list(sample.keys())) if isinstance(sample, dict) else type(sample).__name__,
                    _to_text(json.dumps(sample, ensure_ascii=False)[:800]) if isinstance(sample, dict) else _to_text(sample),
                )
            )
    except Exception:
        pass
    sheets_filtered = [s for s in sheets if isinstance(s, dict) and _match_name(src_sheet_name, _get_sheet_name(s))]
    src_sheet = _pick_best(src_sheet_name, sheets_filtered or [s for s in sheets if isinstance(s, dict)], _get_sheet_name)
    if not isinstance(src_sheet, dict):
        raise RuntimeError("源 sheet 解析失败")
    source_sheet_id = _get_sheet_id(src_sheet)
    source_sheet_name = _get_sheet_name(src_sheet) or src_sheet_name
    if not source_sheet_id:
        raise RuntimeError("源 sheet_id 为空")

    # 4) 收集所有“是否迁移=是”的记录（可能有多条）
    migrate_items = list(iter_migrate_yes_items(auth, file_id=source_file_id, sheet_id=source_sheet_id))
    migrate_yes_count = len(migrate_items)
    if migrate_yes_count <= 0:
        migrate_field_samples = None
        try:
            migrate_field_samples = collect_migrate_field_samples(
                auth, file_id=source_file_id, sheet_id=source_sheet_id, field_name=MIGRATE_FLAG_FIELD_NAME, yes_value=MIGRATE_FLAG_YES_VALUE, limit=10
            )
        except Exception:
            pass
        _print_json(
            {
                "source_drive_id": source_drive_id,
                "source_file_id": source_file_id,
                "source_sheet_id": source_sheet_id,
                "migrate_yes_count": migrate_yes_count,
                "skipped": True,
                "reason": "源 sheet 中不存在 {0}={1} 的记录，跳过创建目标多维表格文件".format(
                    MIGRATE_FLAG_FIELD_NAME, MIGRATE_FLAG_YES_VALUE
                ),
                "migrate_field_samples": migrate_field_samples,
                "run_summary": {
                    "documents_created": 0,
                    "documents_reused_existing_no_create": 0,
                    "sheets_created": 0,
                    "sheets_skipped_already_exist": 0,
                },
                "summary_line": u"新建文档=0，已存在未新建文档=0，新建Sheet=0，已存在未新建Sheet=0",
            }
        )
        return 0

    if dry_run:
        sample = []
        for it in migrate_items[:10]:
            sample.append(
                {
                    "record_id": it.get("record_id"),
                    "report_resource_name": _to_text(it.get("report_resource_name")),
                    "report_table_name": _to_text(it.get("report_table_name")),
                    "template_url": _to_text(it.get("template_url")),
                    "doclib_name": _to_text(it.get("doclib_name")),
                    "doclib_field": _to_text(it.get("doclib_field")),
                }
            )
        _print_json(
            {
                "source_drive_id": source_drive_id,
                "source_file_id": source_file_id,
                "source_sheet_id": source_sheet_id,
                "migrate_yes_count": migrate_yes_count,
                "items_sample": sample,
                "dry_run": True,
                "run_summary": {
                    "documents_created": 0,
                    "documents_reused_existing_no_create": 0,
                    "sheets_created": 0,
                    "sheets_skipped_already_exist": 0,
                    "note": "dry_run 未执行创建，统计为 0",
                },
                "summary_line": u"dry_run：未实际创建；新建文档=0，已存在未新建文档=0，新建Sheet=0，已存在未新建Sheet=0",
            }
        )
        return 0

    # 7) 目标文档库：由配置 sheet「文档库名称」等字段直接给出（不再拼接「-一网共享-手工上报」）
    _doclib_cache = {}

    def _resolve_target_doclib(auth, doclib_text):
        doclib_text = _to_text(doclib_text).strip()
        if not doclib_text:
            raise RuntimeError("无法确定目标表空间：源记录「文档库名称」相关字段为空（已尝试：{0}）".format(
                u",".join([_to_text(x) for x in DOCLIB_FIELD_CANDIDATES])
            ))
        cache_key = doclib_text
        if cache_key in _doclib_cache:
            return _doclib_cache[cache_key]

        tgt_doclibs = doclib_search(auth, keyword=doclib_text)
        if not tgt_doclibs:
            tgt_doclibs = doclib_search(auth, keyword="")

        doclib_l = doclib_text.lower()
        pool = []
        for it in tgt_doclibs or []:
            if not isinstance(it, dict):
                continue
            nm = _get_doclib_name(it)
            nml = _to_text(nm).lower()
            if doclib_l in nml or nml in doclib_l or _match_name_either_contains(doclib_text, nm):
                pool.append(it)

        if not pool:
            pool = [x for x in (tgt_doclibs or []) if isinstance(x, dict)]

        preferred = []
        for it in pool:
            nm = _get_doclib_name(it)
            if re.search(r"^\d+-", _to_text(nm).strip()):
                preferred.append(it)

        use_pool = preferred if preferred else pool
        if not use_pool:
            raise RuntimeError("未找到目标文档库候选：doclib={0!r}".format(doclib_text))

        q = doclib_text
        named = []
        for c in use_pool:
            try:
                n = _get_doclib_name(c) or ""
            except Exception:
                n = ""
            named.append((_to_text(n), c))
        exact = [(n, c) for (n, c) in named if _is_equal_ignore_case(q, n)]
        pick_pool = exact if exact else [(n, c) for (n, c) in named if _match_name_either_contains(q, n)]
        if not pick_pool:
            pick_pool = named
        pick_pool.sort(key=lambda x: (len(x[0] or ""), (x[0] or "").lower()))
        tgt_doclib = pick_pool[0][1]

        target_drive_id = _get_drive_id(tgt_doclib)
        if not target_drive_id:
            raise RuntimeError("目标文档库 drive_id 为空：doclib={0!r}".format(doclib_text))
        _doclib_cache[cache_key] = target_drive_id
        return target_drive_id

    created = []
    skipped = []
    failed = []
    # 目标侧统计：新建 .dbt / 复用已有 .dbt；新建 sheet / 因已存在而跳过
    stat_documents_created = 0
    stat_documents_reused_existing = 0
    stat_sheets_created = 0
    stat_sheets_skipped_existing = 0

    # 8~10) 对每条“迁移=是”的记录，分别下载模板->解析表头->创建文件->创建 sheet->创建字段
    for it in migrate_items:
        record_id = it.get("record_id")
        report_resource_name = it.get("report_resource_name") or ""
        report_table_name = it.get("report_table_name") or ""
        template_url = it.get("template_url") or ""
        doclib_text = it.get("doclib_name") or ""
        doclib_field = it.get("doclib_field") or ""

        if not report_resource_name:
            report_resource_name = source_file_name
        if not report_table_name:
            report_table_name = source_sheet_name

        if not template_url:
            skipped.append(
                {
                    "record_id": record_id,
                    "skipped": True,
                    "reason": "该记录模板下载链接为空，跳过",
                    "report_resource_name": _to_text(report_resource_name),
                    "report_table_name": _to_text(report_table_name),
                }
            )
            continue

        try:
            xlsx_path = None
            try:
                xlsx_path = download_to_tempfile(template_url)

                # 模板：第 1 行为英文 code，第 2 行为中文 label；表头合并为 label(code)
                headers = parse_headers_from_xlsx_combo_rows(xlsx_path, code_row=1, label_row=2, max_col=200)
                headers = _strip_system_default_headers(headers)
            except Exception as e:
                raise
            finally:
                try:
                    if xlsx_path:
                        os.remove(xlsx_path)
                except Exception:
                    pass

            if not headers:
                raise RuntimeError("模板解析出的表头为空（第一个工作表第二列无有效值）")

            target_drive_id = _resolve_target_doclib(auth, doclib_text)

            # 幂等：目标文件若已存在则复用；否则创建
            desired_file_name = _ensure_dbt_name(report_resource_name)
            existed = _find_existing_file_by_exact_name(auth, drive_id=target_drive_id, exact_name=desired_file_name)
            if existed:
                new_file_id = _get_file_id(existed)
                new_file_name_real = _get_file_name(existed) or desired_file_name
                created_file = {"_sign_attempt": "existing_file"}
                stat_documents_reused_existing += 1
            else:
                created_file = create_graph_file(auth, target_drive_id=target_drive_id, name=desired_file_name, parent_id="0")
                new_file_id = _to_text(created_file.get("id") or "").strip()
                new_file_name_real = _to_text(created_file.get("name") or desired_file_name)
                if not new_file_id:
                    raise RuntimeError(
                        "创建文件成功但未返回 id：resp={0}".format(json.dumps(created_file, ensure_ascii=False)[:2000])
                    )
                stat_documents_created += 1

            # 幂等：sheet 若已存在则跳过该条记录
            try:
                schema_now = get_file_schema(auth, file_id=new_file_id)
            except Exception:
                schema_now = {}
            existed_sheet = _find_sheet_in_schema(schema_now, report_table_name)
            if existed_sheet:
                stat_sheets_skipped_existing += 1
                skipped.append(
                    {
                        "record_id": record_id,
                        "skipped": True,
                        "reason": "目标文件/Sheet 已存在，跳过",
                        "report_resource_name": _to_text(report_resource_name),
                        "report_table_name": _to_text(report_table_name),
                        "target_drive_id": target_drive_id,
                        "existing_file_id": new_file_id,
                        "existing_file_name": _to_text(new_file_name_real),
                        "existing_sheet_id": _get_sheet_id(existed_sheet),
                        "existing_sheet_name": _get_sheet_name(existed_sheet),
                    }
                )
                continue

            sheet_resp = create_sheet(auth, file_id=new_file_id, sheet_name=report_table_name)
            sheet_obj = _extract_sheet_from_create_or_update_resp(sheet_resp)
            if sheet_obj is None:
                new_schema = get_file_schema(auth, file_id=new_file_id)
                new_sheets = (new_schema.get("data") or {}).get("sheets") or []
                new_sheets_dict = [s for s in new_sheets if isinstance(s, dict)]
                matched = [s for s in new_sheets_dict if _is_equal_ignore_case(report_table_name, _get_sheet_name(s))]
                sheet_obj = matched[0] if matched else (new_sheets_dict[0] if new_sheets_dict else None)
            if not isinstance(sheet_obj, dict):
                raise RuntimeError("无法从响应解析出 sheet：resp={0}".format(json.dumps(sheet_resp, ensure_ascii=False)[:2000]))
            new_sheet_id = _get_sheet_id(sheet_obj)
            new_sheet_name = _get_sheet_name(sheet_obj) or report_table_name
            if not new_sheet_id:
                raise RuntimeError("创建/更新 sheet 成功但未拿到 sheet_id：sheet={0!r}".format(sheet_obj))
            stat_sheets_created += 1

            # 删除系统默认字段（名称/数量/日期/状态）
            try:
                del_resp = delete_system_default_fields_if_present(
                    auth, file_id=new_file_id, sheet_id=new_sheet_id, sheet_name=new_sheet_name
                )
            except Exception as e:
                del_resp = {"error": _to_text(e)}

            # 若“名称”字段无法删除（常见：主字段），则将其重命名为模板第一个表头，并避免重复创建该字段
            try:
                if headers:
                    did_rename, rename_details = rename_name_field_if_present(
                        auth, file_id=new_file_id, sheet_id=new_sheet_id, new_name=headers[0]
                    )
                    if did_rename:
                        # 已把“名称”改成 headers[0]，避免后续再创建同名字段
                        try:
                            first = _to_text(headers[0]).strip()
                            headers = [h for h in headers if _to_text(h).strip() != first]
                        except Exception:
                            pass
            except Exception as e:
                rename_details = {"error": _to_text(e)}

            create_fields(auth, file_id=new_file_id, sheet_id=new_sheet_id, headers=headers, batch_size=50)

            clear_info = {}
            if CLEAR_SHEET_RECORDS_AFTER_CREATE:
                try:
                    clear_info = clear_sheet_placeholder_records(auth, new_file_id, new_sheet_id)
                except Exception as _clr_e:
                    clear_info = {"error": _to_text(_clr_e)}
            else:
                clear_info = {"skipped": True, "reason": "KINGSOFT_CLEAR_SHEET_RECORDS_AFTER_CREATE=false"}

            created.append(
                {
                    "record_id": record_id,
                    "template_url": _to_text(template_url),
                    "report_resource_name": _to_text(report_resource_name),
                    "report_table_name": _to_text(report_table_name),
                    "doclib_name": _to_text(doclib_text),
                    "doclib_field": _to_text(doclib_field),
                    "target_drive_id": target_drive_id,
                    "new_file_id": new_file_id,
                    "new_file_name": _to_text(new_file_name_real),
                    "new_sheet_id": new_sheet_id,
                    "new_sheet_name": new_sheet_name,
                    "field_count": len(headers),
                    "sign_attempt": created_file.get("_sign_attempt"),
                    "records_clear": clear_info,
                }
            )
        except Exception as e:
            failed.append(
                {
                    "record_id": record_id,
                    "template_url": _to_text(template_url),
                    "report_resource_name": _to_text(report_resource_name),
                    "report_table_name": _to_text(report_table_name),
                    "doclib_name": _to_text(doclib_text),
                    "doclib_field": _to_text(doclib_field),
                    "error": _to_text(e),
                }
            )

    _print_json(
        {
            "source_drive_id": source_drive_id,
            "source_file_id": source_file_id,
            "source_sheet_id": source_sheet_id,
            "migrate_yes_count": migrate_yes_count,
            "created_count": len(created),
            "skipped_count": len(skipped),
            "failed_count": len(failed),
            "run_summary": {
                "documents_created": stat_documents_created,
                "documents_reused_existing_no_create": stat_documents_reused_existing,
                "sheets_created": stat_sheets_created,
                "sheets_skipped_already_exist": stat_sheets_skipped_existing,
            },
            "summary_line": u"新建文档={0}，已存在未新建文档={1}，新建Sheet={2}，已存在未新建Sheet={3}".format(
                stat_documents_created,
                stat_documents_reused_existing,
                stat_sheets_created,
                stat_sheets_skipped_existing,
            ),
            "created": created,
            "skipped": skipped,
            "failed": failed,
        }
    )
    if failed:
        return 2
    return 0


if __name__ == "__main__":
    # 平台兼容模式：
    # 有些调度平台在“任务失败”时不会展示脚本 stdout/stderr，只显示固定文案
    # "Python job execute failed."。因此这里把所有错误都转换成 stdout 的 JSON，
    # 并强制 exit code = 0，让平台把 outputs 展示出来。
    try:
        rc = main(sys.argv)
        try:
            rc = int(rc)
        except Exception:
            rc = 0
        # 永远 0 退出（业务失败信息由 JSON 呈现）
        raise SystemExit(0)
    except KeyboardInterrupt:
        _print_json({"error": "keyboard_interrupt"})
        raise SystemExit(0)
    except SystemExit as e:
        # 即使 main 内部主动 exit 非 0，也转为 0，避免平台吞日志
        code = getattr(e, "code", 0)
        if code not in (0, None, "0"):
            _print_json({"error": "system_exit", "code": _to_text(code)})
        raise SystemExit(0)
    except Exception:
        tb = _to_text(traceback.format_exc())
        _print_json({"error": "unhandled_exception", "traceback": tb})
        raise SystemExit(0)

