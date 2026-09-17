# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
lgbs_table_structure_move_department_prod.py
从金山多维表格读取“Hive 源库（默认 lgbs）表 -> 各部门 Hive 目标库表”映射配置，
按源 Hive 表字段与类型生成目标库建表 DDL 并执行（仅建表，不入数）。

源库名：环境变量 HIVE_SOURCE_DATABASE 或 LGBS_HIVE_SOURCE_DB（默认 lgbs）；
多维表「二期-中心库账号」也可作为源库名（与过滤条件一致时通常为 lgbs）。

性能：无 pyhive 时每条记录默认一次 beeline（DESCRIBE + SHOW TBLPROPERTIES 合并执行）。
若不需源表注释，可设 SKIP_HIVE_SOURCE_TABLE_COMMENT=1 仅 DESCRIBE。安装 pyhive+sasl+thrift 可改为长连接，元数据阶段会快一个数量级。

兼容：Python 2.7
"""

from __future__ import print_function

import sys
import os
import time
import json
import hashlib
import hmac
import re
import subprocess
import tempfile
import codecs
from email.utils import formatdate

try:
    import http.client as httplib  # py3
except Exception:
    import httplib  # py2

try:
    from urllib import urlencode, quote  # py2
except Exception:
    from urllib.parse import urlencode, quote  # py3

try:
    text_type = unicode  # noqa: F821 (py2)
except Exception:
    text_type = str


def to_text(val):
    """
    安全转换为文本：
    - Py2: unicode
    - Py3: str
    """
    if val is None:
        return text_type("")
    try:
        if isinstance(val, bytes):
            for enc in ("utf-8", "gb18030", "gbk", "latin-1"):
                try:
                    return val.decode(enc, "replace")
                except Exception:
                    continue
            return val.decode("utf-8", "replace")
    except Exception:
        pass
    try:
        return text_type(val)
    except Exception:
        try:
            b = str(val)
            if isinstance(b, bytes):
                for enc in ("utf-8", "gb18030", "gbk", "latin-1"):
                    try:
                        return text_type(b.decode(enc, "replace"))
                    except Exception:
                        continue
                return text_type(b.decode("utf-8", "replace"))
            return text_type(b)
        except Exception:
            return text_type("")


def _print_u(msg):
    """统一 utf-8 输出，避免 Py2 中文乱码/UnicodeEncodeError。"""
    try:
        if isinstance(msg, text_type):
            sys.stdout.write(msg.encode("utf-8") + "\n")
        else:
            sys.stdout.write(to_text(msg).encode("utf-8") + "\n")
    except Exception:
        try:
            print(msg)
        except Exception:
            pass


def _to_utf8_bytes(val):
    if val is None:
        return b""
    try:
        if isinstance(val, text_type):
            return val.encode("utf-8")
    except Exception:
        pass
    try:
        if isinstance(val, str):
            return val.encode("utf-8")
    except Exception:
        pass
    try:
        return bytes(val)
    except Exception:
        try:
            return str(val).encode("utf-8")
        except Exception:
            return b""


def url_quote_any(val, safe=""):
    if sys.version_info[0] < 3:
        return quote(_to_utf8_bytes(val), safe=safe)
    return quote(to_text(val), safe=safe)


def urlencode_any(pairs, doseq=True):
    if sys.version_info[0] < 3:
        encoded_pairs = []
        for k, v in pairs:
            kb = _to_utf8_bytes(k)
            if isinstance(v, (list, tuple)) and doseq:
                for item in v:
                    encoded_pairs.append((kb, _to_utf8_bytes(item)))
            else:
                encoded_pairs.append((kb, _to_utf8_bytes(v)))
        return urlencode(encoded_pairs, doseq=doseq)
    return urlencode(pairs, doseq=doseq)


def _now():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _merge_request_headers(*parts):
    out = {}
    for p in parts:
        if p:
            out.update(p)
    return out


def _trim_token(val):
    if val is None:
        return ""
    try:
        # 注意：Py2 下使用非 unicode 的正则模式时，`\u3000` 可能被错误解释，
        # 进而把字符 '3'/'0' 也当作可裁剪字符，导致 `DJLR20 -> DJLR2`、`_v3 -> _v`。
        # 这里改为纯字符串 strip，稳定处理普通空白与全角空格。
        t = to_text(val)
        return t.strip().strip(u"\u3000")
    except Exception:
        return str(val).strip()


def _normalize_identifier_token(val):
    """
    对配置里读取到的标识符做增强清洗（兼容不可见字符）：
    - 去首尾空白（含中文全角空格）
    - 去 BOM / 零宽字符
    - 去控制字符（保留可见字符）
    """
    t = _trim_token(val)
    if not t:
        return ""
    # 去常见不可见字符：BOM / zero-width chars
    t = t.replace(u"\ufeff", u"").replace(u"\u200b", u"").replace(u"\u200c", u"").replace(u"\u200d", u"")
    # 去控制字符（避免日志看起来正常但 SQL 精确匹配失败）
    try:
        t = re.sub(u"[\x00-\x1f\x7f]", u"", t)
    except Exception:
        pass
    return _trim_token(t)


def _to_hex_preview(val, max_bytes=64):
    """
    诊断用：输出文本的 utf-8 十六进制预览（便于识别不可见字符）。
    """
    try:
        b = to_text(val).encode("utf-8")
    except Exception:
        try:
            b = str(val)
            if not isinstance(b, bytes):
                b = to_text(b).encode("utf-8")
        except Exception:
            b = b""
    if not b:
        return ""
    try:
        clip = b[:max_bytes]
        hx = clip.hex()
    except Exception:
        try:
            import binascii as _binascii

            hx = _binascii.hexlify(clip).decode("ascii", "ignore")
        except Exception:
            hx = ""
    if len(b) > max_bytes:
        return hx + "...(truncated)"
    return hx


def _parse_owner_table(table_name):
    t = _normalize_identifier_token(table_name)
    if not t:
        return None, None
    if "." in t:
        parts = [p for p in t.split(".") if p]
        if len(parts) >= 2:
            return parts[0], parts[1]
    return None, t


def _escape_hive_comment(s):
    if s is None:
        return ""
    return to_text(s).replace("'", "''")


def _safe_hive_ident(name):
    """
    Hive 标识符安全处理：
    - 保守起见：只允许 [a-zA-Z0-9_]
    - 其它字符替换为 _
    """
    t = to_text(name).strip()
    if not t:
        return ""
    t2 = re.sub(r"[^0-9a-zA-Z_]", "_", t)
    # Hive 不允许以数字开头
    if re.match(r"^\d", t2):
        t2 = "_" + t2
    return t2.lower()


# ==================== CLI 参数 ====================
def _require_cli_args():
    if len(sys.argv) < 4:
        _print_u(
            u"用法：python lgbs_table_structure_move_department_prod.py <doc_lib_name> <file_name> <sheet_name>\n"
            u"（勿将脚本路径当作 Python 代码执行；含连字符的旧名请用：python lgbs-table-structure-move-department-prod.py）"
        )
        sys.exit(2)
    return to_text(sys.argv[1]), to_text(sys.argv[2]), to_text(sys.argv[3])


# ==================== Kingsoft API（对齐 oracle-data-insert-kingsoft-prod-all.py） ====================
API_HOST = "<INTERNAL_API_HOST>"
API_PORT = 5489

DEFAULT_APP_ID = "<YOUR_APP_ID>"
DEFAULT_APP_KEY = "<YOUR_APP_SECRET>"
DEFAULT_CLIENT_ID = "<YOUR_APP_ID>"
DEFAULT_CLIENT_SECRET = "<YOUR_APP_SECRET>"
DEFAULT_COMPANY_ID = "1"

API_PATH_OAUTH_TOKEN = "/openapi/oauth2/token"
API_PATH_DOCLIBS = "/openapi/v7/doclib/search"
API_PATH_FILES_SEARCH = "/openapi/v7/files/search"
API_PATH_FILE_SCHEMA = "/openapi/v7/coop/dbsheet/{file_id}/schema"
API_PATH_FILE_RECORDS_BY_PAGE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records/list_by_page"

CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded"
CONTENT_TYPE_OCTET_STREAM = "application/octet-stream"
CONTENT_TYPE_JSON = "application/json"

SIGNATURE_PREFIX = "KSO-1"
URL_SPLIT_KEYWORD = "openapi"
OAUTH_GRANT_TYPE = "client_credentials"


def get_request_headers(method, url, body="", content_type=CONTENT_TYPE_FORM_URLENCODED, app_id=None, app_key=None):
    if app_id is None:
        app_id = DEFAULT_APP_ID
    if app_key is None:
        app_key = DEFAULT_APP_KEY
    if not app_id or not app_key:
        raise RuntimeError("未配置 KINGSOFT_APP_ID/KINGSOFT_APP_KEY")

    method = method.upper()
    parts = url.split(URL_SPLIT_KEYWORD, 1)
    path = parts[1] if len(parts) == 2 else url
    date_string = formatdate(usegmt=True)
    body = body or ""

    if body == "":
        base_string = "{}{}{}{}{}".format(SIGNATURE_PREFIX, method, path, content_type, date_string)
    else:
        sha256_hex = hashlib.sha256(body.encode("utf-8")).hexdigest()
        base_string = "{}{}{}{}{}{}".format(
            SIGNATURE_PREFIX, method, path, content_type, date_string, sha256_hex
        )

    signature = hmac.new(app_key.encode("utf-8"), base_string.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = "{} {}:{}".format(SIGNATURE_PREFIX, app_id, signature)
    return {"X-Kso-Date": date_string, "Content-Type": content_type, "X-Kso-Authorization": authorization}


def _http_request(method, path, headers, body=None):
    conn = httplib.HTTPConnection(API_HOST, API_PORT, timeout=120)
    try:
        send_body = body
        if isinstance(send_body, text_type):
            send_body = send_body.encode("utf-8")
        conn.request(method.upper(), path, body=send_body, headers=headers)
        resp = conn.getresponse()
        raw = resp.read()
        text = raw.decode("utf-8", "replace") if raw else ""
        if resp.status >= 400:
            body_preview = to_text(text)
            if len(body_preview) > 8000:
                body_preview = body_preview[:8000] + u"...(truncated)"
            msg = u"HTTP {0} {1}: {2}".format(resp.status, to_text(resp.reason), body_preview)
            raise RuntimeError(msg)
        if not text:
            return {}
        return json.loads(text)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def app_authorize(client_id=None, client_secret=None):
    if client_id is None:
        client_id = DEFAULT_CLIENT_ID
    if client_secret is None:
        client_secret = DEFAULT_CLIENT_SECRET

    method = "POST"
    url = "http://{0}:{1}{2}".format(API_HOST, API_PORT, API_PATH_OAUTH_TOKEN)
    payload = "grant_type={0}&client_id={1}&client_secret={2}".format(OAUTH_GRANT_TYPE, client_id, client_secret)
    headers = get_request_headers(method=method, url=url, body=payload, content_type=CONTENT_TYPE_FORM_URLENCODED)
    req_headers = _merge_request_headers(
        {
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": "python",
            "Accept-Encoding": "gzip, deflate, br",
        },
        headers,
    )
    resp = _http_request(method="POST", path=API_PATH_OAUTH_TOKEN, headers=req_headers, body=payload)
    token = (resp or {}).get("access_token") or (resp or {}).get("data", {}).get("access_token")
    token_type = (resp or {}).get("token_type") or "Bearer"
    if not token:
        raise RuntimeError("授权失败，响应：{0}".format(resp))
    return {"access_token": token, "token_type": token_type}


def get_auth():
    injected = (os.getenv("KINGSOFT_ACCESS_TOKEN", "") or "").strip()
    if injected:
        token_type = (os.getenv("KINGSOFT_TOKEN_TYPE", "") or "").strip() or "Bearer"
        print("【{0}】使用注入的 KINGSOFT_ACCESS_TOKEN（token_type={1}）".format(_now(), token_type))
        return {"access_token": injected, "token_type": token_type}
    return app_authorize()


def get_doc_lib_list(auth, keyword):
    method = "GET"
    keyword_str = u"" if keyword is None else to_text(keyword)
    encoded_keyword = url_quote_any(keyword_str, safe="")
    encoded_company_id = url_quote_any(str(DEFAULT_COMPANY_ID), safe="")
    page_size = 50
    path = "{0}?page_size={1}&company_id={2}&keyword={3}".format(API_PATH_DOCLIBS, page_size, encoded_company_id, encoded_keyword)
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    headers = get_request_headers(method=method, url=url_for_sign, body="", content_type=CONTENT_TYPE_OCTET_STREAM)
    req_headers = _merge_request_headers(
        {
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": "python",
            "Accept-Encoding": "gzip, deflate, br",
            "Authorization": "{0} {1}".format(auth.get("token_type", "Bearer"), auth["access_token"]),
            "X-Kso-Id-Type": "internal",
        },
        headers,
    )
    resp = _http_request(method=method, path=path, headers=req_headers, body="")
    items = (resp or {}).get("data", {}).get("items", []) or (resp or {}).get("items", []) or []
    return [it for it in items if isinstance(it, dict)]


def get_files_keyword(auth, keyword, drive_ids):
    method = "GET"
    base_path = API_PATH_FILES_SEARCH
    query = [("keyword", u"" if keyword is None else to_text(keyword)), ("type", "file_name"), ("page_size", "100")]
    for did in drive_ids or []:
        if did:
            query.append(("drive_ids", str(did)))
    path = "{0}?{1}".format(base_path, urlencode_any(query, doseq=True))
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    signed_headers = get_request_headers(method=method, url=url_for_sign, body="", content_type=CONTENT_TYPE_OCTET_STREAM)
    req_headers = _merge_request_headers(
        {
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": "python",
            "Accept-Encoding": "gzip, deflate, br",
            "Authorization": "{0} {1}".format(auth.get("token_type", "Bearer"), auth["access_token"]),
        },
        signed_headers,
    )
    req_headers["Content-Type"] = CONTENT_TYPE_OCTET_STREAM
    resp = _http_request(method=method, path=path, headers=req_headers, body="")
    items = (resp or {}).get("data", {}).get("items", []) or []
    file_items = []
    for it in items:
        if not isinstance(it, dict):
            continue
        fobj = it.get("file")
        if isinstance(fobj, dict):
            file_items.append(fobj)
        else:
            file_items.append(it)
    return file_items


def get_file_schema(auth, file_id):
    method = "GET"
    path = API_PATH_FILE_SCHEMA.format(file_id=file_id)
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    headers = get_request_headers(method=method, url=url_for_sign, body="", content_type=CONTENT_TYPE_OCTET_STREAM)
    req_headers = _merge_request_headers(
        {
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": "python",
            "Accept-Encoding": "gzip, deflate, br",
            "Authorization": "{0} {1}".format(auth.get("token_type", "Bearer"), auth["access_token"]),
        },
        headers,
    )
    req_headers["Content-Type"] = CONTENT_TYPE_OCTET_STREAM
    return _http_request(method=method, path=path, headers=req_headers, body=None)


def resolve_file_sheet_ids(auth, doc_lib_name, file_name, sheet_name):
    if not doc_lib_name or not file_name:
        raise RuntimeError("doc_lib_name/file_name 不能为空")

    doclibs = get_doc_lib_list(auth, keyword=doc_lib_name)
    matched = []
    for it in doclibs:
        name = (it.get("drive", {}) or {}).get("name") or it.get("name") or ""
        if to_text(doc_lib_name) in to_text(name):
            matched.append(it)
    if not matched:
        raise RuntimeError("未找到文档库：{0!r}".format(doc_lib_name))

    drive_ids = []
    for it in matched:
        did = (it.get("drive", {}) or {}).get("id")
        if did:
            drive_ids.append(str(did))

    files = get_files_keyword(auth, keyword=file_name, drive_ids=drive_ids)
    file_id = None
    matched_file_name = None
    for f in files:
        nm = f.get("name") or ""
        if to_text(file_name) in to_text(nm):
            file_id = f.get("id")
            matched_file_name = nm
            break
    if not file_id:
        raise RuntimeError("未找到文件：{0!r}".format(file_name))

    schema = get_file_schema(auth, file_id=str(file_id))
    sheets = (schema.get("data", {}) or {}).get("sheets", []) or []
    if not sheets:
        raise RuntimeError("未获取到 sheets 信息")

    sheet_id = None
    matched_sheet_name = None
    if sheet_name:
        for s in sheets:
            if not isinstance(s, dict):
                continue
            if to_text(sheet_name) in to_text(s.get("name") or ""):
                sheet_id = s.get("id")
                matched_sheet_name = s.get("name")
                break
    if sheet_id is None:
        sheet_id = sheets[0].get("id")
        matched_sheet_name = sheets[0].get("name")
    if not sheet_id:
        raise RuntimeError("未解析到 sheet_id")

    # 打印 sheet 列表，便于排查误匹配
    try:
        sheet_pairs = []
        for s in sheets:
            if not isinstance(s, dict):
                continue
            sid = s.get("id")
            sn = s.get("name")
            if sid is None:
                continue
            sheet_pairs.append("{0}:{1}".format(sid, to_text(sn)))
        print("【{0}】文件 sheet 列表：{1}".format(_now(), " | ".join(sheet_pairs)))
        print(
            "【{0}】定位到：doc_lib_name={1}, file_name={2}->{3}, sheet_name={4}->{5}({6})".format(
                _now(),
                to_text(doc_lib_name),
                to_text(file_name),
                to_text(matched_file_name),
                to_text(sheet_name),
                sheet_id,
                to_text(matched_sheet_name),
            )
        )
    except Exception:
        pass
    return str(file_id), str(sheet_id)


def list_records_by_page(auth, file_id, sheet_id, page_num=1, page_size=100, view_id=""):
    method = "POST"
    path = API_PATH_FILE_RECORDS_BY_PAGE.format(file_id=file_id, sheet_id=sheet_id)
    url = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    body_dict = {"page_num": int(page_num), "page_size": int(page_size), "prefer_id": False}
    if view_id:
        body_dict["view_id"] = view_id
    body = json.dumps(body_dict, ensure_ascii=False)
    headers = get_request_headers(method=method, url=url, body=body, content_type=CONTENT_TYPE_JSON)
    req_headers = _merge_request_headers(
        {
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": "python",
            "Accept-Encoding": "gzip, deflate, br",
            "Authorization": "{0} {1}".format(auth.get("token_type", "Bearer"), auth["access_token"]),
        },
        headers,
    )
    return _http_request(method=method, path=path, headers=req_headers, body=body)


def iter_all_sheet_records(auth, file_id, sheet_id):
    """遍历分页拉取所有 records（原始 record dict）。"""
    page_size = int(os.getenv("KINGSOFT_PAGE_SIZE", "100") or "100")
    view_id = (os.getenv("KINGSOFT_VIEW_ID", "") or "").strip()
    page_num = 1
    total = 0
    while True:
        resp = list_records_by_page(auth, file_id=file_id, sheet_id=sheet_id, page_num=page_num, page_size=page_size, view_id=view_id)
        data = (resp or {}).get("data", {}) if isinstance(resp, dict) else {}
        recs = []
        if isinstance(data, dict):
            recs = data.get("records") or data.get("items") or data.get("list") or []
        if not isinstance(recs, list):
            recs = []
        if not recs:
            break
        for r in recs:
            if isinstance(r, dict):
                yield r
                total += 1
        if len(recs) < page_size:
            break
        page_num += 1
    raise StopIteration


def _parse_record_fields(record):
    """
    兼容解析字段：
    - record["fields_value"]: raw json string
    - record["fields"]: dict 或 raw json string
    """
    if not isinstance(record, dict):
        return {}
    fv = record.get("fields_value")
    if fv is None:
        fv = record.get("fields")
    if fv is None:
        return {}
    if isinstance(fv, dict):
        return fv
    # py2/py3 兼容：fv 可能是 bytes/str/unicode
    try:
        if isinstance(fv, bytes):
            fv_txt = fv.decode("utf-8", "replace")
        else:
            fv_txt = to_text(fv)
    except Exception:
        fv_txt = to_text(fv)
    fv_txt = fv_txt.strip()
    if not fv_txt:
        return {}
    try:
        obj = json.loads(fv_txt)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        # 兜底：非 JSON 时返回空 dict
        return {}


# ==================== Hive 源库表结构读取（lgbs 等） ====================
HIVE_SOURCE_DATABASE = (os.getenv("HIVE_SOURCE_DATABASE") or os.getenv("LGBS_HIVE_SOURCE_DB") or "lgbs").strip()


def _sanitize_hive_type_str(s):
    """DESCRIBE 得到的类型串：去空白，保持 Hive DDL 可用。"""
    t = to_text(s).strip()
    if not t:
        return "string"
    return t


def _ddl_column_type(col):
    """
    根据列元数据生成目标 Hive 列类型。
    - 若 data_type 为 Oracle 风格（历史兼容），走 _oracle_type_to_hive
    - 否则视为源 Hive 类型，原样规范化后使用
    """
    dt_raw = col.get("data_type")
    dt_u = to_text(dt_raw).strip().upper()
    oracle_tokens = (
        "VARCHAR2",
        "NVARCHAR2",
        "CHAR",
        "NCHAR",
        "CLOB",
        "NCLOB",
        "LONG",
        "BLOB",
        "RAW",
        "NUMBER",
        "FLOAT",
        "BINARY_FLOAT",
        "BINARY_DOUBLE",
    )
    if dt_u in oracle_tokens or (dt_u.startswith("TIMESTAMP") and "(" in dt_u):
        return _oracle_type_to_hive(col)
    if dt_u == "DATE" and col.get("data_length") is not None:
        # Oracle DATE；纯 Hive 源多为 TIMESTAMP 或 DATE（Hive 3）
        return _oracle_type_to_hive(col)
    return _sanitize_hive_type_str(dt_raw)


def _hive_fetch_table_columns_pyhive(cur, hive_db, hive_table):
    """使用 pyhive 游标 DESCRIBE，返回与旧 Oracle 列 dict 兼容的结构。"""
    db_ident = _safe_hive_ident(hive_db)
    tb_ident = _safe_hive_ident(hive_table)
    if not db_ident or not tb_ident:
        return []
    _hive_exec(cur, u"USE `{0}`".format(db_ident))
    _hive_exec(cur, u"DESCRIBE `{0}`".format(tb_ident))
    rows = cur.fetchall() or []
    out = []
    for r in rows:
        if not r:
            continue
        cn = to_text(r[0]).strip() if r[0] is not None else ""
        dt = to_text(r[1]).strip() if len(r) > 1 and r[1] is not None else ""
        cm = ""
        if len(r) > 2 and r[2] is not None:
            cm = _trim_token(r[2])
        if not cn:
            continue
        if cn.lower() == "col_name" and dt.lower() == "data_type":
            continue
        if cn.startswith("#"):
            break
        out.append(
            {
                "name": cn,
                "data_type": dt,
                "data_length": None,
                "data_precision": None,
                "data_scale": None,
                "nullable": "",
                "comment": cm,
            }
        )
    return out


def _hive_fetch_table_comment_pyhive(cur, hive_db, hive_table):
    db_ident = _safe_hive_ident(hive_db)
    tb_ident = _safe_hive_ident(hive_table)
    if not db_ident or not tb_ident:
        return ""
    try:
        _hive_exec(cur, u"USE `{0}`".format(db_ident))
        _hive_exec(cur, u"SHOW TBLPROPERTIES `{0}`".format(tb_ident))
        pr = cur.fetchall() or []
        for row in pr:
            if not row or len(row) < 2:
                continue
            k = to_text(row[0]).strip().lower()
            v = row[1]
            if k == "comment":
                return _trim_token(v)
    except Exception:
        pass
    return ""


def _parse_beeline_vertical_table_output(text):
    """
    解析 beeline 表格输出中的 DESCRIBE / SHOW TBLPROPERTIES 行。
    跳过 +- 边框与表头行。
    """
    cols_out = []
    for line in to_text(text or "").splitlines():
        ln = line.strip()
        if "|" not in ln:
            continue
        if ln.startswith("+"):
            continue
        parts = [p.strip() for p in ln.split("|")]
        parts = [p for p in parts if p != ""]
        if len(parts) < 2:
            continue
        c0, c1 = parts[0], parts[1]
        if c0.lower() == "col_name" and c1.lower() == "data_type":
            continue
        if c0.startswith("#"):
            break
        cm = parts[2] if len(parts) > 2 else ""
        cols_out.append((c0, c1, cm))
    return cols_out


def _split_beeline_describe_from_tblproperties(merged_out):
    """
    同一次 beeline 中先 DESCRIBE 再 SHOW TBLPROPERTIES 时，按结果表头切分 stdout。
    """
    lines = to_text(merged_out or "").splitlines()
    for i, line in enumerate(lines):
        if "|" not in line:
            continue
        low = line.lower()
        if "prpt_name" in low and "prpt_value" in low:
            return "\n".join(lines[:i]), "\n".join(lines[i:])
    return merged_out, ""


def _parse_beeline_tblproperties_comment(tbl_blob):
    """从 SHOW TBLPROPERTIES 的 beeline 表格输出中取 comment。"""
    for line in to_text(tbl_blob or "").splitlines():
        ln = line.strip()
        if "|" not in ln or ln.startswith("+"):
            continue
        parts = [p.strip() for p in ln.split("|")]
        parts = [p for p in parts if p != ""]
        if len(parts) < 2:
            continue
        c0, c1 = parts[0], parts[1]
        if c0.lower() == "prpt_name" and c1.lower() == "prpt_value":
            continue
        if c0.strip().lower() == "comment":
            return _trim_token(c1)
    return ""


def _hive_fetch_table_schema_beeline(hive_cfg, hive_db, hive_table, want_comment=True):
    """
    单次 beeline：USE + DESCRIBE（+ 可选 SHOW TBLPROPERTIES），避免每条记录起两次 JVM。
    返回 (columns_list, table_comment_str)。
    """
    db_ident = _safe_hive_ident(hive_db)
    tb_ident = _safe_hive_ident(hive_table)
    if not db_ident or not tb_ident:
        return [], ""
    if want_comment:
        sql = u"USE `{0}`;\nDESCRIBE `{1}`;\nSHOW TBLPROPERTIES `{1}`;".format(db_ident, tb_ident)
    else:
        sql = u"USE `{0}`;\nDESCRIBE `{1}`;".format(db_ident, tb_ident)
    rc, out, err = _beeline_run_sql_capture(hive_cfg, sql)
    if rc != 0:
        raise RuntimeError("beeline 源表元数据失败：rc={0} err={1}".format(rc, to_text(err)[:2000]))
    describe_blob = out
    table_comment = ""
    if want_comment:
        describe_blob, tbl_blob = _split_beeline_describe_from_tblproperties(out)
        table_comment = _parse_beeline_tblproperties_comment(tbl_blob)
    out_rows = _parse_beeline_vertical_table_output(describe_blob)
    cols = []
    for c0, c1, cm in out_rows:
        cols.append(
            {
                "name": c0,
                "data_type": c1,
                "data_length": None,
                "data_precision": None,
                "data_scale": None,
                "nullable": "",
                "comment": _trim_token(cm),
            }
        )
    return cols, table_comment


def _beeline_run_sql_capture(hive_cfg, sql_text):
    """
    beeline 执行 SQL 并捕获 stdout（用于 DESCRIBE / SHOW TBLPROPERTIES）。
    """
    beeline_cmd = (os.getenv("HIVE_BEELINE_CMD", "") or "beeline").strip()
    hadoop_env_sh = (os.getenv("HADOOP_ENV_SH", "") or "/opt/hadoopclient/bigdata_env").strip()
    kinit_cmd = (os.getenv("KINIT_CMD", "") or "").strip()
    if not kinit_cmd:
        k_user = (os.getenv("HIVE_KINIT_USER", "") or os.getenv("KINIT_USER", "") or "").strip()
        k_pwd = (os.getenv("HIVE_KINIT_PASSWORD", "") or os.getenv("KINIT_PASSWORD", "") or "").strip()
        k_keytab = (os.getenv("HIVE_KINIT_KEYTAB", "") or os.getenv("KINIT_KEYTAB", "") or "").strip()
        if not k_user:
            try:
                k_user = (hive_cfg.get("username") or "").strip()
            except Exception:
                k_user = ""
        try:
            if k_user and "@" not in k_user and (hive_cfg.get("auth") or "").upper() == "KERBEROS":
                realm = (os.getenv("HIVE_KRB_REALM", "") or "").strip()
                if (not realm) and os.path.exists("/etc/krb5.conf"):
                    try:
                        with open("/etc/krb5.conf", "r") as _f:
                            for _ln in _f:
                                _ln2 = _ln.strip()
                                if not _ln2 or _ln2.startswith("#") or _ln2.startswith(";"):
                                    continue
                                if "default_realm" in _ln2 and "=" in _ln2:
                                    _k, _v = _ln2.split("=", 1)
                                    if _k.strip().lower() == "default_realm":
                                        realm = _v.strip()
                                        break
                    except Exception:
                        pass
                if realm:
                    k_user = "{0}@{1}".format(k_user, realm)
        except Exception:
            pass
        if k_user and k_keytab:
            kinit_cmd = "kinit -kt '{0}' '{1}'".format(k_keytab.replace("'", "'\\''"), k_user.replace("'", "'\\''"))
        elif k_user and k_pwd:
            kinit_cmd = "echo '{0}' | kinit '{1}'".format(k_pwd.replace("'", "'\\''"), k_user.replace("'", "'\\''"))

    connect_db = (os.getenv("HIVE_BEELINE_CONNECT_DB", "") or "").strip() or "default"
    jdbc_url = _build_hive_jdbc_url(hive_cfg, connect_db)
    username = (hive_cfg.get("username") or "").strip()

    parts_sql = []
    for s in to_text(sql_text).split(";"):
        st = s.strip()
        if st:
            parts_sql.append(st if st.endswith(";") else st + ";")
    sql = u"\n".join(parts_sql)

    tmp_path = None
    f = None
    try:
        f = tempfile.NamedTemporaryFile(prefix="hive_q_", suffix=".sql", delete=False)
        tmp_path = f.name
        data = sql
        try:
            if isinstance(data, text_type):
                data = data.encode("utf-8")
        except Exception:
            pass
        f.write(data)
        f.flush()
        f.close()
        f = None
    finally:
        try:
            if f:
                f.close()
        except Exception:
            pass

    bash_parts = []
    if hadoop_env_sh:
        bash_parts.append("if [ -f '{0}' ]; then source '{0}'; fi".format(hadoop_env_sh.replace("'", "'\\''")))
    if kinit_cmd:
        bash_parts.append(kinit_cmd)
    beeline_args = [beeline_cmd, "-u", "'{0}'".format(jdbc_url.replace("'", "'\\''"))]
    if username:
        beeline_args.extend(["-n", "'{0}'".format(username.replace("'", "'\\''"))])
    if tmp_path:
        beeline_args.extend(["-f", "'{0}'".format(to_text(tmp_path).replace("'", "'\\''"))])
    bash_parts.append(" ".join(beeline_args))
    bash_cmd = " && ".join(bash_parts) if bash_parts else " ".join(beeline_args)
    rc, out, err = _shell_capture(bash_cmd)
    try:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
    except Exception:
        pass
    return rc, out, err


def _hive_fetch_table_columns_beeline(hive_cfg, hive_db, hive_table):
    cols, _ = _hive_fetch_table_schema_beeline(hive_cfg, hive_db, hive_table, want_comment=False)
    return cols


def _hive_fetch_table_comment_beeline(hive_cfg, hive_db, hive_table):
    _, cmt = _hive_fetch_table_schema_beeline(hive_cfg, hive_db, hive_table, want_comment=True)
    return cmt


def _oracle_type_to_hive(col):
    dt = (col.get("data_type") or "").upper()
    p = col.get("data_precision")
    s = col.get("data_scale")

    if dt in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG"):
        return "string"
    if dt in ("BLOB", "RAW", "LONG RAW"):
        return "binary"
    if dt in ("DATE",):
        return "timestamp"
    if dt.startswith("TIMESTAMP"):
        return "timestamp"
    if dt in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return "double"
    if dt in ("NUMBER", "DECIMAL", "NUMERIC"):
        try:
            if s is not None and int(s) > 0:
                pp = int(p) if p is not None else 38
                ss = int(s)
                if pp <= 0:
                    pp = 38
                if ss < 0:
                    ss = 0
                return "decimal({0},{1})".format(pp, ss)
            # 整数
            if p is not None:
                pp2 = int(p)
                if pp2 <= 9:
                    return "int"
                return "bigint"
            return "bigint"
        except Exception:
            return "bigint"
    return "string"


# ==================== Hive DDL execute (pyhive) ====================
HIVE_USER_CONFIG_FILE = "/opt/lgbs/user.txt"
_LAST_HIVE_CFG_FILE = None
_HIVE_CFG_LOGGED = False


def read_hive_user_config(file_path=HIVE_USER_CONFIG_FILE):
    cfg = {}
    fp = file_path
    # Windows 本地跑时可能没有 /tmp，允许通过 env 覆盖
    fp = os.getenv("HIVE_USER_CONFIG_FILE", fp) or fp
    global _LAST_HIVE_CFG_FILE
    _LAST_HIVE_CFG_FILE = fp
    f = None
    try:
        # 对齐 kingsoft-data-insert-hive-prod-all.py：以 UTF-8 读取 key=value
        if sys.version_info[0] < 3:
            f = codecs.open(fp, "r", encoding="utf-8")
        else:
            f = open(fp, "r", encoding="utf-8")
        for line in f:
            ln = (line or "").strip()
            if not ln or ln.startswith("#"):
                continue
            if "=" in ln:
                k, v = ln.split("=", 1)
                kk = k.strip()
                # 兼容：有些文件可能带 UTF-8 BOM，导致 key 匹配失败（尤其是 jdbc_url）
                try:
                    if kk and isinstance(kk, text_type) and kk[0] == u"\ufeff":
                        kk = kk.lstrip(u"\ufeff")
                except Exception:
                    pass
                # key 统一用小写存储，避免大小写不一致
                try:
                    kk2 = to_text(kk).strip().lower()
                except Exception:
                    kk2 = str(kk).strip().lower()
                cfg[kk2] = (v.strip() if hasattr(v, "strip") else to_text(v).strip())
    except IOError as e:
        # Py2/Py3 统一：文件不存在/无法读取
        raise RuntimeError("Hive 配置文件不存在或不可读：{0}，err={1}".format(fp, e))
    except Exception as e:
        raise RuntimeError("读取 Hive 配置文件失败：{0} err={1}".format(fp, e))
    finally:
        try:
            if f:
                f.close()
        except Exception:
            pass
    return cfg


def get_hive_config(database):
    cfg = read_hive_user_config()
    # 只打印一次：确认实际读取到的配置文件 & 是否拿到 jdbc_url
    global _HIVE_CFG_LOGGED
    if not _HIVE_CFG_LOGGED:
        _HIVE_CFG_LOGGED = True
        try:
            _print_u(
                u"【{0}】Hive 配置加载：file={1}，keys={2}，jdbc_url_set={3}".format(
                    to_text(_now()),
                    to_text(_LAST_HIVE_CFG_FILE),
                    ",".join(sorted([to_text(k) for k in (cfg or {}).keys() if k])),
                    bool((cfg or {}).get("jdbc_url")),
                )
            )
        except Exception:
            pass
    host = cfg.get("st") or cfg.get("host", "")
    port_raw = cfg.get("port", "")
    try:
        port = int(port_raw) if str(port_raw).isdigit() else 21066
    except Exception:
        port = 21066
    return {
        "host": host,
        "port": port,
        "username": cfg.get("username", ""),
        "database": database or cfg.get("database", ""),
        "auth": cfg.get("auth", "KERBEROS"),
        "kerberos_service_name": cfg.get("kerberos_service_name", "hive"),
        "krbhost": cfg.get("krbhost", ""),
        "jdbc_url": cfg.get("jdbc_url", "") or "",
    }


def _hive_exec(cur, sql):
    cur.execute(sql)


def _shell_capture(cmd, env=None):
    """
    在 bash 下执行命令并捕获 stdout/stderr。
    兼容 Py2/Py3。
    """
    try:
        if isinstance(cmd, text_type):
            cmd = cmd.encode("utf-8")
    except Exception:
        pass
    p = subprocess.Popen(
        cmd,
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        universal_newlines=True,
    )
    out, err = p.communicate()
    return p.returncode, out, err


def _build_hive_jdbc_url(hive_cfg, database):
    # 允许直接指定完整 JDBC URL（最稳）
    jdbc_url = (hive_cfg.get("jdbc_url") or "").strip()
    if jdbc_url:
        # 支持在配置里用占位符动态切库：{db} / {database}
        db_ident = _safe_hive_ident(database) or "default"
        try:
            jdbc_url = jdbc_url.replace("{db}", db_ident).replace("{database}", db_ident)
        except Exception:
            pass
        return jdbc_url

    host = (hive_cfg.get("host") or "").strip()
    port = hive_cfg.get("port") or 21050
    db = _safe_hive_ident(database) or "default"
    base = "jdbc:hive2://{0}:{1}/{2}".format(host, port, db)

    # Kerberos principal 需要 realm；若无法获知，留空让环境自行处理（例如已在 bigdata_env 中配置）
    auth = (hive_cfg.get("auth") or "").upper()
    if auth == "KERBEROS":
        realm = (os.getenv("HIVE_KRB_REALM", "") or "").strip()
        ksn = (hive_cfg.get("kerberos_service_name") or "hive").strip()
        krbhost = (hive_cfg.get("krbhost") or "").strip()
        if (not realm) and os.path.exists("/etc/krb5.conf"):
            # 尽量从 krb5.conf 里解析 default_realm
            try:
                with open("/etc/krb5.conf", "r") as _f:
                    for _ln in _f:
                        _ln2 = _ln.strip()
                        if not _ln2 or _ln2.startswith("#") or _ln2.startswith(";"):
                            continue
                        if "default_realm" in _ln2 and "=" in _ln2:
                            _k, _v = _ln2.split("=", 1)
                            if _k.strip().lower() == "default_realm":
                                realm = _v.strip()
                                break
            except Exception:
                pass
        if realm and krbhost:
            return base + ";principal={0}/{1}@{2}".format(ksn, krbhost, realm)

    return base


def _beeline_exec_ddl(ddl_list, hive_cfg, hive_db):
    """
    使用 beeline 执行 DDL，避免依赖 pyhive/thrift。

    依赖：运行环境存在 beeline（MRS 通常自带）。
    可选：通过 HADOOP_ENV_SH 指定需要 source 的环境脚本（默认 /opt/hadoopclient/bigdata_env）。
    可选：通过 KINIT_CMD 指定 Kerberos 认证命令（例如 "kinit -kt /path/user.keytab user@REALM"）。
    """
    beeline_cmd = (os.getenv("HIVE_BEELINE_CMD", "") or "beeline").strip()
    hadoop_env_sh = (os.getenv("HADOOP_ENV_SH", "") or "/opt/hadoopclient/bigdata_env").strip()

    # 对齐 kingsoft-data-insert-hive-prod-all.py：尽量在 beeline 前完成 Kerberos 认证
    # 优先使用显式 KINIT_CMD；否则尝试从 env 拼装（支持 password 或 keytab）
    kinit_cmd = (os.getenv("KINIT_CMD", "") or "").strip()
    if not kinit_cmd:
        k_user = (os.getenv("HIVE_KINIT_USER", "") or os.getenv("KINIT_USER", "") or "").strip()
        k_pwd = (os.getenv("HIVE_KINIT_PASSWORD", "") or os.getenv("KINIT_PASSWORD", "") or "").strip()
        k_keytab = (os.getenv("HIVE_KINIT_KEYTAB", "") or os.getenv("KINIT_KEYTAB", "") or "").strip()
        # 若未显式指定 kinit 用户，默认使用配置文件里的 username（常见：mrs_check）
        if not k_user:
            try:
                k_user = (hive_cfg.get("username") or "").strip()
            except Exception:
                k_user = ""

        # 若是 Kerberos 场景且用户未带 realm，尽量补全 @REALM
        try:
            if k_user and "@" not in k_user and (hive_cfg.get("auth") or "").upper() == "KERBEROS":
                realm = (os.getenv("HIVE_KRB_REALM", "") or "").strip()
                if (not realm) and os.path.exists("/etc/krb5.conf"):
                    try:
                        with open("/etc/krb5.conf", "r") as _f:
                            for _ln in _f:
                                _ln2 = _ln.strip()
                                if not _ln2 or _ln2.startswith("#") or _ln2.startswith(";"):
                                    continue
                                if "default_realm" in _ln2 and "=" in _ln2:
                                    _k, _v = _ln2.split("=", 1)
                                    if _k.strip().lower() == "default_realm":
                                        realm = _v.strip()
                                        break
                    except Exception:
                        pass
                if realm:
                    k_user = "{0}@{1}".format(k_user, realm)
        except Exception:
            pass

        if k_user and k_keytab:
            kinit_cmd = "kinit -kt '{0}' '{1}'".format(k_keytab.replace("'", "'\\''"), k_user.replace("'", "'\\''"))
        elif k_user and k_pwd:
            # 兼容：通过 stdin 传密码（与参考脚本一致做法）
            kinit_cmd = "echo '{0}' | kinit '{1}'".format(k_pwd.replace("'", "'\\''"), k_user.replace("'", "'\\''"))

    # 关键点：JDBC URL 的 “/database” 若指向一个不存在的库，HiveServer2 可能直接拒绝建立 session，
    # 导致后续 CREATE DATABASE 也无法执行（你日志里的 ods_qjjdd 就是这种情况）。
    # 因此 beeline 默认连到 default（或 HIVE_BEELINE_CONNECT_DB 显式指定），再在 SQL 里执行 USE/CREATE DATABASE。
    connect_db = (os.getenv("HIVE_BEELINE_CONNECT_DB", "") or "").strip() or "default"
    jdbc_url = _build_hive_jdbc_url(hive_cfg, connect_db)
    username = (hive_cfg.get("username") or "").strip()

    # 打印最终 JDBC URL（脱敏：不打印可能包含的密码参数）
    try:
        url_show = jdbc_url
        url_show = re.sub(r"(password=)[^;]+", r"\\1***", to_text(url_show), flags=re.I)
        _print_u(
            u"【{0}】beeline jdbc_url={1} (connect_db={2}, target_db={3})".format(
                to_text(_now()), url_show, to_text(connect_db), to_text(hive_db)
            )
        )
    except Exception:
        pass

    # 合并多条 SQL，确保以 ; 结束（DDL 可能包含中文注释，需用 UTF-8 写入文件）
    parts = []
    for s in ddl_list or []:
        st = to_text(s).strip()
        if not st:
            continue
        if not st.endswith(";"):
            st += ";"
        parts.append(st)
    sql = u"\n".join(parts)

    # 写临时 sql 文件，使用 beeline -f 执行，避免 -e 复杂 quoting/编码问题
    tmp_path = None
    f = None
    try:
        f = tempfile.NamedTemporaryFile(prefix="hive_ddl_", suffix=".sql", delete=False)
        tmp_path = f.name
        data = sql
        try:
            if isinstance(data, text_type):
                data = data.encode("utf-8")
        except Exception:
            pass
        f.write(data)
        f.flush()
        f.close()
        f = None
    finally:
        try:
            if f:
                f.close()
        except Exception:
            pass

    # 组装 bash 命令：source 环境（若存在） -> kinit（可选） -> beeline
    bash_parts = []
    if hadoop_env_sh:
        bash_parts.append("if [ -f '{0}' ]; then source '{0}'; fi".format(hadoop_env_sh.replace("'", "'\\''")))
    if kinit_cmd:
        bash_parts.append(kinit_cmd)
    else:
        # Kerberos 场景下如果没有票据，beeline 很可能失败；提前提示但不中断
        try:
            if (hive_cfg.get("auth") or "").upper() == "KERBEROS":
                bash_parts.append("klist >/dev/null 2>&1 || echo '[WARN] no kerberos ticket (klist failed), beeline may fail' 1>&2")
        except Exception:
            pass

    # 注意：这里不用 -p/-w 等密码参数，避免泄露；Kerberos 场景建议外部 kinit。
    beeline_args = []
    beeline_args.append(beeline_cmd)
    beeline_args.append("-u")
    beeline_args.append("'{0}'".format(jdbc_url.replace("'", "'\\''")))
    if username:
        beeline_args.append("-n")
        beeline_args.append("'{0}'".format(username.replace("'", "'\\''")))
    if tmp_path:
        beeline_args.append("-f")
        beeline_args.append("'{0}'".format(to_text(tmp_path).replace("'", "'\\''")))
    else:
        # 兜底：仍用 -e（理论上不会走到）
        beeline_args.append("-e")
        beeline_args.append("'{0}'".format(to_text(sql).replace("'", "'\\''")))

    bash_parts.append(" ".join(beeline_args))
    bash_cmd = " && ".join(bash_parts) if bash_parts else " ".join(beeline_args)

    rc, out, err = _shell_capture(bash_cmd)
    return rc, out, err


def _build_beeline_merged_ddl(ddl_list, db_ident, tb_ident):
    """
    单次 beeline：USE -> SHOW（探测，用于统计是否曾存在）-> DROP IF EXISTS -> CREATE
    ddl_list 来自 build_hive_ddl：2 段 [USE, CREATE TABLE]
    """
    merged = []
    for s in (ddl_list or []):
        st = to_text(s).strip()
        if not st:
            continue
        if not st.endswith(";"):
            st += ";"
        merged.append(st)
        # 在 USE 语句后插入探测 + drop
        if st.upper().startswith("USE ") and tb_ident and db_ident:
            like_pat = tb_ident.replace("'", "''")
            merged.append(u"SHOW TABLES IN `{0}` LIKE '{1}';".format(db_ident, like_pat))
            merged.append(u"DROP TABLE IF EXISTS `{0}`;".format(tb_ident))
    return merged


def _beeline_stdout_indicates_table_existed(out_txt, tb_ident):
    """
    根据单次 beeline 合并输出，判断 SHOW TABLES 是否命中（表在 DROP 之前已存在）。
    采用“独立一行等于表名”优先，避免 CREATE TABLE 语句体误匹配。
    """
    if not tb_ident or not out_txt:
        return False
    want = to_text(tb_ident).strip()
    if not want:
        return False
    for line in to_text(out_txt).splitlines():
        ln = line.strip()
        if not ln:
            continue
        low = ln.lower()
        if low in ("tab_name", "table_name"):
            continue
        if low.startswith("|") or low.startswith("+"):
            continue
        if ln == want or ln.lower() == want.lower():
            return True
    if want in to_text(out_txt) and len(want) >= 8:
        return True
    return False


def _is_database_not_found_error(err_text):
    """
    判断错误是否为“目标数据库不存在”。
    """
    t = to_text(err_text or "")
    tl = t.lower()
    return ("database" in tl and "does not exist" in tl) or (u"数据库不存在" in t)


def _hive_table_exists_sql(hive_db, hive_table):
    db_ident = _safe_hive_ident(hive_db) or "default"
    tb_ident = _safe_hive_ident(hive_table)
    if not tb_ident:
        return None, None, None
    # SHOW TABLES IN db LIKE 'table'
    sql = u"SHOW TABLES IN `{0}` LIKE '{1}'".format(db_ident, tb_ident)
    return db_ident, tb_ident, sql


def _hive_table_exists_by_beeline(hive_cfg, hive_db, hive_table):
    db_ident, tb_ident, sql = _hive_table_exists_sql(hive_db, hive_table)
    if not sql:
        return False, ""
    rc, out, err = _beeline_exec_ddl([sql], hive_cfg=hive_cfg, hive_db=db_ident)
    # 若库不存在，直接认为表不存在（后续会走 CREATE DATABASE IF NOT EXISTS）
    try:
        err_txt = to_text(err or "")
        if "Database" in err_txt and "does not exist" in err_txt:
            return False, err_txt
    except Exception:
        pass
    # beeline 输出里如果出现表名（精确匹配），就认为存在
    try:
        out_txt = to_text(out or "")
    except Exception:
        out_txt = out or ""
    exists = False
    try:
        if tb_ident and (u"\n{0}\n".format(tb_ident) in out_txt or out_txt.strip().endswith(tb_ident)):
            exists = True
        elif tb_ident and tb_ident in out_txt:
            # 宽松匹配兜底
            exists = True
    except Exception:
        exists = False
    return exists, (err or out or "")


def _hive_table_exists_by_pyhive(cur, hive_db, hive_table):
    db_ident, tb_ident, sql = _hive_table_exists_sql(hive_db, hive_table)
    if not sql:
        return False
    # 先切库再查
    cur.execute(u"USE `{0}`".format(db_ident))
    cur.execute(sql)
    rows = cur.fetchall()
    try:
        if rows:
            for r in rows:
                try:
                    if r and to_text(r[0]).lower() == tb_ident.lower():
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False


def _format_table_items_every_10(items):
    """
    将表清单按每10个一行输出，便于日志阅读。
    """
    arr = []
    for it in items or []:
        t = to_text(it).strip()
        if t:
            arr.append(t)
    if not arr:
        return u"(无)"
    lines = []
    i = 0
    n = len(arr)
    while i < n:
        lines.append(u" | ".join(arr[i:i + 10]))
        i += 10
    return u"\n".join(lines)


def build_hive_ddl(
    hive_db,
    hive_table,
    oracle_table,
    columns,
    oracle_table_comment=None,
    sheet_table_cn_comment=None,
):
    db_ident = _safe_hive_ident(hive_db)
    tb_ident = _safe_hive_ident(hive_table)
    if not db_ident or not tb_ident:
        raise RuntimeError(
            u"hive_db/hive_table 为空或不合法：hive_db={0!r}, hive_table={1!r}".format(hive_db, hive_table)
        )

    col_lines = []
    seen_cols = set()
    for c in columns or []:
        col_name = _safe_hive_ident(c.get("name"))
        if not col_name:
            continue
        # <INTERNAL_DATASET> 固定作为 Hive 分区列存在，不允许作为普通字段建入表结构
        try:
            if to_text(col_name).lower() == "<INTERNAL_DATASET>":
                continue
        except Exception:
            if str(col_name).lower() == "<INTERNAL_DATASET>":
                continue
        try:
            seen_cols.add(to_text(col_name).lower())
        except Exception:
            try:
                seen_cols.add(str(col_name).lower())
            except Exception:
                pass
        hive_type = _ddl_column_type(c)
        comment = _escape_hive_comment(c.get("comment") or "")
        if comment:
            col_lines.append(u"  `{0}` {1} COMMENT '{2}'".format(col_name, hive_type, comment))
        else:
            col_lines.append(u"  `{0}` {1}".format(col_name, hive_type))

    # 固定追加 lgdsj_* 字段（若已存在则不重复）
    fixed_cols = [
        ("lgdsj_timeflag", "timestamp", ""),
        ("lgdsj_data_source", "string", u"来源部门与系统名称"),
        ("lgdsj_load_time", "timestamp", u"写入时间戳"),
    ]
    for name, typ, cmt in fixed_cols:
        nm = _safe_hive_ident(name)
        if not nm:
            continue
        try:
            if to_text(nm).lower() in seen_cols:
                continue
        except Exception:
            pass
        if cmt:
            col_lines.append(u"  `{0}` {1} COMMENT '{2}'".format(nm, typ, _escape_hive_comment(cmt)))
        else:
            col_lines.append(u"  `{0}` {1}".format(nm, typ))
    if not col_lines:
        raise RuntimeError(u"源 Hive 表无字段/无法解析字段：{0}".format(oracle_table))

    table_comment_src = _trim_token(oracle_table_comment)
    if not table_comment_src:
        table_comment_src = _trim_token(sheet_table_cn_comment)
    table_comment_clause = u""
    if table_comment_src:
        table_comment_clause = u"\nCOMMENT '{0}'".format(_escape_hive_comment(table_comment_src))
    ddl = []
    ddl.append(u"USE `{0}`".format(db_ident))
    ddl.append(
        u"CREATE TABLE IF NOT EXISTS `{0}` (\n{1}\n){2}\nPARTITIONED BY (`<INTERNAL_DATASET>` string COMMENT '分区日期')\nSTORED AS ORC\nTBLPROPERTIES ('orc.compress'='SNAPPY')".format(
            tb_ident, u",\n".join(col_lines), table_comment_clause
        )
    )
    return ddl


# ==================== Main flow ====================
def main():
    doc_lib_name, file_name, sheet_name = _require_cli_args()

    center_user = (os.getenv("KINGSOFT_CENTER_USER", "") or "lgbs").strip()
    center_user = to_text(center_user)

    # 目标字段名（按你的需求固定）
    F_CENTER = u"二期-中心库账号"
    F_ORACLE_TABLE = u"二期-源表"
    F_HIVE_DB = u"三期-hive-ods库"
    F_HIVE_TABLE = u"三期-ods表英文名"
    F_SHEET_TABLE_CN = u"规范化-三期-数据开发-表中文名"

    print("=" * 80)
    _print_u(u"【{0}】作业开始：多维表->Hive源库表结构->各部门Hive目标库建表".format(to_text(_now())))
    _print_u(u"【{0}】入参：doc_lib_name={1}, file_name={2}, sheet_name={3}".format(to_text(_now()), doc_lib_name, file_name, sheet_name))
    _print_u(u"【{0}】过滤条件：{1} == {2}".format(to_text(_now()), F_CENTER, center_user))
    try:
        _print_u(
            u"【{0}】Hive 源库（默认）HIVE_SOURCE_DATABASE={1}，可通过环境变量 HIVE_SOURCE_DATABASE / LGBS_HIVE_SOURCE_DB 覆盖".format(
                to_text(_now()), to_text(HIVE_SOURCE_DATABASE)
            )
        )
    except Exception:
        pass
    print("=" * 80)

    auth = get_auth()
    file_id, sheet_id = resolve_file_sheet_ids(auth, doc_lib_name=doc_lib_name, file_name=file_name, sheet_name=sheet_name)
    print("【{0}】目标定位完成：file_id={1}, sheet_id={2}".format(_now(), file_id, sheet_id))

    # Hive 连接（database 动态切换为每条记录的 hive_db；但 Connection 只能固定 database）
    # 这里按记录逐个切换：执行 CREATE DATABASE / USE db / CREATE TABLE
    # 若运行环境缺少 pyhive/thrift：优先改用 beeline 执行（无需 Python 依赖）；若 beeline 不可用再降级为仅输出。
    hive = None
    pyhive_import_err = None
    try:
        from pyhive import hive as _hive_mod  # 延迟导入，避免无依赖时启动就报错

        hive = _hive_mod
    except Exception as e:
        pyhive_import_err = e
        _print_u(
            u"【{0}】WARNING: 导入 pyhive 失败，将尝试使用 beeline 读取源表 DESCRIBE 并执行 DDL。err={1}".format(
                to_text(_now()), to_text(e)
            )
        )
        _print_u(u"【{0}】如果 beeline 也不可用，才会降级为仅输出 DDL。".format(to_text(_now())))

    hive_cfg_base = read_hive_user_config()
    hive_host = hive_cfg_base.get("st") or hive_cfg_base.get("host", "")
    hive_port = int(hive_cfg_base.get("port", "21066")) if str(hive_cfg_base.get("port", "")).isdigit() else 21066
    hive_username = hive_cfg_base.get("username", "")
    hive_auth = hive_cfg_base.get("auth", "KERBEROS")
    hive_ksn = hive_cfg_base.get("kerberos_service_name", "hive")
    hive_krbhost = hive_cfg_base.get("krbhost", "")

    # 源表元数据：优先 pyhive 长连接；否则 beeline DESCRIBE
    src_meta_hive_cfg = None
    try:
        _bc_db = (os.getenv("HIVE_BEELINE_CONNECT_DB", "") or "").strip() or "default"
        src_meta_hive_cfg = get_hive_config(_bc_db)
    except Exception as _cfg_e:
        _print_u(u"【{0}】WARNING: 加载 beeline 用 Hive 配置失败，无 pyhive 时无法 DESCRIBE 源表。err={1}".format(to_text(_now()), to_text(_cfg_e)))

    src_meta_conn = None
    if hive is not None:
        try:
            src_meta_conn = hive.Connection(
                host=hive_host,
                port=hive_port,
                username=hive_username,
                database=_safe_hive_ident(HIVE_SOURCE_DATABASE) or "default",
                auth=hive_auth,
                kerberos_service_name=hive_ksn,
                krbhost=hive_krbhost,
            )
            _print_u(
                u"【{0}】源库 Hive 元数据连接已建立（database={1}，读取时仍按记录 USE 源库）".format(
                    to_text(_now()), to_text(_safe_hive_ident(HIVE_SOURCE_DATABASE) or "default")
                )
            )
        except Exception as _se:
            src_meta_conn = None
            _print_u(
                u"【{0}】WARNING: 源库 Hive 元数据 pyhive 连接失败，将改用 beeline DESCRIBE。err={1}".format(to_text(_now()), to_text(_se))
            )

    total_records = 0
    matched_records = 0
    ddl_success = 0
    ddl_failed = 0
    ddl_dry_run = 0
    ddl_by_beeline = 0
    ddl_deleted = 0
    ddl_created = 0
    deleted_tables = []
    created_tables = []
    dry_run_tables = []
    failed_tables = []
    skipped_missing_fields = 0
    # beeline 优化：按库分组，单次执行多张表
    beeline_group_jobs = {}
    beeline_group_order = []

    # page iterator
    page_size = int(os.getenv("KINGSOFT_PAGE_SIZE", "100") or "100")
    view_id = (os.getenv("KINGSOFT_VIEW_ID", "") or "").strip()
    page_num = 1

    while True:
        try:
            resp = list_records_by_page(auth, file_id=file_id, sheet_id=sheet_id, page_num=page_num, page_size=page_size, view_id=view_id)
        except Exception as e:
            raise RuntimeError("拉取 records 失败：page_num={0} err={1}".format(page_num, e))

        data = (resp or {}).get("data", {}) if isinstance(resp, dict) else {}
        recs = []
        if isinstance(data, dict):
            recs = data.get("records") or data.get("items") or data.get("list") or []
        if not isinstance(recs, list):
            recs = []
        if not recs:
            break

        print("【{0}】分页拉取：page_num={1}, page_size={2}, 本页记录数={3}".format(_now(), page_num, page_size, len(recs)))

        for rec in recs:
            if not isinstance(rec, dict):
                continue
            total_records += 1
            fields = _parse_record_fields(rec)
            if not fields:
                skipped_missing_fields += 1
                continue

            v_center = fields.get(F_CENTER)
            if v_center is None:
                # 兼容 key 可能被接口返回为 str
                v_center = fields.get(to_text(F_CENTER))
            if to_text(v_center).strip() != center_user:
                continue

            matched_records += 1
            # 「二期-中心库账号」：行过滤字段，同时作为源 Hive 库名（一般为 lgbs）
            source_hive_db_field = _normalize_identifier_token(fields.get(F_CENTER) or "")
            source_table_raw = to_text(fields.get(F_ORACLE_TABLE) or "")
            source_table_norm = _normalize_identifier_token(source_table_raw)
            hive_db = _normalize_identifier_token(fields.get(F_HIVE_DB) or "")
            hive_table = _normalize_identifier_token(fields.get(F_HIVE_TABLE) or "")
            sheet_table_cn_raw = to_text(fields.get(F_SHEET_TABLE_CN) or fields.get(to_text(F_SHEET_TABLE_CN)) or "")
            sheet_table_cn = _normalize_identifier_token(sheet_table_cn_raw)
            full_tb_raw = u"{0}.{1}".format(to_text(hive_db), to_text(hive_table))

            s_db, s_tbl = _parse_owner_table(source_table_raw)
            if not s_tbl:
                s_tbl = source_table_norm
            if not s_db:
                s_db = source_hive_db_field
            if not s_db:
                s_db = HIVE_SOURCE_DATABASE
            src_full_label = u"{0}.{1}".format(to_text(s_db), to_text(s_tbl))

            if not s_tbl or not hive_db or not hive_table:
                skipped_missing_fields += 1
                _print_u(
                    u"【{0}】记录缺少必要字段，跳过：source_hive_db={1}, source_table={2}, hive_db={3}, hive_table={4}".format(
                        to_text(_now()), s_db, s_tbl, hive_db, hive_table
                    )
                )
                continue

            _print_u(
                u"【{0}】处理记录：source_hive={1}.{2} -> target_hive={3}.{4}".format(
                    to_text(_now()), to_text(s_db), to_text(s_tbl), to_text(hive_db), to_text(hive_table)
                )
            )

            # 1) 源 Hive 库取字段（DESCRIBE）
            try:
                if src_meta_conn is not None:
                    cur_m = src_meta_conn.cursor()
                    try:
                        cols = _hive_fetch_table_columns_pyhive(cur_m, s_db, s_tbl)
                        if not cols:
                            raise RuntimeError("未获取到字段信息（表不存在/无权限？）")
                        table_comment = ""
                        if os.getenv("SKIP_HIVE_SOURCE_TABLE_COMMENT", "").strip().lower() not in ("1", "true", "yes"):
                            try:
                                table_comment = _hive_fetch_table_comment_pyhive(cur_m, s_db, s_tbl)
                            except Exception:
                                table_comment = ""
                    finally:
                        try:
                            cur_m.close()
                        except Exception:
                            pass
                elif src_meta_hive_cfg is not None:
                    _want_src_tbl_comment = (
                        os.getenv("SKIP_HIVE_SOURCE_TABLE_COMMENT", "").strip().lower() not in ("1", "true", "yes")
                    )
                    cols, table_comment = _hive_fetch_table_schema_beeline(
                        src_meta_hive_cfg, s_db, s_tbl, want_comment=_want_src_tbl_comment
                    )
                    if not cols:
                        raise RuntimeError("未获取到字段信息（表不存在/无权限？）")
                else:
                    raise RuntimeError("无法读取源表：既无 pyhive 连接也无可用 Hive 配置以运行 beeline")
            except Exception as e:
                ddl_failed += 1
                failed_tables.append(full_tb_raw)
                _print_u(
                    u"【{0}】源Hive表结构获取失败：{1} err={2}".format(to_text(_now()), src_full_label, to_text(e))
                )
                try:
                    _print_u(
                        u"【{0}】源表名诊断：table_raw={1} raw_hex={2} | table_norm={3} norm_hex={4}".format(
                            to_text(_now()),
                            to_text(source_table_raw),
                            to_text(_to_hex_preview(source_table_raw)),
                            to_text(source_table_norm),
                            to_text(_to_hex_preview(source_table_norm)),
                        )
                    )
                except Exception:
                    pass
                continue

            # 2) 生成 DDL
            try:
                ddl_list = build_hive_ddl(
                    hive_db=hive_db,
                    hive_table=hive_table,
                    oracle_table=src_full_label,
                    columns=cols,
                    oracle_table_comment=table_comment,
                    sheet_table_cn_comment=sheet_table_cn,
                )
            except Exception as e:
                ddl_failed += 1
                failed_tables.append(full_tb_raw)
                _print_u(u"【{0}】DDL 生成失败：hive_db={1}, hive_table={2} err={3}".format(to_text(_now()), hive_db, hive_table, to_text(e)))
                continue

            # 3) 执行 DDL（若 hive 依赖缺失，则仅输出）
            try:
                if hive is None:
                    db_ident = _safe_hive_ident(hive_db) or "default"
                    tb_ident = _safe_hive_ident(hive_table)
                    full_tb = u"{0}.{1}".format(db_ident, tb_ident) if tb_ident else u"{0}.{1}".format(to_text(hive_db), to_text(hive_table))
                    if db_ident not in beeline_group_jobs:
                        beeline_group_jobs[db_ident] = []
                        beeline_group_order.append(db_ident)
                    beeline_group_jobs[db_ident].append(
                        {
                            "db_ident": db_ident,
                            "tb_ident": tb_ident,
                            "full_tb": full_tb,
                            "full_tb_raw": full_tb_raw,
                            "ddl_list": ddl_list,
                        }
                    )
                    continue

                hive_conn = hive.Connection(
                    host=hive_host,
                    port=hive_port,
                    username=hive_username,
                    database=_safe_hive_ident(hive_db) or "default",
                    auth=hive_auth,
                    kerberos_service_name=hive_ksn,
                    krbhost=hive_krbhost,
                )
                cur = hive_conn.cursor()
                try:
                    db_ident = _safe_hive_ident(hive_db) or "default"
                    tb_ident = _safe_hive_ident(hive_table)
                    full_tb = u"{0}.{1}".format(db_ident, tb_ident) if tb_ident else u"{0}.{1}".format(to_text(hive_db), to_text(hive_table))
                    try:
                        exists = _hive_table_exists_by_pyhive(cur, hive_db=hive_db, hive_table=hive_table)
                    except Exception:
                        # 若存在性检测失败，不影响后续执行（保留原行为）
                        exists = False
                    if exists and tb_ident:
                        _hive_exec(cur, u"USE `{0}`".format(db_ident))
                        _hive_exec(cur, u"DROP TABLE IF EXISTS `{0}`".format(tb_ident))
                        ddl_deleted += 1
                        deleted_tables.append(full_tb)
                        _print_u(u"【{0}】目标表已存在，已删除并准备重建：{1}".format(to_text(_now()), full_tb))
                    for sql in ddl_list:
                        _hive_exec(cur, sql)
                    try:
                        hive_conn.commit()
                    except Exception:
                        # pyhive/hive 有的实现不需要 commit
                        pass
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
                    try:
                        hive_conn.close()
                    except Exception:
                        pass
                ddl_success += 1
                ddl_created += 1
                created_tables.append(full_tb)
                _print_u(u"【{0}】DDL 执行成功（新建完成）：{1}".format(to_text(_now()), full_tb))
            except Exception as e:
                ddl_failed += 1
                failed_tables.append(full_tb_raw)
                if _is_database_not_found_error(e):
                    _print_u(u"【{0}】新建表失败：{1}，未找到对应的数据库".format(
                        to_text(_now()), full_tb_raw
                    ))
                _print_u(u"【{0}】DDL 执行失败：{1}.{2} err={3}".format(to_text(_now()), hive_db, hive_table, to_text(e)))
                continue

        if len(recs) < page_size:
            break
        page_num += 1

    # beeline 优化执行：按库分组，单次 beeline 执行该库下多张表 DDL
    if hive is None and beeline_group_order:
        for db_ident in beeline_group_order:
            jobs = beeline_group_jobs.get(db_ident) or []
            if not jobs:
                continue
            try:
                hive_cfg = get_hive_config(db_ident)
            except Exception as e:
                for job in jobs:
                    ddl_dry_run += 1
                    ddl_failed += 1
                    dry_run_tables.append(job.get("full_tb"))
                    failed_tables.append(job.get("full_tb_raw") or job.get("full_tb"))
                _print_u(u"【{0}】WARNING: 读取 Hive 配置失败，库 {1} 下 {2} 张表仅输出 DDL。err={3}".format(
                    to_text(_now()), to_text(db_ident), len(jobs), to_text(e)
                ))
                continue

            merged_sql = []
            for job in jobs:
                merged_sql.extend(
                    _build_beeline_merged_ddl(job.get("ddl_list") or [], db_ident=job.get("db_ident"), tb_ident=job.get("tb_ident"))
                )

            _print_u(u"【{0}】beeline 分组执行：hive_db={1}，本组表数={2}".format(
                to_text(_now()), to_text(db_ident), len(jobs)
            ))
            rc, out, err = _beeline_exec_ddl(ddl_list=merged_sql, hive_cfg=hive_cfg, hive_db=db_ident)
            if rc == 0:
                for job in jobs:
                    tb_ident = job.get("tb_ident")
                    full_tb = job.get("full_tb")
                    existed_before_drop = False
                    if tb_ident:
                        try:
                            existed_before_drop = _beeline_stdout_indicates_table_existed(out, tb_ident)
                        except Exception:
                            existed_before_drop = False
                    if existed_before_drop:
                        ddl_deleted += 1
                        deleted_tables.append(full_tb)
                        _print_u(u"【{0}】目标表已存在，已删除并准备重建：{1}".format(to_text(_now()), full_tb))
                    ddl_by_beeline += 1
                    ddl_success += 1
                    ddl_created += 1
                    created_tables.append(full_tb)
                    _print_u(u"【{0}】DDL 执行成功（beeline 按库分组，新建完成）：{1}".format(to_text(_now()), full_tb))
            else:
                if err:
                    _print_u(u"【{0}】beeline 分组执行失败 stderr(截断)：\n{1}".format(to_text(_now()), to_text(err)[:8000]))
                if out:
                    _print_u(u"【{0}】beeline 分组执行失败 stdout(截断)：\n{1}".format(to_text(_now()), to_text(out)[:8000]))
                for job in jobs:
                    full_tb = job.get("full_tb")
                    ddl_dry_run += 1
                    ddl_failed += 1
                    dry_run_tables.append(full_tb)
                    failed_tables.append(job.get("full_tb_raw") or full_tb)
                    if _is_database_not_found_error(err):
                        _print_u(u"【{0}】新建表失败：{1}，未找到对应的数据库".format(
                            to_text(_now()), full_tb
                        ))
                    _print_u(u"【{0}】WARNING: beeline 分组执行失败，降级为仅输出 DDL。table={1}".format(
                        to_text(_now()), full_tb
                    ))
                    _print_u(u"【{0}】DRY-RUN: 以下 DDL 未执行，仅输出（table={1}）：".format(
                        to_text(_now()), full_tb
                    ))
                    for sql in (job.get("ddl_list") or []):
                        try:
                            _print_u(to_text(sql))
                        except Exception:
                            print(sql)

    if src_meta_conn is not None:
        try:
            src_meta_conn.close()
        except Exception:
            pass

    print("=" * 80)
    _print_u(u"【{0}】作业完成汇总：".format(to_text(_now())))
    _print_u(u"- 总记录数：{0}".format(total_records))
    _print_u(u"- 命中过滤记录数：{0}".format(matched_records))
    _print_u(u"- 建表成功：{0}".format(ddl_success))
    _print_u(u"- 建表失败：{0}".format(ddl_failed))
    _print_u(u"【{0}】建表失败的表（按“表空间.表名”，每10个一行）：\n{1}".format(
        to_text(_now()), _format_table_items_every_10(failed_tables)
    ))
    _print_u(u"- 删除表数量：{0}".format(ddl_deleted))
    _print_u(u"- 新建表数量：{0}".format(ddl_created))
    _print_u(u"- 使用 beeline 执行成功：{0}".format(ddl_by_beeline))
    _print_u(u"- 仅输出DDL未执行（缺少pyhive/thrift）：{0}".format(ddl_dry_run))
    _print_u(u"【{0}】仅输出DDL未执行的表（按“表空间.表名”，每10个一行）：\n{1}".format(
        to_text(_now()), _format_table_items_every_10(dry_run_tables)
    ))
    _print_u(u"- 跳过（fields缺失/必要字段缺失）：{0}".format(skipped_missing_fields))
    _print_u(u"【{0}】删除的表（按“表空间.表名”，每10个一行）：\n{1}".format(
        to_text(_now()), _format_table_items_every_10(deleted_tables)
    ))
    _print_u(u"【{0}】新建的表（按“表空间.表名”，每10个一行）：\n{1}".format(
        to_text(_now()), _format_table_items_every_10(created_tables)
    ))
    print("=" * 80)


if __name__ == "__main__":
    main()

