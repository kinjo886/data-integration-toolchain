# -*- coding: utf-8 -*-
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
from email.utils import formatdate
from _compat import http_lib, text_type, urllib_parse

# ==================== 全局配置 ====================
API_HOST = os.getenv("KINGSOFT_API_HOST", "<INTERNAL_API_HOST>")
API_PORT = int(os.getenv("KINGSOFT_API_PORT", "5489"))
DEFAULT_CLIENT_ID = os.getenv("KINGSOFT_CLIENT_ID", "<YOUR_APP_ID>")
DEFAULT_CLIENT_SECRET = os.getenv("KINGSOFT_CLIENT_SECRET", "<YOUR_APP_SECRET>")
DEFAULT_APP_ID = DEFAULT_CLIENT_ID
DEFAULT_APP_KEY = DEFAULT_CLIENT_SECRET
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

# API路径
API_PATH_OAUTH_TOKEN = "/openapi/oauth2/token"
API_PATH_DOCLIB_SEARCH = "/openapi/v7/doclib/search"
API_PATH_FILES_SEARCH = "/openapi/v7/files/search"
API_PATH_FILE_SCHEMA = "/openapi/v7/coop/dbsheet/{file_id}/schema"
API_PATH_FILE_RECORDS_BY_PAGE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records/list_by_page"
API_PATH_FILE_RECORD_BATCH_UPDATE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records/batch_update"

# 业务字段常量
MIGRATE_DELETE_FLAG = "是否已删除"
MIGRATE_FINISH_FLAG = "三期-是否完成"

def _to_text(x):
    if x is None:
        return ""
    if isinstance(x, text_type):
        return x
    if sys.version_info[0] < 3:
        try:
            if isinstance(x, str):
                return x.decode("utf-8", "replace")
        except Exception:
            pass
    try:
        return text_type(x)
    except Exception:
        return text_type(repr(x))

def _to_bytes(s):
    if s is None:
        return b"" if sys.version_info[0] >= 3 else ""
    if sys.version_info[0] >= 3:
        if isinstance(s, bytes):
            return s
        return _to_text(s).encode("utf-8")
    try:
        if isinstance(s, unicode):  # type: ignore[name-defined]
            return s.encode("utf-8")
    except Exception:
        pass
    return _to_text(s).encode("utf-8")

def _urlencode(query_items, doseq=False):
    try:
        return urllib_parse.urlencode(query_items, doseq=doseq)
    except Exception:
        parts = []
        for k, v in query_items:
            parts.append("{0}={1}".format(urllib_parse.quote(str(k)), urllib_parse.quote(str(v))))
        return "&".join(parts)

def _sanitize_for_json(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            out[_to_text(k)] = _sanitize_for_json(v)
        return out
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(x) for x in obj]
    if sys.version_info[0] >= 3 and isinstance(obj, bytes):
        return obj.decode("utf-8", "replace")
    if sys.version_info[0] < 3:
        try:
            if isinstance(obj, str):
                return obj.decode("utf-8", "replace")
        except Exception:
            pass
    return obj

def _normalize_compare_text(s):
    t = _to_text(s).replace(u"\u3000", " ").strip()
    out = []
    for ch in t:
        cat = unicodedata.category(ch)
        if cat in ("Cc", "Cf"):
            continue
        out.append(ch)
    return u"".join(out).lower()

def _extract_cell_text(value):
    if value is None:
        return ""
    if _is_string(value):
        s = _to_text(value).strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            parsed = _json_loads_loose(s)
            if parsed is not None:
                return _extract_cell_text(parsed)
        return s
    if isinstance(value, dict):
        for k in ("text", "name", "value", "label", "title"):
            if k in value:
                res = _extract_cell_text(value[k])
                if res:
                    return res
        for v in value.values():
            res = _extract_cell_text(v)
            if res:
                return res
        return ""
    if isinstance(value, (list, tuple)):
        for it in value:
            res = _extract_cell_text(it)
            if res:
                return res
        return ""
    return _to_text(value).strip()

def _is_string(obj):
    try:
        return isinstance(obj, basestring)  # type: ignore[name-defined]
    except Exception:
        return isinstance(obj, (str, text_type))

def _json_loads_loose(text):
    try:
        return json.loads(text)
    except Exception:
        try:
            return ast.literal_eval(text)
        except Exception:
            return None

def _get_field_value_by_name(fields_dict, desired_name):
    if not isinstance(fields_dict, dict):
        return False, None
    if desired_name in fields_dict:
        return True, fields_dict[desired_name]
    dn = _normalize_compare_text(desired_name)
    for k in fields_dict.keys():
        if _normalize_compare_text(k) == dn:
            return True, fields_dict[k]
    return False, None

def _match_name_either_contains(query, target):
    q = _normalize_compare_text(query)
    t = _normalize_compare_text(target)
    if not q or not t:
        return False
    return q in t or t in q

def _pick_best(query, candidates, get_name):
    if not candidates:
        raise RuntimeError(f"无匹配资源，查询关键字：{_to_text(query)}")
    named = []
    for c in candidates:
        try:
            n = get_name(c) or ""
        except Exception:
            n = ""
        named.append((_to_text(n), c))
    q_norm = _normalize_compare_text(query)
    exact = [(n, c) for n, c in named if _normalize_compare_text(n) == q_norm]
    pool = exact if exact else [(n,c) for n,c in named if q_norm in _normalize_compare_text(n)]
    if not pool:
        pool = [(n,c) for n,c in named if _match_name_either_contains(query, n)]
    if not pool:
        sample = [n for n,_ in named[:10]]
        raise RuntimeError(f"未匹配到资源，query={query}，候选样本：{sample}")
    pool.sort(key=lambda x: (len(x[0]), x[0].lower()))
    if len(pool) > 1:
        print(f"匹配多条资源，自动选择最短名称：{[n for n,_ in pool[:5]]}")
    return pool[0][1]

def get_request_headers(method, url_for_sign, body="", content_type=CONTENT_TYPE_FORM_URLENCODED, sign_path_override=None):
    app_id = DEFAULT_APP_ID
    app_key = DEFAULT_APP_KEY
    method = method.upper()
    date_string = formatdate(usegmt=True)
    if sign_path_override is not None:
        path = sign_path_override
    else:
        parts = url_for_sign.split(URL_SPLIT_KEYWORD, 1)
        path = parts[1] if len(parts)==2 else url_for_sign
    body_bytes = _to_bytes(body)
    if not body_bytes:
        base_str = f"{SIGNATURE_PREFIX}{method}{path}{content_type}{date_string}"
    else:
        sha256 = hashlib.sha256(body_bytes).hexdigest()
        base_str = f"{SIGNATURE_PREFIX}{method}{path}{content_type}{date_string}{sha256}"
    sign = hmac.new(app_key.encode("utf-8"), base_str.encode("utf-8"), hashlib.sha256).hexdigest()
    auth_header = f"{SIGNATURE_PREFIX} {app_id}:{sign}"
    return {
        "X-Kso-Date": date_string,
        "Content-Type": content_type,
        "X-Kso-Authorization": auth_header
    }

def _http_request(method, path, headers, body=None, timeout=60):
    conn = http_lib.HTTPConnection(API_HOST, API_PORT, timeout=timeout)
    try:
        req_body = _to_bytes(body) if body is not None else b""
        conn.request(method, path, body=req_body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        text = data.decode("utf-8", "replace")
        return resp.status, text
    finally:
        conn.close()

class KingsoftDbtClient:
    def __init__(self):
        self.auth = self._authorize()
        self.drive_cache = {}

    def _authorize(self):
        method = "POST"
        path = API_PATH_OAUTH_TOKEN
        payload = _urlencode([
            ("grant_type", OAUTH_GRANT_TYPE),
            ("client_id", DEFAULT_CLIENT_ID),
            ("client_secret", DEFAULT_CLIENT_SECRET)
        ])
        url_sign = f"http://{API_HOST}:{API_PORT}{path}"
        signed_headers = get_request_headers(method, url_sign, payload, CONTENT_TYPE_FORM_URLENCODED)
        headers = {
            "Accept": HTTP_HEADER_ACCEPT,
            "Accept-Encoding": HTTP_HEADER_ACCEPT_ENCODING,
            "User-Agent": HTTP_HEADER_USER_AGENT,
            "Connection": HTTP_HEADER_CONNECTION
        }
        headers.update(signed_headers)
        status, text = _http_request(method, path, headers, payload)
        if status != 200:
            raise RuntimeError(f"金山鉴权失败 HTTP{status}：{text[:1000]}")
        resp = json.loads(text)
        token = resp.get("access_token")
        token_type = resp.get("token_type", OAUTH_DEFAULT_TOKEN_TYPE)
        if not token:
            raise RuntimeError("未获取access_token")
        return {"access_token": token, "token_type": token_type}

    def _auth_header(self):
        return {"Authorization": f"{self.auth['token_type']} {self.auth['access_token']}"}

    def search_doclib(self, keyword):
        query = [
            ("page_size", "200"),
            ("company_id", DEFAULT_COMPANY_ID),
            ("keyword", keyword)
        ]
        path = f"{API_PATH_DOCLIB_SEARCH}?{_urlencode(query)}"
        method = "GET"
        url_sign = f"http://{API_HOST}:{API_PORT}{path}"
        signed = get_request_headers(method, url_sign, "", CONTENT_TYPE_OCTET_STREAM)
        headers = self._auth_header()
        headers.update(signed)
        headers["Content-Type"] = CONTENT_TYPE_OCTET_STREAM
        status, text = _http_request(method, path, headers)
        if status != 200:
            raise RuntimeError(f"文档库查询失败 HTTP{status}")
        resp = json.loads(text)
        data = resp.get("data", {})
        items = data.get("items", []) if isinstance(data, dict) else resp.get("items", [])
        out = []
        for item in items:
            if isinstance(item, dict):
                out.append(item)
        return out

    def _get_doclib_info(self, item):
        base = item.get("doclib", item)
        drive = base.get("drive", {})
        name = ""
        for k in ("name", "display_name", "title"):
            val = base.get(k) or drive.get(k)
            if _to_text(val).strip():
                name = _to_text(val).strip()
                break
        drive_id = _to_text(drive.get("id", "")).strip()
        return name, drive_id

    def get_drive_id_by_doclib_name(self, doclib_name):
        if doclib_name in self.drive_cache:
            return self.drive_cache[doclib_name]
        candidates = self.search_doclib(doclib_name)
        if not candidates:
            candidates = self.search_doclib("")
        target = _pick_best(doclib_name, candidates, lambda x: self._get_doclib_info(x)[0])
        _, drive_id = self._get_doclib_info(target)
        if not drive_id:
            raise RuntimeError("文档库drive_id为空")
        self.drive_cache[doclib_name] = drive_id
        return drive_id

    def search_file(self, drive_id, file_name):
        query = [("keyword", file_name), ("type", "file_name"), ("page_size", "100")]
        query.extend([("drive_ids", drive_id)])
        path = f"{API_PATH_FILES_SEARCH}?{_urlencode(query, doseq=True)}"
        method = "GET"
        url_sign = f"http://{API_HOST}:{API_PORT}{path}"
        signed = get_request_headers(method, url_sign, "", CONTENT_TYPE_OCTET_STREAM)
        headers = self._auth_header()
        headers.update(signed)
        headers["Content-Type"] = CONTENT_TYPE_OCTET_STREAM
        status, text = _http_request(method, path, headers)
        if status != 200:
            raise RuntimeError(f"文件查询失败 HTTP{status}")
        resp = json.loads(text)
        data = resp.get("data", {})
        items = data.get("items", [])
        file_list = []
        for it in items:
            f = it.get("file", it)
            if isinstance(f, dict):
                file_list.append(f)
        return file_list

    def get_file_id(self, drive_id, file_name):
        candidates = self.search_file(drive_id, file_name)
        target = _pick_best(file_name, candidates, lambda x: _to_text(x.get("name", "")))
        fid = _to_text(target.get("id", "")).strip()
        if not fid:
            raise RuntimeError("文件id为空")
        return fid

    def get_sheet_meta(self, file_id, sheet_name):
        method = "GET"
        path = API_PATH_FILE_SCHEMA.format(file_id=file_id)
        url_sign = f"http://{API_HOST}:{API_PORT}{path}"
        signed = get_request_headers(method, url_sign, "", CONTENT_TYPE_OCTET_STREAM)
        headers = self._auth_header()
        headers.update(signed)
        headers["Content-Type"] = CONTENT_TYPE_OCTET_STREAM
        status, text = _http_request(method, path, headers)
        if status != 200:
            raise RuntimeError(f"获取schema失败 HTTP{status}")
        schema = json.loads(text)
        sheets = schema.get("data", {}).get("sheets", [])
        sheet_target = None
        for s in sheets:
            sname = _to_text(s.get("name", ""))
            if _match_name_either_contains(sheet_name, sname):
                sheet_target = s
                break
        if not sheet_target:
            raise RuntimeError(f"未匹配sheet：{sheet_name}")
        return _to_text(sheet_target.get("id")), schema

    def iter_all_records(self, file_id, sheet_id):
        page = 1
        while True:
            body = json.dumps({"page_num": page, "page_size": 100, "prefer_id": False}, ensure_ascii=False)
            method = "POST"
            path = API_PATH_FILE_RECORDS_BY_PAGE.format(file_id=file_id, sheet_id=sheet_id)
            url_sign = f"http://{API_HOST}:{API_PORT}{path}"
            signed = get_request_headers(method, url_sign, body, CONTENT_TYPE_JSON)
            headers = self._auth_header()
            headers.update(signed)
            headers["Content-Type"] = CONTENT_TYPE_JSON
            status, text = _http_request(method, path, headers, body)
            if status != 200:
                raise RuntimeError(f"分页查询记录失败 HTTP{status}")
            resp = json.loads(text)
            records = resp.get("data", {}).get("records", [])
            if not records:
                break
            for rec in records:
                fields = rec.get("fields", {})
                if _is_string(fields):
                    fields = _json_loads_loose(fields) or {}
                yield {
                    "record_id": rec.get("id"),
                    "fields": fields
                }
            if len(records) < 100:
                break
            page += 1

    def load_all_migrate_records(self, doclib_name, file_name, sheet_name):
        drive_id = self.get_drive_id_by_doclib_name(doclib_name)
        file_id = self.get_file_id(drive_id, file_name)
        sheet_id, _ = self.get_sheet_meta(file_id, sheet_name)
        raw_records = []
        for item in self.iter_all_records(file_id, sheet_id):
            fields = item["fields"]
            row = {}
            # 全量读取所有字段
            for k in fields.keys():
                found, val = _get_field_value_by_name(fields, k)
                row[_to_text(k)] = _extract_cell_text(val)
            row["record_id"] = item["record_id"]
            # 过滤标记
            del_flag = _normalize_compare_text(row.get(MIGRATE_DELETE_FLAG, ""))
            finish_flag = _normalize_compare_text(row.get(MIGRATE_FINISH_FLAG, ""))
            row["_skip_delete"] = (del_flag == "是")
            row["_skip_finish"] = (finish_flag == "是")
            raw_records.append(row)
        return {
            "source_drive_id": drive_id,
            "source_file_id": file_id,
            "source_sheet_id": sheet_id,
            "records": raw_records
        }

    def batch_update_records(self, file_id, sheet_id, update_list):
        """批量回填Dbt状态"""
        if not update_list:
            return
        batch_body = {"records": update_list}
        body = json.dumps(_sanitize_for_json(batch_body), ensure_ascii=False)
        method = "POST"
        path = API_PATH_FILE_RECORD_BATCH_UPDATE.format(file_id=file_id, sheet_id=sheet_id)
        url_sign = f"http://{API_HOST}:{API_PORT}{path}"
        signed = get_request_headers(method, url_sign, body, CONTENT_TYPE_JSON)
        headers = self._auth_header()
        headers.update(signed)
        headers["Content-Type"] = CONTENT_TYPE_JSON
        status, text = _http_request(method, path, headers, body)
        if status != 200:
            raise RuntimeError(f"批量更新Dbt记录失败 HTTP{status}：{text[:1000]}")
        return json.loads(text)