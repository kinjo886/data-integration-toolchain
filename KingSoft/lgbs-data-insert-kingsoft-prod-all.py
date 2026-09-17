#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
lgbs-data-insert-kingsoft-prod-all.py
从 Hive 抽取数据，批量写入金山多维表格（创建记录）。

说明：
- 兼容 Python 2.7（服务器默认 python=2.7 的场景）
- Hive 连接参数从配置文件读取，方式与 lgbs-table-structure-move-department-prod.py 一致（默认 /opt/lgbs/user.txt，可用环境变量 HIVE_USER_CONFIG_FILE 覆盖）。若运维在 ``user.txt`` 的 Hive 键值段之后追加完整 ``krb5.conf``（以 ``[kdcdefaults]`` / ``[libdefaults]`` 等 ``[节名]`` 行起头），解析器在首节处停止扁平 ``key=value`` 映射，避免 ``default_realm`` / ``kdc`` 等污染连接配置；可选将嵌入式内容落盘并设置 ``KRB5_CONFIG``（见 ``HIVE_USER_TXT_EMBED_KRB5``）。
- **Hive 拉数（仅 PyHive / HS2）**：通过 Thrift 直连 HiveServer2，``DESCRIBE`` 取列名，``cursor.fetchmany`` 分批拉 ``HIVE_SQL``；不再使用 beeline 子进程与 stdout/csv 解析。依赖 ``pip install pyhive thrift``（Kerberos 常加 ``thrift_sasl``、系统 ``libsasl2``）。连接参数与 ``HADOOP_ENV_SH``、``HIVE_KINIT_*``、``user.txt`` 等与历史脚本一致。可选 ``HIVE_BEELINE_CONNECT_DB`` 作为 HS2 连接库名（默认 ``default``，仅为兼容旧环境变量名）。拉数前默认对当前 ``HIVE_SQL`` 打 ``COUNT(*)`` 日志（``HIVE_BEELINE_LOG_SELECT_COUNT=0`` 可关；变量名沿用旧版）。Kerberos 下 ``HIVE_PYHIVE_TCP_SASL_SPLIT``：``1``/``on`` 强制「TCP 连 IP + SASL 用 FQDN」；``0``/``off`` 仅用标准 ``hive.Connection``；未设置（auto）时**先试标准连接**，若报 ``no serverFQDN`` 再**自动回退**到分离路径。分离路径下默认 ``HIVE_PYHIVE_SPLIT_FETCH_CAP=1``（逐行 ``fetch``，最稳；可调大）；或 ``HIVE_PYHIVE_SPLIT_MICRO_BATCH=1`` 显式逐行。
- 写入前按多维表 schema 字段类型对源值做转换（KINGSOFT_TYPE_COERCION，默认开启）；日期列可用 KINGSOFT_DATETIME_VALUE_MODE=iso|epoch_ms|epoch_s
- **Hive 列名 → 多维表栏位**：括号代码自动映射（如 ``num`` → ``序号(NUM)``）的查找键含 **去库表限定名**（``lgqyqtz_v1.num`` 与 ``num`` 等价）。显式 ``field_mapping_json`` / ``KINGSOFT_FIELD_MAPPING_JSON`` 同样支持限定列名。默认 **``KINGSOFT_STRICT_HIVE_NUM_TO_PAREN_NUM_FIELD=1``**：若存在 Hive 列 ``num``/``*.num``，其单元格值覆盖写入由 ``num`` 映射到的多维表栏位（保证 ``序号(NUM)`` 与库 ``num`` 同源列）。
- 数据语义：每个 Hive **逻辑**行对应 1 条多维表创建记录（循环内对每行只调用一次 ``_row_to_record``）。默认 **边拉边写**（``HIVE_MATERIALIZE_BEFORE_KINGSOFT_INSERT=0``）以降低峰值内存；需先固定 Hive 结果集再映射时可设 ``HIVE_MATERIALIZE_BEFORE_KINGSOFT_INSERT=1`` 全量物化。``HIVE_FETCH_SIZE`` 默认 100，且受 ``HIVE_PYHIVE_FETCH_CAP``（默认 50）上限；PyHive ``fetchmany`` 遇 ``MemoryError`` 会自动减半批次直至 ``fetchone``。``KINGSOFT_RECORDS_BATCH_SIZE`` 只控制单次 ``/records/create`` 合并条数；若需每次 HTTP 只 1 条，设 ``KINGSOFT_ONE_RECORD_PER_HTTP_REQUEST=1``。
"""
import sys
import os
import time
import json
import hashlib
import hmac
import re
import subprocess
import codecs
import tempfile
from email.utils import formatdate
import base64
import datetime
import socket

# Py2 全局编码兜底：减少隐式 str/unicode 转换导致的 ascii 编码异常
if sys.version_info[0] < 3:
    try:
        reload(sys)  # type: ignore[name-defined]
        sys.setdefaultencoding("utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

# 用于确认调度实际执行的脚本版本（避免跑到旧副本）
SCRIPT_BUILD_ID = "2026-05-19+kinit-ld-library-path-from-bigdata-env"

# Hive 默认库名：配置 sheet「上报库名」为空时使用（可用 HIVE_DEFAULT_DATABASE / HIVE_SOURCE_DATABASE / LGBS_HIVE_SOURCE_DB 覆盖）
HIVE_DATABASE_DEFAULT = (
    (os.getenv("HIVE_DEFAULT_DATABASE") or os.getenv("HIVE_SOURCE_DATABASE") or os.getenv("LGBS_HIVE_SOURCE_DB") or "lgbs")
).strip()

# -------------------- 兼容导入：Python2/3 HTTP + URL 编码 --------------------
try:
    import http.client as httplib  # py3
except Exception:
    import httplib  # py2

try:
    from urllib import urlencode, quote  # py2
except Exception:
    from urllib.parse import urlencode, quote  # py3

try:
    text_type = unicode  # type: ignore[name-defined]
except Exception:
    text_type = str


def to_text(val):
    """
    将输入安全转换为文本类型：
    - Py2: 返回 unicode
    - Py3: 返回 str
    避免 Py2 下 str(unicode) 触发默认 ascii 编码异常。
    """
    if val is None:
        return text_type("")
    try:
        # py2: bytes / str
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
            # 不直接 str(val)，避免 Py2 下 str(unicode) 触发 ascii 编码异常
            b = val if isinstance(val, (bytes, bytearray)) else repr(val)
            # 兼容：某些对象 repr 返回 bytes
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
    """
    统一输出：Py2 下写 utf-8 bytes，避免中文在日志里变 ???。
    """
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
    """将输入转为 utf-8 bytes（兼容 Py2 urllib.quote/urlencode）。"""
    if val is None:
        return b""
    try:
        # py2: unicode -> encode
        if isinstance(val, text_type):
            return val.encode("utf-8")
    except Exception:
        pass
    try:
        # py3: str -> encode
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
    """
    URL 编码（兼容 Py2/Py3）：
    - Py2: quote 需要 bytes，unicode 会 KeyError
    - Py3: quote 可接受 str
    """
    if sys.version_info[0] < 3:
        return quote(_to_utf8_bytes(val), safe=safe)
    return quote(to_text(val), safe=safe)


def urlencode_any(pairs, doseq=True):
    """
    urlencode（兼容 Py2/Py3）：
    - Py2: 建议传入 bytes，避免 unicode 编码异常
    """
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


# ==================== 入参解析（参考 kingsoft-data-insert-hive-prod-all.py） ====================
def get_job_variable(value):
    """
    解析作业变量格式 #{variable_name}，从环境变量中获取对应的值。
    如果 value 是作业变量格式（如 #{doc_lib_name}），则从环境变量获取；
    否则直接返回原值。
    """
    if value is None or value == "":
        return None
    # Py2 下命令行参数通常是 bytes，需要先转为 unicode
    v_text = to_text(value)
    match = re.match(r'^#\{(\w+)\}$', v_text)
    if match:
        return os.environ.get(match.group(1))
    return v_text


# ==================== 入参解析（配置驱动） ====================
# 新入参仅 3 个：用于定位「配置 sheet」
# 第1个 doc_lib_name：配置所在文档库名称（支持模糊匹配）
# 第2个 file_name：配置所在文件名称（支持模糊匹配）
# 第3个 sheet_name：配置所在 sheet 名称（支持模糊匹配）
#
# 配置 sheet 中需要包含（按展示名）：
# - 是否迁移标识：值为“是”的记录才会执行
# - 上报库名：Hive 库名（database）；为空时用环境变量 HIVE_DEFAULT_DATABASE / HIVE_SOURCE_DATABASE / LGBS_HIVE_SOURCE_DB 或默认 lgbs
# - 上报表名称：Hive 表名（可写 `库.表` 或仅表名）；也作为目标 sheet 名
# - 文档库名称 / 上报资源名：目标多维表位置
# - 特殊字段转换配置：field_mapping_json（JSON 字符串，可为空）
JOB_CONFIG = {
    "config_doc_lib_name": get_job_variable(sys.argv[1] if len(sys.argv) > 1 else None),
    "config_file_name": get_job_variable(sys.argv[2] if len(sys.argv) > 2 else None),
    "config_sheet_name": get_job_variable(sys.argv[3] if len(sys.argv) > 3 else None),
    "migration_type": get_job_variable(sys.argv[4] if len(sys.argv) > 4 else None),
    # 兼容调度把 "yyyy-MM-dd HH:mm:ss" 按空格拆成两个参数的场景：
    # argv[5]=yyyy-MM-dd, argv[6]=HH:mm:ss
    "migration_time": get_job_variable(
        (
            "{0} {1}".format(sys.argv[5], sys.argv[6])
            if len(sys.argv) > 6 and re.match(r"^\d{4}-\d{2}-\d{2}$", to_text(sys.argv[5] or "").strip()) and re.match(r"^\d{2}:\d{2}:\d{2}$", to_text(sys.argv[6] or "").strip())
            else (sys.argv[5] if len(sys.argv) > 5 else None)
        )
    ),
}


def _normalize_and_validate_migration_args():
    """
    校验并规范化迁移参数：
    - migrationType 仅允许 all/add/partially（默认 all）
    - migrationType=add 或 partially 时，migrationTime 必填且格式必须为 yyyy-MM-dd HH:mm:ss
    返回：(migration_type, migration_time_text)
    """
    migration_type_raw = JOB_CONFIG.get("migration_type")
    migration_time_raw = JOB_CONFIG.get("migration_time")

    migration_type = (to_text(migration_type_raw) if migration_type_raw is not None else "").strip().lower()
    if not migration_type:
        migration_type = "all"
    if migration_type not in ("all", "add", "partially"):
        raise RuntimeError(
            "migrationType 仅支持 all、add 或 partially，当前值={0!r}".format(migration_type_raw)
        )

    migration_time_text = (to_text(migration_time_raw) if migration_time_raw is not None else "").strip()
    # 兼容 ISO 风格时间：yyyy-MM-ddTHH:mm:ss -> yyyy-MM-dd HH:mm:ss
    if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", migration_time_text):
        migration_time_text = migration_time_text.replace("T", " ", 1)
    if migration_type in ("add", "partially"):
        if not migration_time_text:
            raise RuntimeError("migrationType=add 或 partially 时，migrationTime 不能为空，格式应为 yyyy-MM-dd HH:mm:ss")
        try:
            datetime.datetime.strptime(migration_time_text, "%Y-%m-%d %H:%M:%S")
        except Exception:
            raise RuntimeError(
                "migrationTime 格式错误：{0!r}，要求格式 yyyy-MM-dd HH:mm:ss".format(migration_time_raw)
            )
    return migration_type, migration_time_text


def get_field_mapping_from_arg():
    """
    获取字段映射（第6位入参 JSON 字符串）。
    - 入参为空：返回空映射（忽略）
    - 入参有值：必须是合法 JSON 对象，否则报错
    """
    raw_mapping = JOB_CONFIG.get("field_mapping_json")
    if raw_mapping is None or str(raw_mapping).strip() == "":
        return {}

    try:
        parsed = json.loads(str(raw_mapping))
    except Exception as e:
        raise RuntimeError(
            "第6位入参 field_mapping_json 不是合法 JSON 字符串：{0!r}，err={1}".format(raw_mapping, e)
        )

    if not isinstance(parsed, dict):
        raise RuntimeError(
            "第6位入参 field_mapping_json 必须是 JSON 对象（key/value），当前类型={0}".format(
                type(parsed).__name__
            )
        )

    def _trim_mapping_token(val):
        if val is None:
            return ""
        return re.sub(r"^[\s\u3000]+|[\s\u3000]+$", "", str(val))

    normalized_mapping = {}
    for k, v in parsed.items():
        if k is None or v is None:
            continue
        src_name = _trim_mapping_token(k)
        dst_name = _trim_mapping_token(v)
        if not src_name or not dst_name:
            continue

        # 映射方向：Oracle列名/别名 -> 多维表字段名/字段ID（取决于 prefer_id）
        normalized_mapping[src_name] = dst_name
        normalized_mapping[src_name.lower()] = dst_name
        normalized_src_name = re.sub(r"[\s\u3000]+", "", src_name)
        if normalized_src_name:
            normalized_mapping[normalized_src_name] = dst_name
            normalized_mapping[normalized_src_name.lower()] = dst_name

    if not normalized_mapping:
        raise RuntimeError(
            "第6位入参 field_mapping_json 解析后为空，请检查入参：{0!r}".format(raw_mapping)
        )

    return normalized_mapping


# ==================== Hive 连接配置（与 lgbs-table-structure-move-department-prod.py 一致） ====================
HIVE_USER_CONFIG_FILE = "/opt/lgbs/user.txt"
_LAST_HIVE_CFG_FILE = None
_HIVE_CFG_LOGGED = False
_HIVE_KRB_REALM_FALLBACK_LOGGED = False
_HIVE_EMBEDDED_KRB5_FROM_USER_TXT_LOGGED = False

# 每条作业执行前动态设置 HIVE_SQL；也可通过环境变量 HIVE_SQL 强制指定整段 SQL
HIVE_SQL = (os.getenv("HIVE_SQL", "") or "").strip()

# 最近一次 PyHive COUNT(*) 成功得到的行数（与 fetch 行数对照日志）
_LAST_HIVE_REF_ROW_COUNT = None

# 最近一次尝试 import pyhive 失败原因（供日志 / RuntimeError）
_LAST_PYHIVE_IMPORT_ERROR = None

# 每批从 Hive 拉取多少行（兼容旧环境变量 ORACLE_FETCH_SIZE；默认 100，避免 PyHive zip(*columns) OOM）
HIVE_FETCH_SIZE = int(os.getenv("HIVE_FETCH_SIZE", os.getenv("ORACLE_FETCH_SIZE", "100")))


def _hive_effective_fetch_size():
    """
    实际 ``fetchmany`` 批量：``HIVE_FETCH_SIZE`` 与全局 ``HIVE_PYHIVE_FETCH_CAP``（默认 50）取较小值；
    若当前连接走 TCP/SASL 分离传输（``_HIVE_PYHIVE_TRANSPORT_SPLIT=1``），再与 ``HIVE_PYHIVE_SPLIT_FETCH_CAP``
    （默认 **1**，逐行）取较小值。``HIVE_PYHIVE_SPLIT_MICRO_BATCH=1`` 显式强制每批 1 行。
    """
    try:
        base = int(os.getenv("HIVE_FETCH_SIZE", os.getenv("ORACLE_FETCH_SIZE", "100")))
    except Exception:
        base = 100
    base = max(1, base)
    try:
        global_cap = int(os.getenv("HIVE_PYHIVE_FETCH_CAP", "50"))
    except Exception:
        global_cap = 50
    global_cap = max(1, global_cap)
    size = min(base, global_cap)
    if (os.getenv("_HIVE_PYHIVE_TRANSPORT_SPLIT", "") or "").strip() != "1":
        return size
    if (os.getenv("HIVE_PYHIVE_SPLIT_MICRO_BATCH", "") or "").strip().lower() in ("1", "true", "yes", "on"):
        return 1
    try:
        cap = int(os.getenv("HIVE_PYHIVE_SPLIT_FETCH_CAP", "1"))
    except Exception:
        cap = 1
    cap = max(1, cap)
    return min(size, cap)


def _pyhive_cursor_fetch_raw_batches(cur, fsz_init):
    """
    从 PyHive cursor 分批取原始行（list of tuple）。
    ``fsz_init<=1`` 或 ``HIVE_PYHIVE_USE_FETCHONE=1`` 时用 ``fetchone``；
    ``fetchmany`` 遇 ``MemoryError`` 时自动减半批次直至逐行。
    """
    try:
        fsz = max(1, int(fsz_init))
    except Exception:
        fsz = 1
    use_one = fsz <= 1 or (os.getenv("HIVE_PYHIVE_USE_FETCHONE", "") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    mem_retries = 0
    while True:
        try:
            if use_one:
                row = cur.fetchone()
                if row is None:
                    break
                yield [row]
            else:
                batch = cur.fetchmany(fsz)
                if not batch:
                    break
                yield list(batch)
        except MemoryError:
            mem_retries += 1
            if use_one and fsz <= 1:
                raise RuntimeError(
                    u"PyHive 逐行 fetch 仍 MemoryError：单行过宽或进程内存不足。"
                    u"可缩小 HIVE_SQL（避免 SELECT *）、增大容器内存，或检查超大字段列。"
                )
            if fsz <= 1:
                use_one = True
                fsz = 1
            else:
                fsz = max(1, fsz // 2)
                if fsz <= 1:
                    use_one = True
            try:
                cur.arraysize = max(1, int(fsz))
            except Exception:
                pass
            try:
                _print_u(
                    u"【{0}】PyHive fetch MemoryError，降为 batch={1}（fetchone={2}）后重试（第 {3} 次）".format(
                        to_text(_now()),
                        fsz,
                        u"1" if use_one else u"0",
                        mem_retries,
                    )
                )
            except Exception:
                pass
            if mem_retries > 12:
                raise RuntimeError(
                    u"PyHive fetch 多次 MemoryError 后仍失败；请设 HIVE_FETCH_SIZE=10、"
                    u"HIVE_PYHIVE_FETCH_CAP=1、HIVE_MATERIALIZE_BEFORE_KINGSOFT_INSERT=0 或缩小 SQL 结果集。"
                )
            continue

# 增量/分段迁移时用于比较的列名（Hive 表须含该列）
HIVE_INCREMENT_TIME_COLUMN = (os.getenv("HIVE_INCREMENT_TIME_COLUMN", "TYKY_SJCP_UPDATE_TIME") or "TYKY_SJCP_UPDATE_TIME").strip()


def _hive_user_txt_line_is_ini_section_header(ln):
    """识别 krb5.conf / INI 节标题行，如 ``[libdefaults]``（Hive 扁平段到此为止）。"""
    s = to_text(ln or u"").strip()
    return bool(re.match(r"^\[[^\]]+\]$", s))


def _hive_user_txt_line_is_hex_noise_line(ln):
    """
    user.txt 尾部误粘贴的二进制/十六进制转储行（无 ``=``、无 ``#``），解析 krb5 嵌入段时丢弃。
    """
    s = to_text(ln or u"").strip()
    if len(s) < 8:
        return False
    if u"=" in s or s.startswith(u"#") or s.startswith(u";") or s.startswith(u"["):
        return False
    parts = s.split()
    if not parts:
        return False
    for p in parts:
        if not re.match(r"^[0-9a-fA-F]+$", to_text(p)):
            return False
    return True


def _hive_user_txt_strip_trailing_hex_noise(lines):
    out = [to_text(x) for x in (lines or [])]
    while out and _hive_user_txt_line_is_hex_noise_line(out[-1]):
        out.pop()
    while out and not out[-1].strip():
        out.pop()
    return out


def _hive_apply_embedded_krb5_from_user_txt(krb5_lines):
    """
    将 user.txt 内嵌的 krb5 正文落盘并设置 ``KRB5_CONFIG``（供 kinit / GSSAPI 使用）。

    - ``HIVE_USER_TXT_EMBED_KRB5=auto``（默认）：存在嵌入式段且进程内 ``KRB5_CONFIG`` 未设置时写入并设置。
    - ``force``：覆盖已有 ``KRB5_CONFIG``。
    - ``0`` / ``false``：不写文件、不改环境变量。
    """
    global _HIVE_EMBEDDED_KRB5_FROM_USER_TXT_LOGGED
    mode = (os.getenv("HIVE_USER_TXT_EMBED_KRB5", "auto") or "auto").strip().lower()
    if mode in ("0", "false", "no", "off"):
        return
    cleaned = _hive_user_txt_strip_trailing_hex_noise(krb5_lines)
    if not cleaned:
        return
    body = u"\n".join(cleaned).rstrip() + u"\n"
    try:
        body_b = body.encode("utf-8")
    except Exception:
        body_b = body if isinstance(body, bytes) else str(body)

    base_dir = os.path.join(tempfile.gettempdir() or u"/tmp", u"lgbs")
    try:
        if not os.path.isdir(base_dir):
            os.makedirs(base_dir)
    except Exception:
        base_dir = tempfile.gettempdir() or u"/tmp"
    out_path = os.path.join(base_dir, u"krb5_embedded_from_user_txt.conf")

    cur = (os.getenv("KRB5_CONFIG") or "").strip()
    if cur and mode != "force":
        if not _HIVE_EMBEDDED_KRB5_FROM_USER_TXT_LOGGED:
            _HIVE_EMBEDDED_KRB5_FROM_USER_TXT_LOGGED = True
            try:
                _print_u(
                    u"【{0}】user.txt 内含 krb5 嵌入段，但已存在 KRB5_CONFIG={1}，未覆盖（设 HIVE_USER_TXT_EMBED_KRB5=force 可强制使用嵌入配置）".format(
                        to_text(time.strftime("%Y-%m-%d %H:%M:%S")),
                        to_text(cur),
                    )
                )
            except Exception:
                pass
        return

    try:
        with open(out_path, "wb") as wf:
            wf.write(body_b)
    except Exception as e:
        raise RuntimeError(u"写入 user.txt 嵌入 krb5 失败：path={0} err={1}".format(to_text(out_path), e))

    try:
        _p = to_text(out_path)
        if sys.version_info[0] < 3:
            os.environ["KRB5_CONFIG"] = _p.encode("utf-8")
        else:
            os.environ["KRB5_CONFIG"] = _p
    except Exception:
        os.environ["KRB5_CONFIG"] = str(out_path)
    if not _HIVE_EMBEDDED_KRB5_FROM_USER_TXT_LOGGED:
        _HIVE_EMBEDDED_KRB5_FROM_USER_TXT_LOGGED = True
        try:
            _print_u(
                u"【{0}】user.txt 内嵌 krb5 已落盘并设置 KRB5_CONFIG={1}（HIVE_USER_TXT_EMBED_KRB5={2}）".format(
                    to_text(time.strftime("%Y-%m-%d %H:%M:%S")),
                    to_text(out_path),
                    to_text(mode or u"auto"),
                )
            )
        except Exception:
            pass


def read_hive_user_config(file_path=None):
    """
    从 user.txt 读取 Hive 连接参数（key=value，UTF-8）。
    Windows 或无 /tmp 时可通过环境变量 HIVE_USER_CONFIG_FILE 覆盖路径。

    若文件在 Hive 键值段之后追加完整 ``krb5.conf``（以 ``[libdefaults]`` 等 ``[节名]`` 行开始），则：
    - 首节之前的行仍按扁平 ``key=value`` 解析（与旧版一致）；
    - 首节及之后不再写入 Hive cfg，避免 ``default_realm`` / ``kdc`` 等覆盖 ``host`` 等键；
    - 可选将嵌入段落盘并设置 ``KRB5_CONFIG``（见 ``_hive_apply_embedded_krb5_from_user_txt``）。
    """
    cfg = {}
    fp = file_path or os.getenv("HIVE_USER_CONFIG_FILE", HIVE_USER_CONFIG_FILE) or HIVE_USER_CONFIG_FILE
    global _LAST_HIVE_CFG_FILE
    _LAST_HIVE_CFG_FILE = fp
    krb_embed = []
    in_embed = False
    f = None
    try:
        if sys.version_info[0] < 3:
            f = codecs.open(fp, "r", encoding="utf-8")
        else:
            f = open(fp, "r", encoding="utf-8")
        for line in f:
            raw = line or u""
            ln = raw.strip()
            if in_embed:
                krb_embed.append(raw.rstrip(u"\r\n"))
                continue
            if not ln or ln.startswith(u"#"):
                continue
            if _hive_user_txt_line_is_ini_section_header(ln):
                in_embed = True
                krb_embed.append(raw.rstrip(u"\r\n"))
                continue
            if u"=" not in ln:
                continue
            k, v = ln.split(u"=", 1)
            kk = k.strip()
            try:
                if kk and isinstance(kk, text_type) and kk[0] == u"\ufeff":
                    kk = kk.lstrip(u"\ufeff")
            except Exception:
                pass
            try:
                kk2 = to_text(kk).strip().lower()
            except Exception:
                kk2 = str(kk).strip().lower()
            cfg[kk2] = v.strip() if hasattr(v, "strip") else to_text(v).strip()
    except IOError as e:
        raise RuntimeError("Hive 配置文件不存在或不可读：{0}，err={1}".format(fp, e))
    except Exception as e:
        raise RuntimeError("读取 Hive 配置文件失败：{0} err={1}".format(fp, e))
    finally:
        try:
            if f:
                f.close()
        except Exception:
            pass

    if krb_embed:
        _hive_apply_embedded_krb5_from_user_txt(krb_embed)
    return cfg


_HIVE_KRB_PLACEHOLDER_REALMS = frozenset(
    ("EXAMPLE.COM", "EXAMPLE.ORG", "INVALID.REALM", "TEST.REALM", "MYREALM.ORG")
)


def _hive_host_looks_like_ipv4(h):
    return bool(re.match(r"^(?:\d{1,3}\.){3}\d{1,3}$", to_text(h or u"").strip()))


def _hive_kerberos_realm_is_placeholder(realm):
    t = to_text(realm or u"").strip().upper()
    return bool(t and t in _HIVE_KRB_PLACEHOLDER_REALMS)


def _hive_parse_jdbc_principal_realm(jdbc_url):
    s = to_text(jdbc_url or u"")
    m = re.search(r"(?i)\bprincipal\s*=\s*[^/;=]+/[^@;/\s]+@([A-Za-z0-9._-]+)", s)
    if not m:
        return ""
    return m.group(1).strip()


def _hive_read_default_realm_from_krb5_conf():
    """
    从 krb5 配置读取 ``default_realm``。优先 ``KRB5_CONFIG``（含 user.txt 嵌入段落盘路径），其次 ``/etc/krb5.conf``。
    """
    paths = []
    k5 = (os.getenv("KRB5_CONFIG") or "").strip()
    if k5:
        paths.append(k5)
    paths.append("/etc/krb5.conf")
    seen = set()
    for p in paths:
        if not p or p in seen:
            continue
        seen.add(p)
        if not os.path.isfile(p):
            continue
        try:
            if sys.version_info[0] < 3:
                _f = codecs.open(p, "r", encoding="utf-8", errors="replace")
            else:
                _f = open(p, "r", encoding="utf-8", errors="replace")
            try:
                for _ln in _f:
                    _ln2 = _ln.strip()
                    if not _ln2 or _ln2.startswith("#") or _ln2.startswith(";"):
                        continue
                    if "default_realm" in _ln2 and "=" in _ln2:
                        _k, _v = _ln2.split("=", 1)
                        if _k.strip().lower() == "default_realm":
                            return _v.strip()
            finally:
                try:
                    _f.close()
                except Exception:
                    pass
        except Exception:
            continue
    return ""


def _hive_guess_kerberos_realm_from_krbhost(krbhost):
    """
    从 krbhost FQDN 推断 realm：如 ``<INTERNAL_HIVE_HOSTNAME>`` -> ``LGXC_HADOOP.COM``。
    首段为常见服务名（hive/hadoop/nn 等）且段数≥3 时去掉首段再拼域名。
    """
    h = to_text(krbhost or u"").strip()
    if not h or _hive_host_looks_like_ipv4(h) or "." not in h:
        return ""
    parts = h.lower().split(".")
    if len(parts) < 2:
        return ""
    service_first = frozenset(
        (
            "hadoop",
            "hive",
            "hs2",
            "hs",
            "nn",
            "jn",
            "dn",
            "rm",
            "nm",
            "master",
            "worker",
            "jhs",
        )
    )
    if len(parts) >= 3 and parts[0] in service_first:
        dom = ".".join(parts[1:])
    else:
        dom = ".".join(parts)
    return dom.upper()


def _hive_resolve_kerberos_realm_for_cfg(hive_cfg):
    """
    供 JDBC ``principal=...`` 与 ``kinit`` 主体后缀一致。
    忽略占位 realm（如 EXAMPLE.COM，见 ``HIVE_KRB_ALLOW_PLACEHOLDER_REALM=1`` 可强制使用）。
    占位或未配置时可用 ``HIVE_KRB_REALM_GUESS_FROM_KRBHOST``（默认 auto：在 krbhost 为 FQDN 时推断）。
    """
    global _HIVE_KRB_REALM_FALLBACK_LOGGED
    allow_ph = (os.getenv("HIVE_KRB_ALLOW_PLACEHOLDER_REALM", "") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    guess_mode = (os.getenv("HIVE_KRB_REALM_GUESS_FROM_KRBHOST", "auto") or "auto").strip().lower()

    def _usable(r):
        t = to_text(r or u"").strip()
        if not t:
            return ""
        if _hive_kerberos_realm_is_placeholder(t) and not allow_ph:
            return ""
        return t

    for _, _raw in (
        ("HIVE_KRB_REALM", (os.getenv("HIVE_KRB_REALM", "") or "").strip()),
        ("jdbc_principal", _hive_parse_jdbc_principal_realm((hive_cfg or {}).get("jdbc_url") or "")),
        ("krb5_default_realm", _hive_read_default_realm_from_krb5_conf()),
    ):
        u = _usable(_raw)
        if u:
            return u

    krbhost = ""
    try:
        krbhost = to_text((hive_cfg or {}).get("krbhost") or u"").strip()
    except Exception:
        krbhost = ""
    if guess_mode in ("0", "false", "no", "off"):
        return ""
    g = _usable(_hive_guess_kerberos_realm_from_krbhost(krbhost))
    if g and not _HIVE_KRB_REALM_FALLBACK_LOGGED:
        try:
            _print_u(
                u"【{0}】Kerberos：未采用占位/无效 realm，已从 krbhost={1} 推断 realm={2}（可设 HIVE_KRB_REALM 显式覆盖）".format(
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    krbhost,
                    g,
                )
            )
        except Exception:
            pass
        _HIVE_KRB_REALM_FALLBACK_LOGGED = True
    return g


def get_hive_config(database):
    """构建 Hive 连接参数：host/port/username/auth 等来自 user.txt，与部门结构迁移脚本一致。"""
    cfg = read_hive_user_config()
    global _HIVE_CFG_LOGGED
    if not _HIVE_CFG_LOGGED:
        _HIVE_CFG_LOGGED = True
        try:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            _print_u(
                u"【{0}】Hive 配置加载：file={1}，keys={2}，jdbc_url_set={3}".format(
                    ts,
                    to_text(_LAST_HIVE_CFG_FILE),
                    u",".join(sorted([to_text(k) for k in (cfg or {}).keys() if k])),
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
    # user.txt 中 ``auth=`` 空行会存成 ""，cfg.get("auth","KERBEROS") 仍得到 ""，导致误当作非 Kerberos
    _ar = cfg.get("auth")
    if _ar is None or not to_text(_ar).strip():
        _auth_m = u"KERBEROS"
    else:
        _auth_m = to_text(_ar).strip()
    return {
        "host": host,
        "port": port,
        "username": cfg.get("username", ""),
        "database": database or cfg.get("database", ""),
        "auth": _auth_m,
        "kerberos_service_name": cfg.get("kerberos_service_name", "hive"),
        "krbhost": cfg.get("krbhost", ""),
        "jdbc_url": cfg.get("jdbc_url", "") or "",
        # PyHive 与 beeline 同源 kinit：可在 user.txt 配置，避免仅子 shell 有票（见 kinit_keytab）
        "kinit_keytab": to_text(
            cfg.get("kinit_keytab", "") or cfg.get("keytab", "") or cfg.get("kerberos_keytab", "") or ""
        ).strip(),
        "kinit_user": to_text(cfg.get("kinit_user", "") or "").strip(),
    }


def _safe_hive_ident(name):
    """Hive 标识符保守规范化（与部门结构迁移脚本一致）。"""
    t = to_text(name).strip()
    if not t:
        return ""
    t2 = re.sub(r"[^0-9a-zA-Z_]", "_", t)
    if re.match(r"^\d", t2):
        t2 = "_" + t2
    return t2.lower()


def _hive_full_table_qualified(default_db, table_spec):
    """
    生成 `` `db`.`table` ``。table_spec 可为 `表名` 或 `库.表`；单表名时挂 default_db。
    """
    db0 = _safe_hive_ident(default_db) or "default"
    ts = to_text(table_spec).strip()
    if not ts:
        raise RuntimeError(u"上报表名称（Hive 表）为空")
    if "." in ts:
        parts = [p for p in ts.split(".") if p]
        if len(parts) >= 2:
            return u"`{0}`.`{1}`".format(_safe_hive_ident(parts[0]), _safe_hive_ident(parts[1]))
    return u"`{0}`.`{1}`".format(db0, _safe_hive_ident(ts))


def _shell_capture(cmd, env=None):
    """在 bash 下执行命令并捕获 stdout/stderr（与部门结构迁移脚本一致）。"""
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
    """与 lgbs-table-structure-move-department-prod.py 一致。"""
    jdbc_url = (hive_cfg.get("jdbc_url") or "").strip()
    if jdbc_url:
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

    auth = (hive_cfg.get("auth") or "").upper()
    if auth == "KERBEROS":
        realm = _hive_resolve_kerberos_realm_for_cfg(hive_cfg)
        ksn = (hive_cfg.get("kerberos_service_name") or "hive").strip()
        krbhost = (hive_cfg.get("krbhost") or "").strip()
        if realm and krbhost:
            return base + ";principal={0}/{1}@{2}".format(ksn, krbhost, realm)

    return base


def _hive_resolve_kerberos_client_bin_dir():
    """
    华为 MRS 等环境 ``kinit`` 常在 ``/opt/hadoopclient/KrbClient/kerberos/bin``，
    跳过 ``source bigdata_env`` 时须显式加入 PATH 或使用绝对路径。
    """
    explicit = (os.getenv("HIVE_KERBEROS_BIN_DIR", "") or os.getenv("KRB5_BIN_DIR", "") or "").strip()
    if explicit and os.path.isdir(explicit):
        return explicit.rstrip("/")
    kinit_bin = (os.getenv("HIVE_KINIT_BIN", "") or os.getenv("KINIT_BIN", "") or "").strip()
    if kinit_bin and os.path.isfile(kinit_bin):
        try:
            return os.path.dirname(os.path.abspath(kinit_bin))
        except Exception:
            pass
    krb5_home = (os.getenv("KRB5_HOME", "") or "").strip()
    if krb5_home:
        for sub in ("bin", os.path.join("kerberos", "bin")):
            try:
                d = os.path.join(krb5_home, sub)
                if os.path.isfile(os.path.join(d, "kinit")):
                    return d
            except Exception:
                pass
    for d in (
        "/opt/hadoopclient/KrbClient/kerberos/bin",
        "/opt/hadoopclient/Kerberos/kerberos/bin",
    ):
        try:
            if os.path.isfile(os.path.join(d, "kinit")):
                return d
        except Exception:
            pass
    return ""


def _hive_resolve_kinit_executable():
    explicit = (os.getenv("HIVE_KINIT_BIN", "") or os.getenv("KINIT_BIN", "") or "").strip()
    if explicit:
        return explicit
    bdir = _hive_resolve_kerberos_client_bin_dir()
    if bdir:
        p = os.path.join(bdir, "kinit")
        if os.path.isfile(p):
            return p
    try:
        import shutil  # py3

        w = shutil.which("kinit")
        if w:
            return w
    except Exception:
        pass
    return "kinit"


def _hive_resolve_klist_executable():
    explicit = (os.getenv("HIVE_KLIST_BIN", "") or os.getenv("KLIST_BIN", "") or "").strip()
    if explicit:
        return explicit
    bdir = _hive_resolve_kerberos_client_bin_dir()
    if bdir:
        p = os.path.join(bdir, "klist")
        if os.path.isfile(p):
            return p
    kinit_p = _hive_resolve_kinit_executable()
    if kinit_p and kinit_p != "kinit":
        try:
            kd = os.path.dirname(kinit_p)
            p2 = os.path.join(kd, "klist")
            if os.path.isfile(p2):
                return p2
        except Exception:
            pass
    return "klist"


def _hive_kerberos_ld_library_path_candidates():
    """kinit 依赖的 Kerberos 动态库目录（未 source bigdata_env 时需显式设置 LD_LIBRARY_PATH）。"""
    out = []
    seen = set()
    for p in (
        (os.getenv("LD_LIBRARY_PATH", "") or "").strip(),
    ):
        for seg in p.split(":"):
            seg = seg.strip()
            if seg and seg not in seen:
                seen.add(seg)
                out.append(seg)
    bdir = _hive_resolve_kerberos_client_bin_dir()
    if bdir:
        for rel in ("../lib", "lib", "../lib64", "lib64"):
            try:
                d = os.path.normpath(os.path.join(bdir, rel))
                if d and os.path.isdir(d) and d not in seen:
                    seen.add(d)
                    out.append(d)
            except Exception:
                pass
    for d in (
        "/opt/hadoopclient/KrbClient/kerberos/lib",
        "/opt/hadoopclient/KrbClient/kerberos/lib64",
    ):
        try:
            if os.path.isdir(d) and d not in seen:
                seen.add(d)
                out.append(d)
        except Exception:
            pass
    return out


def _hive_bash_export_kerberos_runtime_env():
    """
    子 shell 未 source bigdata_env 时，补齐 PATH 与 LD_LIBRARY_PATH（kinit 需 libkadm5srv_mit 等）。
    父进程若已通过 ``_hive_merge_whitelisted_krb_env_from_hadoop_sh`` 合并，则优先用其值。
    """
    parts = []
    bdir = _hive_resolve_kerberos_client_bin_dir()
    if bdir:
        safe = bdir.replace("'", "'\\''")
        parts.append("export PATH='{0}':\"$PATH\"".format(safe))
    ld = _hive_kerberos_ld_library_path_candidates()
    if ld:
        safe_ld = ":".join(ld).replace("'", "'\\''")
        parts.append("export LD_LIBRARY_PATH='{0}'${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}".format(safe_ld))
    return " && ".join(parts)


def _hive_kinit_principal_string(hive_cfg):
    """
    构造 kinit 用的主体字符串（必要时补 realm），与 beeline/PyHive 前序 kinit 一致。
    """
    k_user = (os.getenv("HIVE_KINIT_USER", "") or os.getenv("KINIT_USER", "") or "").strip()
    if not k_user:
        try:
            k_user = to_text(hive_cfg.get("kinit_user") or "").strip()
        except Exception:
            k_user = ""
    if not k_user:
        try:
            k_user = (hive_cfg.get("username") or "").strip()
        except Exception:
            k_user = ""
    try:
        auth_u = to_text(hive_cfg.get("auth") or "").strip().upper()
        if k_user and "@" not in k_user and auth_u == "KERBEROS":
            realm = _hive_resolve_kerberos_realm_for_cfg(hive_cfg)
            if realm:
                k_user = "{0}@{1}".format(k_user, realm)
    except Exception:
        pass
    return k_user


def _hive_shell_fragment_kinit_if_keytab_files(principal):
    """
    供 ``source HADOOP_ENV_SH`` **之后** 拼入 bash：若 shell 内存在可读 keytab 路径则 ``kinit -kt``，否则 ``true``。
    避免无 keytab 时执行 ``kinit -k`` 误用系统默认 ``/etc/krb5.keytab``（常见于容器/作业机）。
    """
    if not principal:
        return "true"
    p = to_text(principal).replace("'", "'\\''")
    return (
        "_hipr='"
        + p
        + "' && "
        "if [ -n \"${KRB5_CLIENT_KTNAME:-}\" ] && [ -f \"${KRB5_CLIENT_KTNAME}\" ]; then kinit -kt \"${KRB5_CLIENT_KTNAME}\" \"${_hipr}\"; "
        "elif [ -n \"${KRB5_KTNAME:-}\" ] && [ -f \"${KRB5_KTNAME}\" ]; then kinit -kt \"${KRB5_KTNAME}\" \"${_hipr}\"; "
        "elif [ -n \"${HADOOP_USER_KEYTAB:-}\" ] && [ -f \"${HADOOP_USER_KEYTAB}\" ]; then kinit -kt \"${HADOOP_USER_KEYTAB}\" \"${_hipr}\"; "
        "elif [ -n \"${MAPREDUCE_USER_KEYTAB:-}\" ] && [ -f \"${MAPREDUCE_USER_KEYTAB}\" ]; then kinit -kt \"${MAPREDUCE_USER_KEYTAB}\" \"${_hipr}\"; "
        "elif [ -n \"${YARN_USER_KEYTAB:-}\" ] && [ -f \"${YARN_USER_KEYTAB}\" ]; then kinit -kt \"${YARN_USER_KEYTAB}\" \"${_hipr}\"; "
        "elif [ -n \"${SPARK_USER_KEYTAB:-}\" ] && [ -f \"${SPARK_USER_KEYTAB}\" ]; then kinit -kt \"${SPARK_USER_KEYTAB}\" \"${_hipr}\"; "
        "else true; fi"
    )


def _hive_build_kinit_bash_command(hive_cfg):
    """
    与 beeline 子 shell 一致的 ``kinit`` 单行（不含 ``source HADOOP_ENV_SH``）。
    优先 ``KINIT_CMD``；否则 ``HIVE_KINIT_KEYTAB``/``KINIT_KEYTAB`` + ``HIVE_KINIT_USER``；
    亦可在 ``user.txt`` 中配置 ``kinit_keytab=``（或 ``keytab=``）、可选 ``kinit_user=``（缺省用 ``username=``）。

    无 keytab/密码时**不**再执行 ``kinit -k``（易误用不存在的 ``/etc/krb5.keytab``）；请在 ``source`` 后由
    ``_hive_shell_fragment_kinit_if_keytab_files`` 按环境变量中的 keytab 路径条件 kinit，或在 ``user.txt`` 配置 ``kinit_keytab=``。
    """
    kinit_cmd = (os.getenv("KINIT_CMD", "") or "").strip()
    if kinit_cmd:
        return kinit_cmd
    principal = _hive_kinit_principal_string(hive_cfg)
    k_pwd = (os.getenv("HIVE_KINIT_PASSWORD", "") or os.getenv("KINIT_PASSWORD", "") or "").strip()
    k_keytab = (os.getenv("HIVE_KINIT_KEYTAB", "") or os.getenv("KINIT_KEYTAB", "") or "").strip()
    if not k_keytab:
        k_keytab = (os.getenv("KRB5_CLIENT_KTNAME", "") or os.getenv("KRB5_KTNAME", "") or "").strip()
    if not k_keytab:
        try:
            k_keytab = to_text(
                hive_cfg.get("kinit_keytab") or hive_cfg.get("keytab") or hive_cfg.get("kerberos_keytab") or ""
            ).strip()
        except Exception:
            k_keytab = ""
    if not k_keytab:
        for _cand in (
            "KRB5_CLIENT_KTNAME",
            "KRB5_KTNAME",
            "HADOOP_USER_KEYTAB",
            "MAPREDUCE_USER_KEYTAB",
            "YARN_USER_KEYTAB",
            "SPARK_USER_KEYTAB",
        ):
            try:
                _kt = (os.getenv(_cand, "") or "").strip()
            except Exception:
                _kt = ""
            if _kt:
                k_keytab = _kt
                break
    kinit_ex = _hive_resolve_kinit_executable()
    if principal and k_keytab:
        return "{0} -kt '{1}' '{2}'".format(
            kinit_ex.replace("'", "'\\''"),
            k_keytab.replace("'", "'\\''"),
            principal.replace("'", "'\\''"),
        )
    if principal and k_pwd:
        return "echo '{0}' | {1} '{2}'".format(
            k_pwd.replace("'", "'\\''"),
            kinit_ex.replace("'", "'\\''"),
            principal.replace("'", "'\\''"),
        )
    return ""


def _hive_merge_whitelisted_krb_env_from_hadoop_sh():
    """
    ``source HADOOP_ENV_SH`` 仅在 beeline 子 shell 生效时，父进程 Python 无 ``KRB5_CLIENT_KTNAME`` 等。
    从 ``HADOOP_ENV_SH`` 执行后的 ``env`` 中合并变量到 ``os.environ``，供 ``kinit``/GSSAPI 使用。

    - ``KRB5_CLIENT_KTNAME`` / ``KRB5_KTNAME``：若 bigdata_env 里为 **非空路径**，**始终覆盖** 父进程中的空/旧值（常见父进程曾 export 空串导致旧逻辑合并不进来）。
    - 其它 ``KRB5*`` / ``KINIT*``：仅当父进程对应键为空时写入。
    - 名称含 ``KEYTAB`` 的大写标识符（如 ``HADOOP_USER_KEYTAB``）：同上，仅父进程为空时写入。
    - ``LD_LIBRARY_PATH`` / ``PATH``：从 bigdata_env 合并（``LD_LIBRARY_PATH`` 覆盖），供未 source 的 kinit 子进程加载 ``libkadm5srv_mit`` 等。
    不覆盖已由本脚本设置的 ``KRB5CCNAME``（PyHive 专用 FILE ccache）。
    ``HIVE_PYHIVE_IMPORT_KRB_ENV_FROM_HADOOP_SH=0`` 关闭整段。
    """
    if os.environ.get("_HIVE_MERGED_KRB_ENV_FROM_SH") == "1":
        return
    hadoop_env_sh = (os.getenv("HADOOP_ENV_SH", "") or "/opt/hadoopclient/bigdata_env").strip()
    if not hadoop_env_sh or not os.path.isfile(hadoop_env_sh):
        os.environ["_HIVE_MERGED_KRB_ENV_FROM_SH"] = "1"
        return
    safe = hadoop_env_sh.replace("'", "'\\''")
    try:
        out = subprocess.check_output(
            ["bash", "-lc", "set -a; . '{0}' 2>/dev/null; set +a; env".format(safe)],
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            timeout=90,
        )
    except Exception:
        os.environ["_HIVE_MERGED_KRB_ENV_FROM_SH"] = "1"
        return
    merged = 0
    merged_keys = []
    skip_cc = bool((os.environ.get("KRB5CCNAME") or "").strip())
    for line in (out or "").splitlines():
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.rstrip("\r\n")
        if not k:
            continue
        if k == "KRB5CCNAME" and skip_cc:
            continue
        vs = (v or "").strip()
        if not vs:
            continue
        take = False
        force = False
        if k in ("KRB5_CLIENT_KTNAME", "KRB5_KTNAME"):
            take = True
            force = True
        elif k.startswith("KRB5") or k.startswith("KINIT"):
            take = True
        elif "KEYTAB" in k.upper() and re.match(r"^[A-Za-z0-9_]+$", k):
            take = True
        elif k == "LD_LIBRARY_PATH":
            take = True
            force = True
        elif k == "PATH":
            take = True
        if not take:
            continue
        if k == "KRB5CCNAME":
            continue
        try:
            if k == "PATH":
                cur = (os.environ.get("PATH") or "").strip()
                if force or not cur:
                    os.environ["PATH"] = vs
                elif vs and vs not in cur:
                    os.environ["PATH"] = vs + os.pathsep + cur
                merged += 1
                merged_keys.append(k)
            elif force or not (os.environ.get(k) or "").strip():
                os.environ[k] = vs
                merged += 1
                merged_keys.append(k)
        except Exception:
            pass
    os.environ["_HIVE_MERGED_KRB_ENV_FROM_SH"] = "1"
    if merged:
        try:
            _mk = u",".join([to_text(x) for x in merged_keys[:12]])
            if len(merged_keys) > 12:
                _mk += u",..."
            _print_u(
                u"【{0}】PyHive Kerberos：已从 HADOOP_ENV_SH 合并 {1} 个变量：{2}".format(
                    to_text(_now()), merged, _mk
                )
            )
        except Exception:
            pass
    else:
        try:
            _print_u(
                u"【{0}】PyHive Kerberos：已 source HADOOP_ENV_SH 但未解析到可合并的 Kerberos 相关变量（"
                u"仍请在 user.txt 增加 kinit_keytab= 或调度 export HIVE_KINIT_KEYTAB）".format(to_text(_now()))
            )
        except Exception:
            pass


def _hive_klist_s_ok(env=None):
    """当前 ``KRB5CCNAME``（或传入 env）下 ``klist -s`` 是否成功（有未过期 TGT）。"""
    env_use = env if env is not None else os.environ.copy()
    try:
        dn = open(os.devnull, "wb")
    except Exception:
        dn = None
    try:
        kwargs = {"env": env_use}
        if dn is not None:
            kwargs["stdout"] = dn
            kwargs["stderr"] = dn
        if sys.version_info >= (3, 3):
            kwargs["timeout"] = 25
        subprocess.check_call(["klist", "-s"], **kwargs)
        return True
    except Exception:
        return False
    finally:
        try:
            if dn is not None:
                dn.close()
        except Exception:
            pass


def _hive_try_adopt_mit_default_ccache():
    """
    若 ``/tmp/krb5cc_<uid>`` 存在且 ``klist -s`` 成功，将 ``KRB5CCNAME`` 设为 ``FILE:`` 该路径。
    beeline 子 shell 中 kinit/JVM 常把 TGT 落在默认 cache，父进程 PyHive 需显式复用。
    """
    if (os.getenv("HIVE_PYHIVE_REUSE_MIT_DEFAULT_CCACHE", "1") or "").strip().lower() in ("0", "false", "no", "off"):
        return False
    try:
        uid = os.getuid()
    except Exception:
        uid = 0
    dft = os.path.join(tempfile.gettempdir() or "/tmp", "krb5cc_{0}".format(uid))
    if not os.path.isfile(dft):
        return False
    _tr = os.environ.copy()
    _tr["KRB5CCNAME"] = "FILE:" + dft
    if _hive_klist_s_ok(_tr):
        os.environ["KRB5CCNAME"] = "FILE:" + dft
        return True
    return False


def _hive_prepare_pyhive_kerberos_ccache(hive_cfg, need_kerberos):
    """
    PyHive 在 **当前 Python 进程** 内走 GSSAPI；beeline 的 ``kinit`` 只在子 shell 生效，父进程常仍用
    krb5 默认 ``KCM:``，调度环境无 KCM 时报 ``No KCM server found``。

    - 若 ``KRB5CCNAME`` 为空或为 ``KCM:...``：优先尝试复用 MIT 默认 ``FILE:/tmp/krb5cc_<uid>``（beeline 子 shell 中 kinit 常写入），
      否则新建 ``FILE:.../krb5cc_lgbs_pyhive_*``（可用 ``HIVE_PYHIVE_KRB5CCNAME`` 覆盖，``HIVE_PYHIVE_REUSE_MIT_DEFAULT_CCACHE=0`` 关闭复用）。
    - 若 ``HIVE_PYHIVE_RUN_KINIT`` 未关闭且能构造 ``kinit``：在 **当前进程环境** 下执行一次（与 beeline 同源逻辑，含 ``source HADOOP_ENV_SH``），并以 ``klist -s`` 校验凭据。
    ``HIVE_PYHIVE_IMPORT_KRB_ENV_FROM_HADOOP_SH``：非 ``0`` 时在构造 kinit **之前** 先 ``source`` 并合并（默认 ``auto``）；``0`` 关闭。
    关闭整段：``HIVE_PYHIVE_PREPARE_CCACHE=0``；仅跳过 kinit：``HIVE_PYHIVE_RUN_KINIT=0``。
    """
    if not need_kerberos:
        return
    if (os.getenv("HIVE_PYHIVE_PREPARE_CCACHE", "1") or "").strip().lower() in ("0", "false", "no", "off"):
        return

    explicit = (os.getenv("HIVE_PYHIVE_KRB5CCNAME", "") or "").strip()
    cur = (os.environ.get("KRB5CCNAME") or "").strip()
    if explicit:
        ex = explicit
        if ex.upper().startswith("FILE:") or ex.upper().startswith("DIR::"):
            os.environ["KRB5CCNAME"] = ex
        else:
            os.environ["KRB5CCNAME"] = "FILE:" + ex
    else:
        if cur and not cur.upper().startswith("KCM:"):
            if not _hive_klist_s_ok():
                try:
                    del os.environ["KRB5CCNAME"]
                except Exception:
                    try:
                        os.environ.pop("KRB5CCNAME", None)
                    except Exception:
                        pass
        if not _hive_klist_s_ok():
            _hive_try_adopt_mit_default_ccache()
        if not _hive_klist_s_ok():
            cur2 = (os.environ.get("KRB5CCNAME") or "").strip()
            if (not cur2) or cur2.upper().startswith("KCM:"):
                tmpd = tempfile.gettempdir() or "/tmp"
                try:
                    uid = os.getuid()
                except Exception:
                    uid = 0
                if cur2.upper().startswith("KCM:"):
                    try:
                        del os.environ["KRB5CCNAME"]
                    except Exception:
                        try:
                            os.environ.pop("KRB5CCNAME", None)
                        except Exception:
                            pass
                path_file = os.path.join(tmpd, "krb5cc_lgbs_pyhive_{0}_{1}".format(uid, os.getpid()))
                os.environ["KRB5CCNAME"] = "FILE:" + path_file
    try:
        _print_u(
            u"【{0}】PyHive Kerberos：当前进程 KRB5CCNAME={1}（避免子 shell 外无凭据 / KCM 不可用；"
            u"可设 HIVE_PYHIVE_KRB5CCNAME、HIVE_PYHIVE_PREPARE_CCACHE=0 关闭）".format(
                to_text(_now()), to_text(os.environ.get("KRB5CCNAME", "") or u"(空)")[:500]
            )
        )
    except Exception:
        pass

    _imp = (os.getenv("HIVE_PYHIVE_IMPORT_KRB_ENV_FROM_HADOOP_SH", "auto") or "auto").strip().lower()
    if _imp not in ("0", "false", "no", "off"):
        _hive_merge_whitelisted_krb_env_from_hadoop_sh()
        if not _hive_klist_s_ok():
            _hive_try_adopt_mit_default_ccache()

    if _hive_klist_s_ok():
        os.environ["_HIVE_PYHIVE_KINIT_DONE"] = "1"
        try:
            _print_u(
                u"【{0}】PyHive Kerberos：当前 ccache 已有有效凭据（klist -s），跳过 kinit"
                u"（beeline 先于 PyHive 时 TGT 常在 /tmp/krb5cc_<uid>；可设 HIVE_PYHIVE_REUSE_MIT_DEFAULT_CCACHE=0 禁止复用 MIT 默认路径）。".format(
                    to_text(_now())
                )
            )
        except Exception:
            pass
        return

    if (os.getenv("HIVE_PYHIVE_RUN_KINIT", "1") or "").strip().lower() in ("0", "false", "no", "off"):
        return
    if os.environ.get("_HIVE_PYHIVE_KINIT_DONE") == "1":
        return
    kline = _hive_build_kinit_bash_command(hive_cfg)
    if not kline:
        _psk = (os.getenv("HIVE_PYHIVE_POST_SOURCE_KINIT", "1") or "").strip().lower()
        if _psk not in ("0", "false", "no", "off"):
            _pr = _hive_kinit_principal_string(hive_cfg)
            if _pr:
                kline = _hive_shell_fragment_kinit_if_keytab_files(_pr)
    if not kline:
        try:
            _print_u(
                u"【{0}】PyHive Kerberos：未检测到 kinit 参数（KINIT_CMD 或 HIVE_KINIT_KEYTAB+HIVE_KINIT_USER，"
                u"或 user.txt 的 kinit_k<INTERNAL_B64> + username；"
                u"默认已尝试从 HADOOP_ENV_SH 合并 KRB5*（HIVE_PYHIVE_IMPORT_KRB_ENV_FROM_HADOOP_SH，=0 可关）。"
                u"若随后 GSSAPI 报无凭据，请配置 keytab/用户或事先 kinit 并设 HIVE_PYHIVE_KRB5CCNAME 指向同一 FILE ccache。".format(
                    to_text(_now())
                )
            )
        except Exception:
            pass
        return
    hadoop_env_sh = (os.getenv("HADOOP_ENV_SH", "") or "/opt/hadoopclient/bigdata_env").strip()
    parts = []
    _force_src = (os.getenv("HIVE_PYHIVE_KINIT_SOURCE_BIGDATA_ENV", "") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    _has_explicit_kt = bool(
        re.match(r"^\s*kinit\s+-kt\s+", to_text(kline or u""), re.I)
        or (hive_cfg or {}).get("kinit_keytab")
        or (hive_cfg or {}).get("keytab")
        or (hive_cfg or {}).get("kerberos_keytab")
    )
    _skip_src = _has_explicit_kt and not _force_src
    if hadoop_env_sh and not _skip_src:
        parts.append("if [ -f '{0}' ]; then source '{0}'; fi".format(hadoop_env_sh.replace("'", "'\\''")))
    elif _skip_src:
        _path_px = _hive_bash_export_kerberos_runtime_env()
        if _path_px:
            parts.append(_path_px)
        try:
            _print_u(
                u"【{0}】PyHive Kerberos：user.txt 已配置 keytab，kinit 子进程不再 source bigdata_env（"
                u"使用 {1}；减轻 fork/OOM；强制 source 请设 HIVE_PYHIVE_KINIT_SOURCE_BIGDATA_ENV=1）".format(
                    to_text(_now()),
                    to_text(_hive_resolve_kinit_executable()),
                )
            )
        except Exception:
            pass
    parts.append(kline)
    _klist_ex = _hive_resolve_klist_executable().replace("'", "'\\''")
    full = " && ".join(parts) + " && {0} -s".format(_klist_ex)
    env_copy = os.environ.copy()
    try:
        p = subprocess.Popen(
            ["bash", "-lc", full],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env_copy,
            universal_newlines=True,
        )
        _ko, _ke2 = p.communicate()
        rc = p.returncode
    except Exception as _ke:
        raise RuntimeError(
            u"PyHive 前序 kinit 子进程启动失败（当前 KRB5CCNAME={0}）：{1}。"
            u"请检查 bash、网络与 KRB5_CONFIG。".format(
                to_text(os.environ.get("KRB5CCNAME", "") or u"")[:300],
                to_text(_ke)[:800],
            )
        )
    if rc != 0:
        _es = (to_text(_ke2).strip() if _ke2 else u"")[:1500]
        _os = (to_text(_ko).strip() if _ko else u"")[:800]
        _hint = (
            u"常见原因：keytab 内 principal 与 kinit 主体不一致（请用 ``klist -kt '{0}'`` 查看 keytab 内条目，"
            u"并在 user.txt 设 ``kinit_user=`` 或 export HIVE_KINIT_USER）；"
            u"文件权限不可读；时钟偏差；KDC 不可达；realm 与 keytab 不匹配。"
        ).format(to_text(hive_cfg.get("kinit_keytab") or hive_cfg.get("keytab") or u"<keytab>")[:400])
        if _es and "command not found" in _es.lower():
            _hint += (
                u" 另：子 shell 找不到 kinit/klist（跳过 source bigdata_env 后 PATH 未含 KrbClient/bin）；"
                u"脚本已尝试使用绝对路径，可设 HIVE_KINIT_BIN=/opt/hadoopclient/KrbClient/kerberos/bin/kinit。"
            )
        if _es and (
            "error while loading shared libraries" in _es.lower()
            or "libkadm5srv" in _es.lower()
            or "cannot open shared object file" in _es.lower()
        ):
            _hint += (
                u" 另：kinit 缺少 Kerberos 动态库（LD_LIBRARY_PATH）；"
                u"脚本已从 bigdata_env 合并 LD_LIBRARY_PATH，并在子 shell 导出 KrbClient/lib；"
                u"仍失败可设 HIVE_PYHIVE_KINIT_SOURCE_BIGDATA_ENV=1 恢复 source bigdata_env。"
            )
        if _es and (
            "cannot allocate memory" in _es.lower()
            or "fork:" in _es.lower()
            or "failed to map segment" in _es.lower()
        ):
            _hint += (
                u" 另：stderr 含内存/fork 失败（常见于 source bigdata_env 链过重）；"
                u"已默认在 user.txt 有 keytab 时跳过 kinit 内 source bigdata_env，可单独重跑失败作业或释放内存。"
            )
        raise RuntimeError(
            u"PyHive 前序 kinit 失败（exit={3}，当前 KRB5CCNAME={0}）。bash 片段末尾为：kinit … && klist -s。"
            u"{4}"
            u"{1}{2}"
            u"仍请核对 HIVE_KINIT_KEYTAB、HIVE_KINIT_USER、KINIT_CMD；"
            u"或事先 kinit 并设 HIVE_PYHIVE_KRB5CCNAME 指向已有 FILE ccache。".format(
                to_text(os.environ.get("KRB5CCNAME", "") or u"")[:300],
                (u" stderr=" + _es + u" ") if _es else u" ",
                (u"stdout=" + _os + u" ") if _os else u" ",
                rc,
                _hint,
            )
        )
    os.environ["_HIVE_PYHIVE_KINIT_DONE"] = "1"
    try:
        _print_u(u"【{0}】PyHive Kerberos：已在当前进程环境执行 kinit 且 klist -s 校验通过".format(to_text(_now())))
    except Exception:
        pass




def _hive_col_unqualified_name(col_name):
    raw = to_text(col_name).strip().strip(u"`")
    low = raw.lower()
    if u"." in low:
        return low.rsplit(u".", 1)[-1]
    return low

def _hive_pyhive_import_available():
    """
    本机是否可加载 PyHive（脚本 **仅** 依赖 HS2 拉数）。
    先校验 ``thrift.Thrift``：仅装 pyhive 未装 thrift 时常见 ``No module named thrift.Thrift``。
    """
    global _LAST_PYHIVE_IMPORT_ERROR
    _LAST_PYHIVE_IMPORT_ERROR = None
    try:
        __import__("thrift.Thrift")
    except Exception as e:
        _LAST_PYHIVE_IMPORT_ERROR = (
            to_text(e)[:800]
            + u"  → 请在同一 Python 中安装 Apache Thrift 绑定：pip install 'thrift>=0.10.0'（与 pyhive 一起：pip install 'pyhive' 'thrift>=0.10.0'）"
        )
        return False
    try:
        from pyhive import hive  # type: ignore[import-not-found,unused-import]  # noqa: F401

        return True
    except Exception as e:
        try:
            _LAST_PYHIVE_IMPORT_ERROR = to_text(e)[:1200]
        except Exception:
            try:
                _LAST_PYHIVE_IMPORT_ERROR = text_type(repr(e))[:1200]
            except Exception:
                _LAST_PYHIVE_IMPORT_ERROR = u"(repr failed)"
        try:
            el = to_text(e).lower()
            if u"thrift" in el:
                _LAST_PYHIVE_IMPORT_ERROR += (
                    u"  → 可尝试：pip install 'pyhive' 'thrift>=0.10.0'（Kerberos 另见 thrift_sasl）"
                )
        except Exception:
            pass
        return False


def _hive_sql_strip_trailing_semicolons(sql_text):
    s = to_text(sql_text or u"").strip()
    while s.endswith(u";"):
        s = s[:-1].strip()
    return s


def _hive_first_select_sql_block(sql_text):
    """
    取首条 SQL 片段（按分号粗切；去尾部空句），供 PyHive 侧 ``COUNT(*)`` 诊断。
    多语句脚本只对其首段生成计数 SQL。
    """
    t = _hive_sql_strip_trailing_semicolons(to_text(sql_text or u"")).strip()
    if not t:
        return u""
    parts = [p.strip() for p in t.split(u";")]
    parts = [p for p in parts if p]
    return parts[0] if parts else u""


def _hive_count_sql_replace_select_star(inner_sql):
    """
    将语句首部的 ``SELECT *``（大小写不敏感）替换为 ``SELECT COUNT(*)``；
    不含 ``SELECT *`` 模式时返回空串（调用方走子查询包裹路径）。
    """
    s = to_text(inner_sql or u"").strip()
    if not s:
        return u""
    m = re.match(r"(?is)^\s*select\s+\*\s+", s)
    if not m:
        return u""
    return u"SELECT COUNT(*) " + s[m.end() :]


def _hive_count_sql_wrapped(inner_sql):
    """``SELECT COUNT(*) FROM (<inner>) t``，与 ``_hive_count_sql_replace_select_star`` 互为备用。"""
    s = _hive_sql_strip_trailing_semicolons(to_text(inner_sql or u"")).strip()
    if not s:
        return u""
    return u"SELECT COUNT(*) FROM (" + s + u") t"


def _parse_jdbc_hive2_host_port_db(jdbc_url):
    """
    从 ``jdbc:hive2://host:port/db`` 解析 (host, port, database_path)。
    不含 ``jdbc:hive2://`` 或解析失败时返回 None。
    """
    s = to_text(jdbc_url or u"").strip()
    if not s.lower().startswith("jdbc:hive2://"):
        return None
    try:
        rest = s.split("://", 1)[1]
        rest0 = rest.split(";", 1)[0]
        hostpart, slash, path = rest0.partition("/")
        hostpart = hostpart.strip()
        path = (path or "").strip()
        db = path.split("?", 1)[0].strip() if path else u""
        if not db:
            db = u"default"
        if u":" in hostpart:
            host, _, port_s = hostpart.rpartition(":")
            host = host.strip()
            port = int(port_s)
        else:
            host = hostpart
            port = 10000
        if not host:
            return None
        return host, port, db
    except Exception:
        return None


def _parse_jdbc_hive2_principal_hostname(jdbc_url):
    """从 ``principal=service/hostname@REALM`` 解析出 SPN 中的服务端主机名（供 Kerberos SASL / PyHive host）。"""
    s = to_text(jdbc_url or u"")
    m = re.search(r"(?i)\bprincipal\s*=\s*[^/;=]+/([^@;/\s]+)@", s)
    if m:
        return m.group(1).strip()
    return u""


def _host_looks_like_ipv4(h):
    return _hive_host_looks_like_ipv4(h)


def _hive_hs2_cell_to_text(val):
    """PyHive / HS2 单元格 → unicode 文本（供 ``_row_to_record``）。"""
    if val is None:
        return text_type("")
    try:
        if isinstance(val, bool):
            return text_type("true") if val else text_type("false")
    except Exception:
        pass
    try:
        if isinstance(val, bytes):
            return to_text(val)
    except Exception:
        pass
    try:
        if isinstance(val, datetime.datetime):
            return to_text(val.strftime("%Y-%m-%d %H:%M:%S"))
        if isinstance(val, datetime.date):
            return to_text(val.isoformat())
    except Exception:
        pass
    try:
        from decimal import Decimal

        if isinstance(val, Decimal):
            return to_text(format(val, "f"))
    except Exception:
        pass
    try:
        if sys.version_info[0] < 3:
            import types

            if isinstance(val, types.BufferType):  # type: ignore[attr-defined]
                return to_text(val.decode("utf-8", "replace"))
    except Exception:
        pass
    return to_text(val)


def _pyhive_describe_rows_to_colnames(rows):
    """从 PyHive ``DESCRIBE`` 行集解析列名顺序，跳过表头行与 ``#`` 注释行。"""
    names = []
    for row in rows or []:
        if not row:
            continue
        c0 = to_text(row[0]).strip()
        if not c0 or c0.startswith("#"):
            continue
        c1 = to_text(row[1]).strip().lower() if len(row) > 1 else u""
        if c0.lower() == u"col_name" and c1 == u"data_type":
            continue
        names.append(c0)
    return names


def _pyhive_open_kerberos_tcp_sasl_split(hive_mod, tcp_host, port, sasl_host, database, username, kerberos_service_name):
    """
    PyHive Kerberos：``TSocket`` 连 ``tcp_host``（常为 JDBC 中的 IPv4），
    SASL GSSAPI 的 ``host`` 使用 ``sasl_host``（须为 principal 中的 FQDN），
    以缓解部分 ``libsasl2+GSSAPI`` 在 **JDBC 为 IPv4** 而 **SPN 主机须为 FQDN** 时仅用单一 ``host`` 传 IP 导致的 ``no serverFQDN``。

    成功返回前将 ``_HIVE_PYHIVE_TRANSPORT_SPLIT=1`` 写入环境，供 ``_hive_effective_fetch_size`` 收紧 ``fetchmany`` 批量。
    显式关闭分离：``HIVE_PYHIVE_TCP_SASL_SPLIT=0`` 且勿触发 auto 回退（标准连失败且含 serverFQDN 仍会回退）。
    """
    try:
        os.environ["_HIVE_PYHIVE_TRANSPORT_SPLIT"] = "1"
    except Exception:
        pass
    import getpass

    try:
        import sasl  # type: ignore[import-not-found]
        import thrift_sasl  # type: ignore[import-not-found]
        from thrift.transport.TSocket import TSocket  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            u"PyHive Kerberos（TCP/SASL 分离）需要 sasl 与 thrift_sasl 包。原始错误：{0}".format(e)
        )

    tcp_h = to_text(tcp_host).strip()
    sasl_h = to_text(sasl_host).strip()
    ksn = to_text(kerberos_service_name or u"hive").strip() or u"hive"
    un = to_text(username).strip() if username else getpass.getuser()

    def _sasl_factory():
        c = sasl.Client()
        if sys.version_info[0] < 3:
            c.setAttr("host", str(sasl_h))
            c.setAttr("service", str(ksn))
        else:
            c.setAttr("host", sasl_h)
            c.setAttr("service", ksn)
        c.init()
        return c

    sock = TSocket(str(tcp_h) if sys.version_info[0] < 3 else tcp_h, int(port))
    trans = thrift_sasl.TSaslClientTransport(_sasl_factory, "GSSAPI", sock)
    return hive_mod.Connection(thrift_transport=trans, username=un, database=to_text(database), configuration={})


def _open_pyhive_connection(hive_cfg):
    try:
        from pyhive import hive  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            u"PyHive 需要 PyHive 及 Apache Thrift Python 包（常见缺省报 No module named thrift.Thrift）。"
            u"请执行：pip install 'pyhive' 'thrift>=0.10.0'（Kerberos 常加 thrift_sasl）。原始错误：{0}".format(e)
        )
    _connect_db = (os.getenv("HIVE_BEELINE_CONNECT_DB", "") or "").strip() or u"default"
    try:
        jdbc_resolved = to_text(_build_hive_jdbc_url(hive_cfg, _connect_db)).strip()
    except Exception:
        jdbc_resolved = u""
    parsed = _parse_jdbc_hive2_host_port_db(jdbc_resolved) if jdbc_resolved else None
    if parsed:
        host, port, _db_in_url = parsed
        database = _connect_db
    else:
        host = to_text(hive_cfg.get("host") or u"").strip()
        try:
            port = int(hive_cfg.get("port") or 10000)
        except Exception:
            port = 10000
        database = _connect_db
    if not host:
        raise RuntimeError(
            u"PyHive：无法得到 HS2 host（请在 user.txt 配置 jdbc_url=jdbc:hive2://... 或 st/host）"
        )
    tcp_host = to_text(host).strip()
    username = to_text(hive_cfg.get("username") or u"").strip()
    auth_cfg = to_text(hive_cfg.get("auth") or u"NONE").strip().upper()
    pw = (os.getenv("HIVE_PYHIVE_PASSWORD", "") or "").strip()
    _auth_ov = (os.getenv("HIVE_PYHIVE_AUTH", "") or "").strip().upper()
    effective_auth = _auth_ov or auth_cfg

    ph_host = (os.getenv("HIVE_PYHIVE_HOST", "") or "").strip()
    krb_h = to_text(hive_cfg.get("krbhost") or u"").strip()
    # krbhost 若为 IPv4，与 JDBC host 相同，不能作为 GSSAPI 的 serverFQDN，忽略以免阻断 principal FQDN 回退
    if not ph_host and krb_h and not _host_looks_like_ipv4(krb_h):
        ph_host = krb_h
    _prefer_princ = (os.getenv("HIVE_PYHIVE_PREFER_PRINCIPAL_HOST_ON_IPV4", "1") or "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    principal_h = u""
    jdbc_has_principal = False
    if jdbc_resolved:
        try:
            jdbc_has_principal = bool(re.search(r"(?i)\bprincipal\s*=", jdbc_resolved))
        except Exception:
            jdbc_has_principal = False
        try:
            principal_h = _parse_jdbc_hive2_principal_hostname(jdbc_resolved)
        except Exception:
            principal_h = u""
    kerberos_like = (effective_auth == u"KERBEROS") or jdbc_has_principal
    used_principal_fqdn = False
    if (
        (not ph_host)
        and kerberos_like
        and _prefer_princ
        and _host_looks_like_ipv4(tcp_host)
        and principal_h
    ):
        ph_host = principal_h
        used_principal_fqdn = True
    if not ph_host:
        ph_host = tcp_host
    if used_principal_fqdn:
        try:
            _print_u(
                u"【{0}】PyHive Kerberos：JDBC 为 IPv4（{1}），SASL 需服务端 FQDN，"
                u"已改用 principal 主机名 {2} 作为连接 host（须 DNS 或 /etc/hosts 解析到 HS2；"
                u"亦可设 HIVE_PYHIVE_HOST 或 user.txt 的 krbhost（勿填 IP）；关闭本逻辑设 HIVE_PYHIVE_PREFER_PRINCIPAL_HOST_ON_IPV4=0）".format(
                    to_text(_now()), tcp_host, ph_host
                )
            )
        except Exception:
            pass

    kw = {
        "host": ph_host,
        "port": int(port),
        "database": to_text(database),
    }
    if username:
        kw["username"] = username
    if pw:
        kw["password"] = pw
    _ksn = to_text(hive_cfg.get("kerberos_service_name") or u"hive").strip() or u"hive"
    if _auth_ov:
        kw["auth"] = _auth_ov
        if _auth_ov == u"KERBEROS":
            kw["kerberos_service_name"] = _ksn
    elif auth_cfg == u"KERBEROS" or jdbc_has_principal:
        kw["auth"] = "KERBEROS"
        kw["kerberos_service_name"] = _ksn

    use_kerberos = False
    try:
        if _auth_ov:
            use_kerberos = _auth_ov == u"KERBEROS"
        else:
            use_kerberos = (auth_cfg == u"KERBEROS") or bool(jdbc_has_principal)
    except Exception:
        use_kerberos = False

    try:
        _hive_prepare_pyhive_kerberos_ccache(hive_cfg, use_kerberos)
    except RuntimeError:
        raise
    except Exception as _pc_e:
        try:
            _print_u(
                u"【{0}】PyHive Kerberos：准备 ccache/kinit 时异常（忽略继续连）：{1}".format(
                    to_text(_now()), to_text(_pc_e)[:600]
                )
            )
        except Exception:
            pass

    _spl_raw = (os.getenv("HIVE_PYHIVE_TCP_SASL_SPLIT", "") or "").strip().lower()
    if _spl_raw in ("1", "true", "yes", "on"):
        # 显式开启：JDBC 为 IPv4 且 principal 为 FQDN 时，TCP 连 IP、SASL 用 FQDN（无 DNS 解析 HS2 名时常需此模式）
        use_tcp_sasl_split = bool(
            use_kerberos
            and _host_looks_like_ipv4(tcp_host)
            and ph_host
            and (not _host_looks_like_ipv4(ph_host))
        )
    elif _spl_raw in ("0", "false", "no", "off"):
        use_tcp_sasl_split = False
    else:
        # auto：先试标准 Connection；若报 no serverFQDN 再回退 TCP/SASL 分离（见下方 try/except）
        use_tcp_sasl_split = False

    tcp_wire = (os.getenv("HIVE_PYHIVE_TCP_HOST", "") or "").strip() or tcp_host
    can_tcp_sasl_split = bool(
        use_kerberos
        and _host_looks_like_ipv4(tcp_host)
        and ph_host
        and (not _host_looks_like_ipv4(ph_host))
    )
    try:
        os.environ.pop("_HIVE_PYHIVE_TRANSPORT_SPLIT", None)
    except Exception:
        pass

    if use_tcp_sasl_split:
        try:
            _print_u(
                u"【{0}】PyHive Kerberos：TCP/SASL 分离（显式 HIVE_PYHIVE_TCP_SASL_SPLIT=1；TCP={1}:{2}，SASL host={3}，service={4}）；"
                u"TCP 目标可设 HIVE_PYHIVE_TCP_HOST；大块拉数若 sasl_decode 可调小 HIVE_FETCH_SIZE 或 HIVE_PYHIVE_SPLIT_FETCH_CAP".format(
                    to_text(_now()),
                    tcp_wire,
                    int(port),
                    ph_host,
                    _ksn,
                )
            )
        except Exception:
            pass
        return _pyhive_open_kerberos_tcp_sasl_split(
            hive,
            tcp_wire,
            int(port),
            ph_host,
            to_text(database),
            username,
            _ksn,
        )

    try:
        conn = hive.Connection(**kw)
    except Exception as e0:
        es = to_text(e0).lower()
        if can_tcp_sasl_split and ("serverfqdn" in es):
            try:
                _print_u(
                    u"【{0}】PyHive Kerberos：标准 Connection 失败，自动改用 TCP/SASL 分离。摘要：{1}".format(
                        to_text(_now()),
                        to_text(e0)[:500],
                    )
                )
            except Exception:
                pass
            return _pyhive_open_kerberos_tcp_sasl_split(
                hive,
                tcp_wire,
                int(port),
                ph_host,
                to_text(database),
                username,
                _ksn,
            )
        raise
    try:
        os.environ.pop("_HIVE_PYHIVE_TRANSPORT_SPLIT", None)
    except Exception:
        pass
    return conn


def _iter_hive_rows_pyhive():
    """
    PyHive 直连 HiveServer2：``DESCRIBE`` 列名 + ``cursor.fetchmany`` 流式分批 yield，
    行单元格经 ``_hive_hs2_cell_to_text`` 转为与多维表写入一致的 unicode 文本。
    """
    global _LAST_HIVE_REF_ROW_COUNT
    _LAST_HIVE_REF_ROW_COUNT = None
    hive_db = (to_text(JOB_CONFIG.get("database")) or "").strip()
    if not hive_db:
        raise RuntimeError(u"Hive 库名（配置「上报库名」）为空，无法查询")

    hive_cfg = get_hive_config(hive_db)
    sql_run = (HIVE_SQL or (os.getenv("HIVE_SQL", "") or "").strip()).strip()
    if not sql_run:
        raise RuntimeError(u"HIVE_SQL 为空，请检查配置或环境变量 HIVE_SQL")

    try:
        url_show = _build_hive_jdbc_url(hive_cfg, (os.getenv("HIVE_BEELINE_CONNECT_DB", "") or "").strip() or "default")
        url_show = re.sub(r"(password=)[^;]+", r"\1***", to_text(url_show), flags=re.I)
        _print_u(u"【{0}】Hive PyHive(HS2) 拉数：jdbc_url(脱敏)={1}".format(to_text(_now()), url_show))
    except Exception:
        pass
    _print_u(u"【{0}】Hive SQL={1}".format(to_text(_now()), to_text(sql_run)))

    tbl_name = (to_text(JOB_CONFIG.get("table_name")) or "").strip()
    if not tbl_name:
        raise RuntimeError(u"上报表名（Hive 表）为空，无法 DESCRIBE")
    tbl_q = _hive_full_table_qualified(hive_db, tbl_name)
    sql_desc = u"DESCRIBE {0}".format(tbl_q)

    conn = None
    cur = None
    try:
        conn = _open_pyhive_connection(hive_cfg)
        _fsz = _hive_effective_fetch_size()
        if (os.getenv("_HIVE_PYHIVE_TRANSPORT_SPLIT", "") or "").strip() == "1":
            try:
                _print_u(
                u"【{0}】PyHive：当前为 TCP/SASL 分离传输，单批 fetch 上限={1}（默认 1 行/批以规避 sasl_decode；"
                u"稳定后可 ``export HIVE_PYHIVE_SPLIT_FETCH_CAP=8`` 提速）".format(
                    to_text(_now()),
                    _fsz,
                )
                )
            except Exception:
                pass
        cur = conn.cursor()
        cur.execute(_hive_sql_strip_trailing_semicolons(sql_desc))
        desc_rows = cur.fetchall() or []
        columns = _pyhive_describe_rows_to_colnames(desc_rows)
        if not columns:
            raise RuntimeError(u"PyHive DESCRIBE 未解析到列名")
        ncols = len(columns)

        _log_cnt = (os.getenv("HIVE_BEELINE_LOG_SELECT_COUNT", "1") or "").strip().lower() not in (
            "0",
            "false",
            "no",
            "off",
        )
        if _log_cnt:
            _inner_cnt = _hive_first_select_sql_block(sql_run)
            _cand = []
            _q_rep = _hive_count_sql_replace_select_star(_inner_cnt)
            if _q_rep:
                _cand.append((u"替换SELECT*", _q_rep))
            _q_wrap = _hive_count_sql_wrapped(_inner_cnt)
            if _q_wrap and (not _q_rep or _q_wrap.strip() != _q_rep.strip()):
                _cand.append((u"子查询包裹", _q_wrap))
            _cval = None
            _used_tag = None
            _used_sql = None
            _last_diag = u""
            if _cand:
                for _tag, _qraw in _cand:
                    sql_cnt = _hive_sql_strip_trailing_semicolons(_qraw)
                    try:
                        cur.execute(sql_cnt)
                        one = cur.fetchone()
                        if one and one[0] is not None:
                            try:
                                _cval = int(one[0])
                            except Exception:
                                try:
                                    _cval = int(float(one[0]))
                                except Exception:
                                    _cval = None
                            if _cval is not None:
                                _used_tag, _used_sql = _tag, _qraw
                                break
                        _last_diag = u"(COUNT 结果为空或非数字)"
                    except Exception as _ec:
                        _last_diag = to_text(_ec)[:800]
                if _cval is not None:
                    _LAST_HIVE_REF_ROW_COUNT = _cval
                    try:
                        _print_u(
                            u"【{0}】Hive PyHive：COUNT(*) 与当前 HIVE_SQL 同条件= {1}；方式={2}；SQL(预览)={3}".format(
                                to_text(_now()), _cval, _used_tag, to_text(_used_sql)[:500]
                            )
                        )
                    except Exception:
                        pass
                else:
                    try:
                        _print_u(
                            u"【{0}】Hive PyHive：COUNT(*) 未得到数值（已尝试：{1}）；摘要={2}".format(
                                to_text(_now()),
                                u"、".join([t for t, _ in _cand]),
                                _last_diag[:1500] if _last_diag else u"(无)",
                            )
                        )
                    except Exception:
                        pass
            else:
                try:
                    _print_u(
                        u"【{0}】Hive PyHive：跳过 COUNT（无法从 HIVE_SQL 生成 COUNT 语句）".format(to_text(_now()))
                    )
                except Exception:
                    pass

        sql_sel = _hive_sql_strip_trailing_semicolons(sql_run)
        cur.execute(sql_sel)
        try:
            cur.arraysize = max(1, int(_fsz))
        except Exception:
            pass

        col_comments = {}
        col_oracle_cats = ["text"] * ncols
        total = 0
        _warned_ncols = False
        try:
            _print_u(
                u"【{0}】Hive PyHive：拉数 batch_size={1}（HIVE_FETCH_SIZE 与 HIVE_PYHIVE_FETCH_CAP 已取 min；"
                u"物化模式 HIVE_MATERIALIZE_BEFORE_KINGSOFT_INSERT={2}）".format(
                    to_text(_now()),
                    _fsz,
                    (os.getenv("HIVE_MATERIALIZE_BEFORE_KINGSOFT_INSERT", "0") or "0").strip(),
                )
            )
        except Exception:
            pass
        for batch in _pyhive_cursor_fetch_raw_batches(cur, _fsz):
            if not batch:
                continue
            out_rows = []
            for row in batch:
                rr = list(row) if row is not None else []
                if len(rr) != ncols and not _warned_ncols:
                    _warned_ncols = True
                    try:
                        _print_u(
                            u"【{0}】Hive PyHive：警告 HS2 行宽={1} 与 DESCRIBE 列数={2} 不一致（仅提示首条）；"
                            u"将截断或右补空".format(to_text(_now()), len(rr), ncols)
                        )
                    except Exception:
                        pass
                if len(rr) < ncols:
                    rr.extend([None] * (ncols - len(rr)))
                elif len(rr) > ncols:
                    rr = rr[:ncols]
                out_rows.append(tuple(_hive_hs2_cell_to_text(x) for x in rr))
            total += len(out_rows)
            yield columns, out_rows, col_comments, col_oracle_cats

        if total == 0:
            _print_u(u"【{0}】Hive 查询结果为空（0 行，PyHive）".format(to_text(_now())))
        else:
            try:
                _print_u(
                    u"【{0}】Hive PyHive：字段数={1}，累计行数={2}，batch={3}".format(
                        to_text(_now()), ncols, total, _fsz
                    )
                )
            except Exception:
                pass
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


# ==================== 金山多维表接口配置（参考 kingsoft-data-insert-hive-prod.py） ====================
# 服务器配置
API_HOST = "<INTERNAL_API_HOST>"
API_PORT = 5489

# 默认凭证（与截图一致）
DEFAULT_APP_ID = "<YOUR_APP_ID>"
DEFAULT_APP_KEY = "<YOUR_APP_SECRET>"
DEFAULT_CLIENT_ID = "<YOUR_APP_ID>"
DEFAULT_CLIENT_SECRET = "<YOUR_APP_SECRET>"

# 企业ID（搜索文档库接口必需），默认1
DEFAULT_COMPANY_ID = "1"

API_PATH_OAUTH_TOKEN = "/openapi/oauth2/token"
API_PATH_DOCLIBS = "/openapi/v7/doclib/search"
API_PATH_FILES_SEARCH = "/openapi/v7/files/search"
API_PATH_FILE_SCHEMA = "/openapi/v7/coop/dbsheet/{file_id}/schema"
API_PATH_FILE_RECORDS = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records"
API_PATH_FILE_RECORDS_CREATE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records/create"
API_PATH_FILE_RECORDS_BY_PAGE = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records/list_by_page"
# 创建字段（文档：POST .../sheets/{sheet_id}/fields，body: fields + prefer_id）
API_PATH_FILE_SHEET_FIELDS = "/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/fields"

CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded"
CONTENT_TYPE_OCTET_STREAM = "application/octet-stream"
CONTENT_TYPE_JSON = "application/json"

SIGNATURE_PREFIX = "KSO-1"
URL_SPLIT_KEYWORD = "openapi"
OAUTH_GRANT_TYPE = "client_credentials"


# ==================== 多维表定位参数（用名称定位 file_id + sheet_id） ====================
# 这里的 DOC_LIB_NAME/FILE_NAME/SHEET_NAME 也会在每条配置记录执行前动态设置为“目标多维表”的定位参数。
# 启动时先用 config_* 三参定位配置 sheet。
DOC_LIB_NAME = (JOB_CONFIG.get("config_doc_lib_name") or os.getenv("KINGSOFT_DOC_LIB_NAME", "")).strip()
FILE_NAME = (JOB_CONFIG.get("config_file_name") or os.getenv("KINGSOFT_FILE_NAME", "")).strip()
SHEET_NAME = (JOB_CONFIG.get("config_sheet_name") or os.getenv("KINGSOFT_SHEET_NAME", "")).strip()

# 写入策略：每个 Hive 行 → 至多 1 条多维表记录；RECORDS_BATCH_SIZE 仅控制单次 create 请求合并条数
RECORDS_BATCH_SIZE = int(os.getenv("KINGSOFT_RECORDS_BATCH_SIZE", "200"))
PREFER_ID = os.getenv("KINGSOFT_PREFER_ID", "false").lower() == "true"
try:
    KINGSOFT_HTTP_ERROR_BODY_MAX = int(os.getenv("KINGSOFT_HTTP_ERROR_BODY_MAX", "8000"))
except Exception:
    KINGSOFT_HTTP_ERROR_BODY_MAX = 8000
if KINGSOFT_HTTP_ERROR_BODY_MAX < 1000:
    KINGSOFT_HTTP_ERROR_BODY_MAX = 1000

# 字段映射：Oracle 列名 -> 多维表字段名（或字段 id，当 PREFER_ID=true 时）
# 为空时：默认用 Oracle 列名直接作为多维表字段 key（需与多维表字段名一致）
FIELD_MAPPING_JSON = os.getenv("KINGSOFT_FIELD_MAPPING_JSON", "")

# 多维表「创建人/最后修改人/创建时间/最后修改时间」等为系统自动维护字段，
# 创建记录 API 写入会触发 E_DBSheet_ALTER_AUTO_FIELD；写入前会剔除这些 key（含常见别名）。
# 若需把 Oracle 的 TYKY_SJCP_* 落到表里，请在多维表自建文本/日期列，并在 field_mapping_json 中映射到自建列名。
SHEET_AUTO_MANAGED_FIELD_ALIAS_GROUPS = (
    (u"创建人", u"创建者", u"录入人"),
    (u"最后修改人", u"修改人", u"最后更新人", u"更新人"),
    (u"创建时间", u"创建日期", u"录入时间"),
    (u"最后修改时间", u"修改时间", u"最后更新时间", u"更新时间"),
)

_AUTO_MANAGED_STRIP_LOGGED = False
_USER_SKIP_STRIP_LOGGED = False


def _load_user_skip_field_names():
    """
    写入前额外剔除的栏位展示名（避免 E_INVALID_REQUEST：单选/多选需选项 id、公式/只读栏位等）。
    - KINGSOFT_SKIP_FIELD_NAMES_JSON：JSON 数组，例如 ["危险边坡隐患最后更新时间"]
    - KINGSOFT_SKIP_FIELD_NAMES：竖线分隔，例如 栏位A|栏位B
    """
    names = []
    raw_j = (os.getenv("KINGSOFT_SKIP_FIELD_NAMES_JSON", "") or "").strip()
    if raw_j:
        try:
            arr = json.loads(raw_j)
            if isinstance(arr, list):
                for x in arr:
                    t = to_text(x).strip()
                    if t:
                        names.append(t)
        except Exception:
            pass
    raw_p = (os.getenv("KINGSOFT_SKIP_FIELD_NAMES", "") or "").strip()
    if raw_p:
        for part in raw_p.split("|"):
            t = to_text(part).strip()
            if t:
                names.append(t)
    return names


def _build_block_sets_from_names(name_list):
    """把一组栏位名转为 (精确集合, 归一化集合)，与系统自动字段剔除逻辑一致。"""
    block_exact = set()
    block_norm = set()
    for a in name_list or []:
        t = to_text(a).strip()
        if t:
            block_exact.add(t)
        n = _normalize_key_for_match(a)
        if n:
            block_norm.add(n)
    return block_exact, block_norm


def _strip_fields_by_block_sets(fields_dict, block_exact, block_norm):
    if not fields_dict or (not block_exact and not block_norm):
        return fields_dict, []
    out = {}
    dropped = []
    for k, v in fields_dict.items():
        kt = to_text(k).strip()
        if kt in block_exact:
            dropped.append(kt)
            continue
        kn = _normalize_key_for_match(kt)
        if kn in block_norm:
            dropped.append(kt)
            continue
        out[k] = v
    return out, dropped


def _kingsoft_error_parse_json_from_text(err_text):
    t = to_text(err_text)
    idx = t.find('{"code"')
    if idx < 0:
        idx = t.find("{")
    if idx < 0:
        return None
    tail = t[idx:]
    try:
        return json.loads(tail)
    except Exception:
        return None


def _kingsoft_error_decode_invalid_cells(err_text, echo_decode_error=False):
    """
    从 RuntimeError 文本解析响应 JSON，解码 debug.extra(base64)，返回 invalidCells 列表。
    无法解析或缺少 extra 时返回 None；解析成功但无单元格时返回 []。
    """
    obj = _kingsoft_error_parse_json_from_text(err_text)
    if not isinstance(obj, dict):
        return None
    dbg = obj.get("debug") or {}
    if not isinstance(dbg, dict):
        return None
    extra = dbg.get("extra")
    if not extra:
        return None
    try:
        eb = to_text(extra).strip()
        pad = (-len(eb)) % 4
        if pad:
            eb = eb + ("=" * pad)
        raw = base64.b64decode(eb)
        inner_txt = raw.decode("utf-8", "replace")
        inner = json.loads(inner_txt)
    except Exception as _de:
        if echo_decode_error:
            _print_u(u"【{0}】金山错误 debug.extra 解码失败：{1}".format(to_text(_now()), to_text(_de)))
        return None
    info = (inner or {}).get("invalidCellsInfo") or {}
    cells = (info or {}).get("invalidCells") or []
    if not isinstance(cells, list):
        return []
    return cells


def _invalid_cells_unique_field_keys(cells):
    seen = set()
    out = []
    for c in cells or []:
        if not isinstance(c, dict):
            continue
        fk = to_text(c.get("fieldKey")).strip()
        if fk and fk not in seen:
            seen.add(fk)
            out.append(fk)
    return out


def _strip_norm_records_by_field_display_keys(norm_records, field_keys):
    """
    从 create_records 的 norm_records（每项含 fields_value JSON 字符串）中剔除指定展示名栏位。
    剔除后 fields 为空的记录丢弃。返回新列表。
    """
    if not norm_records or not field_keys:
        return norm_records
    be, bn = _build_block_sets_from_names(field_keys)
    out = []
    for r in norm_records:
        if not isinstance(r, dict):
            continue
        fv = r.get("fields_value")
        if fv is None:
            continue
        try:
            parsed = json.loads(to_text(fv))
        except Exception:
            out.append(r)
            continue
        if not isinstance(parsed, dict):
            out.append(r)
            continue
        stripped, _dropped = _strip_fields_by_block_sets(parsed, be, bn)
        if not stripped:
            continue
        out.append({"fields_value": json.dumps(stripped, ensure_ascii=False)})
    return out


def _summarize_kingsoft_http_error_json(err_text):
    """
    从 RuntimeError 文本中尽量解析 JSON，并解码 debug.extra(base64) 打印 invalidCells 摘要。
    """
    cells = _kingsoft_error_decode_invalid_cells(err_text, echo_decode_error=True)
    if cells is None:
        return
    try:
        if not cells:
            _print_u(u"【{0}】金山错误解码后无 invalidCells".format(to_text(_now())))
            return
        keys = []
        for c in cells[:15]:
            if not isinstance(c, dict):
                continue
            keys.append(
                u"{0} err={1}".format(to_text(c.get("fieldKey")), to_text(c.get("errName")))
            )
        uniq_fk = _invalid_cells_unique_field_keys(cells)
        _print_u(
            u"【{0}】金山 invalidCells 摘要（前15条）：{1}".format(to_text(_now()), u" || ".join(keys))
        )
        if uniq_fk:
            _print_u(
                u"【{0}】涉及栏位（去重）：{1}".format(
                    to_text(_now()), u",".join(uniq_fk[:30])
                )
            )
            _print_u(
                u"【{0}】若为单选/多选/公式等栏位，可设置环境变量剔除："
                u"KINGSOFT_SKIP_FIELD_NAMES_JSON=[\"栏位名\"] 或 KINGSOFT_SKIP_FIELD_NAMES=栏位A|栏位B；"
                u"亦可开启自动剔除重试（默认开）：KINGSOFT_AUTO_STRIP_INVALID_FIELDS=1".format(
                    to_text(_now())
                )
            )
    except Exception as _se:
        _print_u(u"【{0}】invalidCells 摘要打印失败：{1}".format(to_text(_now()), to_text(_se)))


def _now():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _guess_runtime_ipv4():
    """
    推测本机用于出网的 IPv4（不真正发包）；失败或仅为 127.0.0.1 时再尝试其它方式。
    """
    ip = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.settimeout(0.35)
        except Exception:
            pass
        try:
            s.connect(("<INTERNAL_HOST>.255", 1))
            ip = s.getsockname()[0]
        except Exception:
            pass
        finally:
            try:
                s.close()
            except Exception:
                pass
    except Exception:
        pass
    if ip and to_text(ip).strip() and to_text(ip).strip() != u"127.0.0.1":
        return to_text(ip).strip()
    try:
        hn = socket.gethostname()
        g = to_text(socket.gethostbyname(hn or u"localhost")).strip()
        if g and g != u"127.0.0.1":
            return g
    except Exception:
        pass
    try:
        p = subprocess.Popen(
            [u"hostname", u"-I"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            universal_newlines=True,
        )
        out, _err = p.communicate()
        if p.returncode == 0 and out:
            for x in to_text(out).split():
                x = x.strip()
                if x and not x.startswith(u"127."):
                    return x
    except Exception:
        pass
    return ip if ip else u""


def _log_runtime_host_and_pip_diags():
    """
    启动时打印主机名、IP、本脚本所用解释器，以及多套 pip 输出对照。
    - **主结论**以 ``sys.executable -m pip show`` 为准（与脚本内 ``import`` 完全一致，日志里会打出完整路径如 /usr/bin/python3，**不是** shell 里裸敲的 ``python``）。
    - 手工执行 ``python -m pip`` 若指向 **Py2 的 /usr/bin/python** 且无 pip，会误以为自己环境缺包；与调度用 **python3** 不是同一套解释器。
    - 追加：PATH 中 ``pip show``（与 ssh 里习惯一致）、若存在且不同的 ``python -m pip`` 对照；可选 ``import`` 探测。
    关闭：KINGSOFT_LOG_RUNTIME_PIP_DIAG=0；关闭对照段：KINGSOFT_PIP_DIAG_EXTRA=0；关闭 import 行：KINGSOFT_PIP_DIAG_IMPORT_PROBE=0
    Py2 进程下对 ``python3`` 的 sibling 对照：KINGSOFT_PIP_DIAG_PYTHON3_PROBE=0 关闭
    """
    if (os.getenv("KINGSOFT_LOG_RUNTIME_PIP_DIAG", "1") or "").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        return
    try:
        hn = to_text(socket.gethostname())
    except Exception:
        hn = u""
    try:
        fq = to_text(socket.getfqdn())
    except Exception:
        fq = u""
    ip_ov = (os.getenv("KINGSOFT_DIAGNOSTIC_SERVER_IP", "") or "").strip()
    ip_g = ip_ov or _guess_runtime_ipv4() or u"(未能推测)"
    ex = to_text(sys.executable)
    try:
        _print_u(
            u"【{0}】运行环境诊断：hostname={1} fqdn={2} server_ip(推测或覆盖)={3} sys.executable={4}".format(
                to_text(_now()), hn, fq, to_text(ip_g), ex
            )
        )
    except Exception:
        pass
    try:
        maxc = int(os.getenv("KINGSOFT_PIP_SHOW_LOG_MAX_CHARS", "8000"))
    except Exception:
        maxc = 8000
    if maxc < 400:
        maxc = 400

    def _pip_cap(cmd_list):
        blob = u""
        rc = -999
        try:
            p = subprocess.Popen(
                cmd_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                universal_newlines=True,
            )
            out, err = p.communicate()
            rc = p.returncode
            blob = to_text(out) if out else u""
            if err and to_text(err).strip():
                blob += u"\n--- stderr ---\n" + to_text(err)
        except Exception as e:
            blob = to_text(e)
            rc = -1
        if len(blob) > maxc:
            blob = blob[:maxc] + u"\n... (截断，见 KINGSOFT_PIP_SHOW_LOG_MAX_CHARS)"
        return rc, blob

    def _pip_log(title, cmd_list):
        rc, blob = _pip_cap(cmd_list)
        try:
            cmd_s = u" ".join([to_text(x) for x in cmd_list])
            _print_u(
                u"【{0}】{1}\n命令: {2}\n退出码={3}\n{4}".format(
                    to_text(_now()), title, cmd_s, rc, blob if blob.strip() else u"(无输出)"
                )
            )
        except Exception:
            pass

    try:
        _print_u(
            u"【{0}】pip 说明：下列 **主** 段使用本脚本解释器 ``{1} -m pip``（与 import 同源）；"
            u"若 ssh 里 ``python -m pip`` 失败多为 **PATH 中 python 非本解释器**（常见 Py2 无 pip）。".format(
                to_text(_now()), ex
            )
        )
    except Exception:
        pass

    _pip_log(
        u"【主】sys.executable -m pip show pyhive（与脚本 import 同源，非裸命令 python）",
        [ex, u"-m", u"pip", u"show", u"pyhive"],
    )
    _pip_log(
        u"【主】sys.executable -m pip show thrift",
        [ex, u"-m", u"pip", u"show", u"thrift"],
    )

    extra = (os.getenv("KINGSOFT_PIP_DIAG_EXTRA", "1") or "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    if extra:
        wh_pip = None
        wh_py = None
        try:
            import shutil

            if getattr(shutil, "which", None):
                wh_pip = shutil.which("pip") or shutil.which("pip3")
                wh_py = shutil.which("python")
        except Exception:
            wh_pip = None
            wh_py = None
        if wh_pip:
            _pip_log(
                u"【对照】PATH 中 pip show（与手工 ``pip show`` 一致，可能与上面 site-packages 不同）",
                [to_text(wh_pip), u"show", u"pyhive"],
            )
            _pip_log(
                u"【对照】PATH 中 pip show thrift",
                [to_text(wh_pip), u"show", u"thrift"],
            )
        if wh_py and to_text(wh_py).strip() != ex.strip():
            _pip_log(
                u"【对照】PATH 中 python -m pip show pyhive（若失败见 No module named pip，多为 Py2 与脚本 python3 不一致）",
                [to_text(wh_py), u"-m", u"pip", u"show", u"pyhive"],
            )

    if (os.getenv("KINGSOFT_PIP_DIAG_IMPORT_PROBE", "1") or "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    ):
        t_st = u""
        try:
            __import__("thrift.Thrift")
            t_st = u"ok"
        except Exception as e:
            t_st = u"FAIL: " + to_text(e)[:400]
        p_st = u""
        try:
            from pyhive import hive  # type: ignore[import-not-found,unused-import]  # noqa: F401

            p_st = u"ok"
        except Exception as e:
            p_st = u"FAIL: " + to_text(e)[:400]
        try:
            _print_u(
                u"【{0}】import 探测（与 sys.executable 一致）：thrift.Thrift → {1} | pyhive → {2}".format(
                    to_text(_now()), t_st, p_st
                )
            )
        except Exception:
            pass

    # Py2 作业进程：再探测本机 python3（依赖常装在此，与调度误用 python 形成对照）
    if sys.version_info[0] < 3 and (os.getenv("KINGSOFT_PIP_DIAG_PYTHON3_PROBE", "1") or "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    ):
        py3_list = []
        try:
            import shutil

            if getattr(shutil, "which", None):
                w3 = shutil.which("python3")
                if w3:
                    py3_list.append(to_text(w3).strip())
        except Exception:
            pass
        for _c3 in (u"/usr/bin/python3", u"/usr/local/bin/python3"):
            try:
                if os.path.isfile(_c3) and _c3 not in py3_list:
                    py3_list.append(_c3)
            except Exception:
                pass
        py3_ex = py3_list[0] if py3_list else None
        if py3_ex:
            try:
                _print_u(
                    u"【{0}】当前进程为 **Python2**（{1}）；下列为 **python3**  sibling 对照。"
                    u"若此处 pip/import 成功而上面失败，请调度改为：``{2} …/lgbs-data-insert-kingsoft-prod-all.py …``".format(
                        to_text(_now()), ex, py3_ex
                    )
                )
            except Exception:
                pass
            _pip_log(
                u"【python3 对照】{0} -m pip show pyhive".format(py3_ex),
                [py3_ex, u"-m", u"pip", u"show", u"pyhive"],
            )
            _pip_log(
                u"【python3 对照】{0} -m pip show thrift".format(py3_ex),
                [py3_ex, u"-m", u"pip", u"show", u"thrift"],
            )
            try:
                _sn = u"import thrift.Thrift; from pyhive import hive; print('PY3_IMPORT_OK')"
                p3 = subprocess.Popen(
                    [py3_ex, u"-c", _sn],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=False,
                    universal_newlines=True,
                )
                o3, e3 = p3.communicate()
                try:
                    _print_u(
                        u"【{0}】python3 -c import 对照：rc={1} stdout={2} stderr={3}".format(
                            to_text(_now()),
                            p3.returncode,
                            to_text(o3).strip()[:200],
                            to_text(e3).strip()[:500],
                        )
                    )
                except Exception:
                    pass
            except Exception as _e3:
                try:
                    _print_u(u"【{0}】python3 import 对照异常：{1}".format(to_text(_now()), to_text(_e3)[:400]))
                except Exception:
                    pass
        else:
            try:
                _print_u(
                    u"【{0}】当前为 Python2 且未找到 python3（which /usr/bin/python3 等），无法做 sibling 对照".format(
                        to_text(_now())
                    )
                )
            except Exception:
                pass


def _merge_request_headers(*parts):
    """合并 HTTP 头字典（后者覆盖前者）。避免在 dict 字面量里使用 ** 展开（Python2/老版本不支持）。"""
    out = {}
    for p in parts:
        if p:
            out.update(p)
    return out


def get_request_headers(
    method,
    url,
    body="",
    content_type=CONTENT_TYPE_FORM_URLENCODED,
    app_id=None,
    app_key=None,
):
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

    signature = hmac.new(
        app_key.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    authorization = "{} {}:{}".format(SIGNATURE_PREFIX, app_id, signature)
    return {
        "X-Kso-Date": date_string,
        "Content-Type": content_type,
        "X-Kso-Authorization": authorization,
    }


def _http_request(
    method,
    path,
    headers,
    body=None,
):
    conn = httplib.HTTPConnection(API_HOST, API_PORT, timeout=120)
    try:
        send_body = body
        # py2: unicode -> utf-8 bytes；py3: str -> utf-8 bytes
        if isinstance(send_body, text_type):
            send_body = send_body.encode("utf-8")

        conn.request(method.upper(), path, body=send_body, headers=headers)
        resp = conn.getresponse()
        raw = resp.read()
        text = raw.decode("utf-8", errors="replace") if raw else ""
        if resp.status >= 400:
            # Py2：模板必须用 unicode，且响应体可能很长/含中文，否则 format 会触发 UnicodeEncodeError
            body_preview = to_text(text)
            if len(body_preview) > KINGSOFT_HTTP_ERROR_BODY_MAX:
                body_preview = body_preview[:KINGSOFT_HTTP_ERROR_BODY_MAX] + u"...(truncated)"
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
    payload = "grant_type={0}&client_id={1}&client_secret={2}".format(
        OAUTH_GRANT_TYPE, client_id, client_secret
    )
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
    resp = _http_request(
        method="POST",
        path=API_PATH_OAUTH_TOKEN,
        headers=req_headers,
        body=payload,
    )

    token = (resp or {}).get("access_token") or (resp or {}).get("data", {}).get("access_token")
    token_type = (resp or {}).get("token_type") or "Bearer"
    if not token:
        raise RuntimeError("授权失败，响应：{0}".format(resp))
    return {"access_token": token, "token_type": token_type}

def get_auth():
    """
    获取鉴权信息：
    - 若环境变量显式注入了 token（常用于“用户态 token / 有写权限 token”），则优先使用
    - 否则走应用 client_credentials
    """
    injected = (os.getenv("KINGSOFT_ACCESS_TOKEN", "") or "").strip()
    if injected:
        token_type = (os.getenv("KINGSOFT_TOKEN_TYPE", "") or "").strip() or "Bearer"
        print("【{0}】使用注入的 KINGSOFT_ACCESS_TOKEN（token_type={1}）".format(_now(), token_type))
        return {"access_token": injected, "token_type": token_type}
    return app_authorize()


def get_doc_lib_list(auth, keyword):
    # 参考 kingsoft-data-insert-hive-prod-all.py / kingsoft-data-insert-hive-prod.py：
    # 搜索文档库接口使用 GET + query（POST 会在部分网关返回 404 Route Not Found）
    method = "GET"

    keyword_str = u"" if keyword is None else to_text(keyword)
    encoded_keyword = url_quote_any(keyword_str, safe="")
    encoded_company_id = url_quote_any(str(DEFAULT_COMPANY_ID), safe="")
    page_size = 50

    path = "{0}?page_size={1}&company_id={2}&keyword={3}".format(
        API_PATH_DOCLIBS, page_size, encoded_company_id, encoded_keyword
    )
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    body = ""
    headers = get_request_headers(method=method, url=url_for_sign, body=body, content_type=CONTENT_TYPE_OCTET_STREAM)
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
    resp = _http_request(
        method=method,
        path=path,
        headers=req_headers,
        body=body,
    )
    items = (resp or {}).get("data", {}).get("items", []) or (resp or {}).get("items", []) or []
    return [it for it in items if isinstance(it, dict)]


def get_files_keyword(auth, keyword, drive_ids):
    # 参考 kingsoft-data-insert-hive-prod.py：
    # 文件搜索接口使用 GET，并且签名必须基于“包含 query 的完整 path”，content-type 用 application/octet-stream
    method = "GET"
    base_path = API_PATH_FILES_SEARCH

    query = [("keyword", u"" if keyword is None else to_text(keyword)), ("type", "file_name"), ("page_size", "100")]
    for did in drive_ids or []:
        if did:
            query.append(("drive_ids", str(did)))

    path = "{0}?{1}".format(base_path, urlencode_any(query, doseq=True))
    url_for_sign = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    body = ""
    signed_headers = get_request_headers(method=method, url=url_for_sign, body=body, content_type=CONTENT_TYPE_OCTET_STREAM)

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
    resp = _http_request(
        method=method,
        path=path,
        headers=req_headers,
        body=body,
    )
    items = (resp or {}).get("data", {}).get("items", []) or []
    # 兼容返回结构：有的网关返回 item.file
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
    return _http_request(
        method=method,
        path=path,
        headers=req_headers,
        body=None,
    )


def _iter_sheet_field_entries(schema_resp, sheet_id, prefer_id):
    """
    遍历当前 sheet 在 schema 中的字段定义，yield (write_key, field_dict)。
    write_key：与创建记录时 prefer_id 一致（展示名 或 字段 id）。
    """
    if not schema_resp:
        return
    try:
        data = (schema_resp or {}).get("data", {}) if isinstance(schema_resp, dict) else {}
        sheets = (data or {}).get("sheets", []) if isinstance(data, dict) else []
        target = None
        for s in sheets or []:
            try:
                if str(s.get("id")) == str(sheet_id):
                    target = s
                    break
            except Exception:
                continue
        field_lists = []
        if isinstance(target, dict):
            for k in ("fields", "fields_schema", "field_schema", "fieldsSchema", "columns", "cols"):
                v = target.get(k)
                if isinstance(v, list) and v:
                    field_lists.append(v)

        for k in ("fields_schema", "fields", "columns", "cols"):
            v2 = (data or {}).get(k) if isinstance(data, dict) else None
            if isinstance(v2, list) and v2:
                field_lists.append(v2)

        def _field_belongs_to_sheet(fobj):
            try:
                sid = fobj.get("sheet_id") or fobj.get("sheetId") or fobj.get("sheet") or fobj.get("sheetID")
                if sid is None:
                    return True
                return str(sid) == str(sheet_id)
            except Exception:
                return True

        def _pick_field_name(fobj):
            for k in ("title", "label", "field_name", "fieldName", "name"):
                v = fobj.get(k)
                if v is None:
                    continue
                vt = to_text(v).strip()
                if vt:
                    return vt
            v = fobj.get("name")
            return to_text(v).strip() if v is not None else ""

        for fl in field_lists:
            for f in fl or []:
                if not isinstance(f, dict):
                    continue
                if not _field_belongs_to_sheet(f):
                    continue
                fid = f.get("id") or f.get("field_id") or f.get("fieldId") or f.get("column_id") or f.get("col_id")
                fname = _pick_field_name(f)
                key = fid if prefer_id else fname
                if key is None:
                    continue
                wk = to_text(key).strip()
                if wk:
                    yield wk, f
    except Exception:
        return


def _extract_allowed_field_keys(schema_resp, sheet_id, prefer_id):
    """
    从 schema 响应中提取当前 sheet 允许写入的字段 key：
    - prefer_id=false：允许字段名（name）
    - prefer_id=true：允许字段 id（id）
    结构兼容：字段列表可能位于 sheet.fields / sheet.fields_schema 等位置。
    """
    allowed = set()
    try:
        for wk, _f in _iter_sheet_field_entries(schema_resp, sheet_id, prefer_id):
            allowed.add(wk)
    except Exception:
        return allowed
    return allowed


def _kingsoft_field_raw_type(fobj):
    if not isinstance(fobj, dict):
        return ""
    for k in ("type", "field_type", "fieldType", "field_kind", "fieldKind", "kind"):
        v = fobj.get(k)
        if v is None:
            continue
        s = to_text(v).strip()
        if s:
            return s
    utype = fobj.get("ui_type") or fobj.get("uiType")
    if utype is not None:
        s = to_text(utype).strip()
        if s:
            return s
    return ""


def _kingsoft_raw_type_to_category(raw):
    """
    将多维表 schema 中的 type 字符串归一为粗粒度类别，便于与 Oracle 列类型对照。
    返回 (category, raw) category 取值：text/number/datetime/date/time/bool/select/attachment/system/unknown
    """
    if not raw:
        return "unknown", ""
    u = to_text(raw).upper().replace(" ", "").replace("_", "")
    if not u:
        return "unknown", raw
    if u in ("CREATEDBY", "LASTMODIFIEDBY", "CREATEDTIME", "LASTMODIFIEDTIME"):
        return "system", raw
    for x in ("SINGLESELECT", "MULTISELECT", "MULTIPLESELECT", "SELECT", "OPTION"):
        if x in u:
            return "select", raw
    for x in ("ATTACHMENT", "IMAGE", "FILE"):
        if x in u:
            return "attachment", raw
    for x in ("CHECKBOX", "BOOLEAN", "BOOL"):
        if x in u or u == "BOOL":
            return "bool", raw
    for x in ("PHONE", "EMAIL", "URL", "LINK", "HYPERLINK", "BARCODE", "TEXT", "STRING", "SINGLELINE", "MULTILINE", "TEXTAREA", "RICHTEXT", "NOTE", "TITLE"):
        if x in u:
            return "text", raw
    for x in ("NUMBER", "NUMERIC", "CURRENCY", "PERCENT", "FLOAT", "INTEGER", "INT"):
        if x in u:
            return "number", raw
    if "DATETIME" in u or u.endswith("DATETIME"):
        return "datetime", raw
    if u == "DATE" or (u.startswith("DATE") and "UPDATE" not in u and "DATETIME" not in u):
        return "date", raw
    for x in ("TIME", "CLOCK"):
        if x in u and "DATE" not in u and "DATETIME" not in u:
            return "time", raw
    if "DATE" in u or "TIME" in u or "CALENDAR" in u:
        return "datetime", raw
    return "unknown", raw


def _extract_sheet_field_type_map(schema_resp, sheet_id, prefer_id):
    """
    write_key -> {"raw_type": str, "category": str}
    同一 write_key 多次出现时保留首次。
    """
    out = {}
    try:
        seen = set()
        for wk, f in _iter_sheet_field_entries(schema_resp, sheet_id, prefer_id):
            if not wk or wk in seen:
                continue
            seen.add(wk)
            raw = _kingsoft_field_raw_type(f)
            cat, _r = _kingsoft_raw_type_to_category(raw)
            out[wk] = {"raw_type": raw, "category": cat}
    except Exception:
        pass
    return out


def _schema_category_compare_key(cat):
    c = cat or "unknown"
    if c in ("integer",):
        return "number"
    if c in ("date",):
        return "datetime"
    return c


def _parse_datetime_loose(val):
    """将常见字符串解析为 datetime；失败返回 None。"""
    if val is None:
        return None
    if isinstance(val, datetime.datetime):
        return val
    if isinstance(val, datetime.date):
        return datetime.datetime.combine(val, datetime.time.min)
    try:
        import decimal

        if isinstance(val, decimal.Decimal):
            return None
    except Exception:
        pass
    try:
        num_types = (int, float)
        try:
            num_types = (int, long, float)  # type: ignore[name-defined]
        except Exception:
            pass
        if isinstance(val, num_types) and not isinstance(val, bool):
            x = float(val)
            if x > 1e15:
                return None
            if x > 1e12:
                return datetime.datetime.fromtimestamp(x / 1000.0)
            return datetime.datetime.fromtimestamp(x)
    except Exception:
        pass
    s = to_text(val).strip()
    if not s:
        return None
    fmts = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%b-%Y",
    )
    for fmt in fmts:
        try:
            return datetime.datetime.strptime(s[:26], fmt)
        except Exception:
            continue
    return None


def _format_kingsoft_datetime_value(dt_val, ks_cat, mode):
    """按多维表日期/时间列预期格式化（默认 ISO8601；可选毫秒时间戳）。"""
    if dt_val is None:
        return None
    if not isinstance(dt_val, datetime.datetime):
        return dt_val
    mode = (mode or "iso").strip().lower()
    if ks_cat == "date":
        return dt_val.strftime("%Y-%m-%d")
    if ks_cat == "time":
        return dt_val.strftime("%H:%M:%S")
    if mode in ("epoch_ms", "ms", "timestamp_ms"):
        try:
            if sys.version_info[0] < 3:
                import time as _time

                sec = _time.mktime(dt_val.timetuple())
            else:
                sec = dt_val.timestamp()
            return int(round(sec * 1000))
        except Exception:
            return dt_val.isoformat()
    if mode in ("epoch_s", "s", "timestamp_s"):
        try:
            if sys.version_info[0] < 3:
                import time as _time

                return int(_time.mktime(dt_val.timetuple()))
            return int(dt_val.timestamp())
        except Exception:
            return dt_val.isoformat()
    try:
        return dt_val.isoformat()
    except Exception:
        return dt_val.strftime("%Y-%m-%d %H:%M:%S")


def _coerce_value_for_sheet(val, oracle_cat, ks_meta):
    """
    以多维表字段类型为准做转换。返回 (new_value, changed_bool, note_unicode)
    """
    ks_cat = (ks_meta or {}).get("category", "unknown")
    if val is None or ks_cat in ("unknown", "system", "attachment"):
        return val, False, u""
    if ks_cat == "select":
        return val, False, u""

    mode = (os.getenv("KINGSOFT_DATETIME_VALUE_MODE", "iso") or "iso").strip()

    if ks_cat == "number":
        try:
            import decimal

            if isinstance(val, decimal.Decimal):
                f = float(val)
                return f, True, u"Decimal→float"
        except Exception:
            pass
        try:
            num_types = (int, float)
            try:
                num_types = (int, long, float)  # type: ignore[name-defined]
            except Exception:
                pass
            if isinstance(val, num_types) and not isinstance(val, bool):
                return val, False, u""
        except Exception:
            pass
        s = to_text(val).strip().replace(",", "")
        if s == "":
            return None, True, u"空串→None"
        try:
            if "." in s or "e" in s.lower():
                return float(s), True, u"文本→数字"
            return int(s), True, u"文本→整数"
        except Exception:
            return val, False, u""

    if ks_cat == "bool":
        if isinstance(val, bool):
            return val, False, u""
        s = to_text(val).strip().upper()
        if s in ("1", "Y", "YES", "TRUE", "T", u"是"):
            return True, True, u"→布尔真"
        if s in ("0", "N", "NO", "FALSE", "F", u"否"):
            return False, True, u"→布尔假"
        try:
            num_types = (int, float)
            try:
                num_types = (int, long, float)  # type: ignore[name-defined]
            except Exception:
                pass
            if isinstance(val, num_types):
                return bool(int(val)), True, u"数值→布尔"
        except Exception:
            pass
        return val, False, u""

    if ks_cat in ("datetime", "date", "time"):
        dt = _parse_datetime_loose(val)
        if dt is None:
            return val, False, u""
        out = _format_kingsoft_datetime_value(dt, ks_cat, mode)
        return out, True, u"→日期/时间({0})".format(mode)

    if ks_cat == "text":
        try:
            import decimal

            if isinstance(val, decimal.Decimal):
                s = format(val, "f").rstrip("0").rstrip(".") if "." in format(val, "f") else to_text(val)
                return s, True, u"Decimal→文本"
        except Exception:
            pass
        if isinstance(val, datetime.datetime):
            s = val.isoformat()
            return s, True, u"日期时间→文本"
        if isinstance(val, datetime.date):
            s = val.strftime("%Y-%m-%d")
            return s, True, u"日期→文本"
        try:
            num_types = (int, float)
            try:
                num_types = (int, long, float)  # type: ignore[name-defined]
            except Exception:
                pass
            if isinstance(val, num_types) and not isinstance(val, bool):
                return to_text(val), True, u"数值→文本"
        except Exception:
            pass
        if not isinstance(val, (bytes, bytearray)):
            return val, False, u""
        try:
            return val.decode("utf-8", "replace"), True, u"bytes→文本"
        except Exception:
            return to_text(val), True, u"bytes→文本"

    return val, False, u""


def _record_type_coercion_entry(acc, oracle_col, sheet_key, oracle_cat, ks_meta, structural_mismatch, converted, note):
    """
    acc: dict[sheet_key] -> {oracle_columns, oracle_categories, kingsoft_raw_type, kingsoft_category, structural_mismatch, converted, notes}
    """
    if acc is None:
        return
    sk = to_text(sheet_key).strip()
    oc = to_text(oracle_col).strip()
    if not sk:
        return
    if not structural_mismatch and not converted:
        return
    ks_cat = (ks_meta or {}).get("category", "unknown")
    ks_raw = (ks_meta or {}).get("raw_type", "")
    ent = acc.get(sk)
    if not ent:
        acc[sk] = {
            "oracle_columns": [oc] if oc else [],
            "oracle_categories": [oracle_cat] if oracle_cat else [],
            "kingsoft_category": ks_cat,
            "kingsoft_raw_type": ks_raw,
            "structural_mismatch": bool(structural_mismatch),
            "converted": bool(converted),
            "notes": [note] if note else [],
        }
        return
    if oc and oc not in ent["oracle_columns"]:
        ent["oracle_columns"].append(oc)
    if oracle_cat and oracle_cat not in ent["oracle_categories"]:
        ent["oracle_categories"].append(oracle_cat)
    ent["structural_mismatch"] = ent["structural_mismatch"] or bool(structural_mismatch)
    ent["converted"] = ent["converted"] or bool(converted)
    ent["kingsoft_category"] = ks_cat or ent.get("kingsoft_category")
    ent["kingsoft_raw_type"] = ks_raw or ent.get("kingsoft_raw_type")
    if note:
        nt = to_text(note)
        if nt and nt not in ent["notes"]:
            ent["notes"].append(nt)


def _structural_type_mismatch(oracle_cat, ks_meta):
    ks_cat = (ks_meta or {}).get("category", "unknown")
    if ks_cat in ("unknown", "system"):
        return False
    o = _schema_category_compare_key(oracle_cat)
    k = _schema_category_compare_key(ks_cat)
    if ks_cat == "select" and o == "text":
        return True
    if o == "unknown":
        return False
    if o == k:
        return False
    if o == "text" and k == "select":
        return True
    if o == "binary" and k != "attachment":
        return True
    if o != k:
        return True
    return False


def _print_type_coercion_summary(acc):
    if not acc:
        _print_u(u"【{0}】类型对照汇总：无（源库列类型与多维表字段类型均一致，或无需记录）".format(to_text(_now())))
        return
    lines = []
    for sk in sorted(acc.keys(), key=lambda x: to_text(x)):
        it = acc[sk]
        ocols = u",".join([to_text(x) for x in (it.get("oracle_columns") or [])])
        ocats = u",".join([to_text(x) for x in (it.get("oracle_categories") or [])])
        notes = u";".join([to_text(x) for x in (it.get("notes") or [])])
        flag = []
        if it.get("structural_mismatch"):
            flag.append(u"结构不一致")
        if it.get("converted"):
            flag.append(u"已做转换")
        lines.append(
            u"栏位[{0}] 源列[{1}] 源类型[{2}] → 多维表类型[{3}]({4}) {5} 说明[{6}]".format(
                sk,
                ocols or u"-",
                ocats or u"-",
                to_text(it.get("kingsoft_category")),
                to_text(it.get("kingsoft_raw_type")),
                u"/".join(flag) if flag else u"-",
                notes or u"-",
            )
        )
    _print_u(u"【{0}】类型对照汇总（源库结构 vs 多维表字段，以表为准已尝试转换）：共 {1} 个栏位".format(to_text(_now()), len(lines)))
    for ln in lines:
        _print_u(ln)


def _normalize_key_for_match(s):
    """
    用于“注释/栏位名”匹配的归一化：
    - 去空白（含全角空格）
    - 去常见括号/中英文括号及少量符号
    """
    if s is None:
        return ""
    try:
        t = to_text(s)
    except Exception:
        t = str(s)
    # 去空白（含全角）
    t = re.sub(r"[\s\u3000]+", "", t)
    # 去常见符号（保守一点，避免误伤）
    t = re.sub(r"[()（）\[\]【】{}<>《》·•\-_/]", "", t)
    return t


def _auto_managed_field_block_sets():
    """返回 (精确栏位名集合, 归一化集合)，用于识别不可写入的系统字段。"""
    block_exact = set()
    block_norm = set()
    for g in SHEET_AUTO_MANAGED_FIELD_ALIAS_GROUPS:
        for a in g:
            t = to_text(a).strip()
            if t:
                block_exact.add(t)
            n = _normalize_key_for_match(a)
            if n:
                block_norm.add(n)
    return block_exact, block_norm


def _strip_auto_managed_bitable_fields(fields_dict):
    """
    创建记录不允许写入系统自动字段，否则会报 E_DBSheet_ALTER_AUTO_FIELD / CoreExecutionFailed。
    返回：(剔除后的 dict, 本次剔除的栏位名列表)
    """
    if not fields_dict:
        return fields_dict, []
    be, bn = _auto_managed_field_block_sets()
    out = {}
    dropped = []
    for k, v in fields_dict.items():
        kt = to_text(k).strip()
        if kt in be:
            dropped.append(kt)
            continue
        kn = _normalize_key_for_match(kt)
        if kn in bn:
            dropped.append(kt)
            continue
        out[k] = v
    return out, dropped


def _check_sheet_system_header_fields(display_names):
    """
    插入前检查：当前 sheet 表头是否包含常见的系统字段（按多维表「展示名/栏位名」判断）。
    display_names: 栏位展示名集合（unicode/str）
    返回：(全部存在则 True, 明细列表[(标准名, 是否找到, 实际匹配到的栏位名或 None)])
    """
    required = [
        (u"创建人", (u"创建人", u"创建者", u"录入人")),
        (u"最后修改人", (u"最后修改人", u"修改人", u"最后更新人", u"更新人")),
        (u"创建时间", (u"创建时间", u"创建日期", u"录入时间")),
        (u"最后修改时间", (u"最后修改时间", u"修改时间", u"最后更新时间", u"更新时间")),
    ]
    if not display_names:
        out = []
        for canon, _alts in required:
            out.append((canon, False, None))
        return False, out

    exact = set()
    for x in display_names:
        xt = to_text(x).strip()
        if xt:
            exact.add(xt)
    norm_to_actual = {}
    for x in exact:
        n = _normalize_key_for_match(x)
        if n and n not in norm_to_actual:
            norm_to_actual[n] = x

    def _resolve_one(candidates):
        for c in candidates:
            ct = to_text(c).strip()
            if ct in exact:
                return ct
        for c in candidates:
            n = _normalize_key_for_match(c)
            if n and n in norm_to_actual:
                return norm_to_actual[n]
        return None

    details = []
    all_ok = True
    for canon, alts in required:
        hit = _resolve_one(alts)
        ok = hit is not None
        if not ok:
            all_ok = False
        details.append((canon, ok, hit))
    return all_ok, details


def _filter_records_by_allowed_keys(records, allowed_keys):
    """
    过滤 records[].fields_value 中不在 allowed_keys 的字段，避免 field not found 导致整批失败。
    返回：(new_records, dropped_field_names_set, dropped_record_count)
    """
    if not allowed_keys:
        return records, set(), 0
    out = []
    dropped_fields = set()
    dropped_records = 0
    for r in records or []:
        if not isinstance(r, dict):
            continue
        fv = r.get("fields_value")
        if fv is None:
            continue
        try:
            obj = json.loads(fv) if not isinstance(fv, dict) else fv
        except Exception:
            # fields_value 不是合法 JSON 时不做过滤，交给服务端报错
            out.append(r)
            continue
        if not isinstance(obj, dict):
            out.append(r)
            continue
        new_obj = {}
        for k, v in obj.items():
            kk = to_text(k)
            if kk in allowed_keys:
                new_obj[k] = v
            else:
                dropped_fields.add(kk)
        if not new_obj:
            dropped_records += 1
            continue
        try:
            new_fv = json.dumps(new_obj, ensure_ascii=False)
        except Exception:
            new_fv = json.dumps({to_text(k): to_text(v) for k, v in new_obj.items()}, ensure_ascii=False)
        out.append({"fields_value": new_fv})
    return out, dropped_fields, dropped_records


def _kingsoft_name_exact_match(want, candidate):
    """多维表文件/ sheet 名：去首尾空白后大小写不敏感精确相等。"""
    w = to_text(want or u"").strip()
    c = to_text(candidate or u"").strip()
    if not w or not c:
        return False
    try:
        return w.lower() == c.lower()
    except Exception:
        return w == c


def _kingsoft_strip_dbt_file_suffix(name):
    """金山多维表文件常带 ``.dbt`` 后缀，比较逻辑名时去掉（大小写不敏感）。"""
    t = to_text(name or u"").strip()
    if not t:
        return t
    try:
        if t.lower().endswith(u".dbt"):
            return t[:-4]
    except Exception:
        pass
    return t


def _kingsoft_file_name_equivalent(want, candidate):
    """
    配置「上报资源名」与金山 **文件标题** 是否同一逻辑名：
    - 全名精确相等；或
    - 候选名为 ``{want}.dbt``；或
    - 候选去掉 ``.dbt`` 后与 ``want`` 相等。
    不把「派出所」等同于「派出所基本情况」（即使后者带 .dbt）。
    """
    w = to_text(want or u"").strip()
    c = to_text(candidate or u"").strip()
    if not w or not c:
        return False
    if _kingsoft_name_exact_match(w, c):
        return True
    c_base = _kingsoft_strip_dbt_file_suffix(c)
    if _kingsoft_name_exact_match(w, c_base):
        return True
    try:
        if c.lower() == (w + u".dbt").lower():
            return True
    except Exception:
        if c == w + u".dbt":
            return True
    return False


def _pick_best_substring_name_hit(hits, want):
    """多个「名称包含 want」时优先名称更短者（如「派出所」优于「派出所基本情况」）。"""
    if not hits:
        return None
    if len(hits) == 1:
        return hits[0]
    try:
        return min(hits, key=lambda pair: len(to_text(pair[1])))
    except Exception:
        return hits[0]


def _resolve_kingsoft_file_id(files, file_name, for_config=False):
    """
    - **目标多维表**（``for_config=False``）：默认仅精确同名；``KINGSOFT_FILE_FUZZY_MATCH=1`` 允许子串。
    - **配置 sheet**（``for_config=True``）：先精确，再子串包含（与旧版一致），多命中取名称最短。
    """
    want = to_text(file_name or u"").strip()
    if not want:
        return None, None
    allow_substr = for_config or (os.getenv("KINGSOFT_FILE_FUZZY_MATCH", "") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    exact_hits = []
    substr_hits = []
    for f in files or []:
        if not isinstance(f, dict):
            continue
        nm = f.get("name") or ""
        fid = f.get("id")
        if fid is None:
            continue
        if _kingsoft_file_name_equivalent(want, nm):
            exact_hits.append((str(fid), to_text(nm)))
        elif allow_substr and (
            want in to_text(nm) or want in _kingsoft_strip_dbt_file_suffix(nm)
        ):
            substr_hits.append((str(fid), to_text(nm)))
    if len(exact_hits) == 1:
        return exact_hits[0]
    if len(exact_hits) > 1:
        names = u", ".join([n for _, n in exact_hits[:8]])
        raise RuntimeError(
            u"文档库内存在多个与 file_name={0!r} 精确同名的文件，请改名或指定唯一名称。候选：{1}".format(want, names)
        )
    if allow_substr and substr_hits:
        picked = _pick_best_substring_name_hit(substr_hits, want)
        if picked:
            return picked
    return None, None


def _resolve_kingsoft_sheet_id(sheets, sheet_name, for_config=False):
    """
    指定 ``sheet_name`` 时默认精确匹配，不回退 ``sheets[0]``。
    ``for_config=True`` 或 ``KINGSOFT_SHEET_FUZZY_MATCH=1`` 时允许子串；多命中取名称最短。
    """
    want = to_text(sheet_name or u"").strip()
    if not want:
        if not sheets:
            return None, None
        s0 = sheets[0]
        return s0.get("id"), s0.get("name")
    allow_substr = for_config or (os.getenv("KINGSOFT_SHEET_FUZZY_MATCH", "") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    exact_hits = []
    substr_hits = []
    for s in sheets or []:
        if not isinstance(s, dict):
            continue
        sn = s.get("name") or ""
        sid = s.get("id")
        if sid is None:
            continue
        if _kingsoft_name_exact_match(want, sn):
            exact_hits.append((sid, sn))
        elif allow_substr and want in to_text(sn):
            substr_hits.append((sid, sn))
    if len(exact_hits) == 1:
        return exact_hits[0]
    if len(exact_hits) > 1:
        raise RuntimeError(u"文件内存在多个与 sheet_name={0!r} 精确同名的 sheet".format(want))
    if allow_substr and substr_hits:
        picked = _pick_best_substring_name_hit(substr_hits, want)
        if picked:
            return picked
    return None, None


def resolve_file_sheet_ids(auth, for_config=False):
    if not DOC_LIB_NAME or not FILE_NAME:
        raise RuntimeError("请配置 KINGSOFT_DOC_LIB_NAME / KINGSOFT_FILE_NAME（可选 KINGSOFT_SHEET_NAME）")

    doclibs = get_doc_lib_list(auth, keyword=DOC_LIB_NAME)
    matched = []
    for it in doclibs:
        name = (it.get("drive", {}) or {}).get("name") or it.get("name") or ""
        if to_text(DOC_LIB_NAME) in to_text(name):
            matched.append(it)
    if not matched:
        raise RuntimeError("未找到文档库：{0!r}".format(DOC_LIB_NAME))

    drive_ids = []
    for it in matched:
        did = (it.get("drive", {}) or {}).get("id")
        if did:
            drive_ids.append(str(did))
    files = get_files_keyword(auth, keyword=FILE_NAME, drive_ids=drive_ids)
    file_id, matched_file_name = _resolve_kingsoft_file_id(files, FILE_NAME, for_config=for_config)
    if not file_id:
        if for_config:
            raise RuntimeError(
                u"未找到配置多维表文件 file_name={0!r}（已尝试精确名与子串包含）。请核对入参 file_name 与金山文件标题。".format(
                    to_text(FILE_NAME)
                )
            )
        raise RuntimeError(
            u"未找到与 file_name={0!r} 等价的多维表文件（逻辑名一致即可，金山侧可为「名称.dbt」；"
            u"仍避免「派出所」误命中「派出所基本情况」）。可设 KINGSOFT_FILE_FUZZY_MATCH=1 恢复子串模糊。".format(
                to_text(FILE_NAME)
            )
        )

    schema = get_file_schema(auth, file_id=str(file_id))
    sheets = (schema.get("data", {}) or {}).get("sheets", []) or []
    if not sheets:
        raise RuntimeError("未获取到 sheets 信息")
    sheet_id, matched_sheet_name = _resolve_kingsoft_sheet_id(sheets, SHEET_NAME, for_config=for_config)
    if SHEET_NAME and not sheet_id:
        sheet_pairs = []
        for s in sheets:
            if not isinstance(s, dict):
                continue
            sid = s.get("id")
            sn = s.get("name")
            if sid is None:
                continue
            sheet_pairs.append("{0}:{1}".format(sid, to_text(sn)))
        raise RuntimeError(
            u"文件「{0}」(id={1}) 中未找到 sheet_name={2!r}；当前 sheet 列表：{3}。"
            u"目标作业须存在与「上报表名称」同名的 sheet；配置表定位可用 for_config 子串匹配。".format(
                matched_file_name or to_text(FILE_NAME),
                file_id,
                to_text(SHEET_NAME),
                u" | ".join(sheet_pairs),
            )
        )
    if not sheet_id:
        raise RuntimeError("未解析到 sheet_id")
    # 打印 sheet 列表，避免“模糊匹配到非预期 sheet”
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
            "【{0}】file_name 入参={1}，匹配到 file_id={2}, 实际文件名={3}".format(
                _now(), to_text(FILE_NAME), file_id, to_text(matched_file_name or u"")
            )
        )
        if SHEET_NAME:
            print("【{0}】sheet_name 入参={1}，匹配到 sheet_id={2}, sheet_name={3}".format(
                _now(), to_text(SHEET_NAME), sheet_id, to_text(matched_sheet_name)
            ))
    except Exception:
        pass
    return str(file_id), str(sheet_id)


def create_records(auth, file_id, sheet_id, records):
    method = "POST"
    # 严格按接口文档：POST /records/create + records[].fields_value(raw json string) + prefer_id
    path = API_PATH_FILE_RECORDS_CREATE.format(file_id=file_id, sheet_id=sheet_id)
    url = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)

    norm_records = []
    for r in records or []:
        if not isinstance(r, dict):
            continue
        fv = r.get("fields_value")
        if fv is None:
            continue
        # 统一为 unicode 文本，避免 Py2 在二次 json.dumps 时触发 ascii 编码异常
        norm_records.append({"fields_value": to_text(fv)})

    auto_strip = (os.getenv("KINGSOFT_AUTO_STRIP_INVALID_FIELDS", "1") or "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    try:
        max_strip_rounds = int(os.getenv("KINGSOFT_AUTO_STRIP_INVALID_FIELDS_MAX", "3"))
    except Exception:
        max_strip_rounds = 3
    if max_strip_rounds < 0:
        max_strip_rounds = 0

    strip_round = 0
    all_auto_stripped = []
    resp = None
    while True:
        if not norm_records:
            raise RuntimeError(
                u"创建记录失败：经自动剔除问题栏位后本批次无有效字段可写。"
                u"已剔除栏位：{0}".format(u",".join(all_auto_stripped))
            )

        body = json.dumps({"records": norm_records, "prefer_id": bool(PREFER_ID)}, ensure_ascii=False)
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
        try:
            resp = _http_request(method=method, path=path, headers=req_headers, body=body)
            break
        except Exception as _e:
            _et = to_text(_e)
            try:
                _summarize_kingsoft_http_error_json(_et)
            except Exception:
                pass
            if "HTTP 403" in _et:
                raise RuntimeError(
                    "创建记录失败（HTTP 403：无写权限/缺少 kso.dbsheet.readwrite）。"
                    "需要给应用或用户授权读写权限，或注入可写 token（KINGSOFT_ACCESS_TOKEN）。原始错误：{0}".format(_e)
                )
            can_retry = (
                auto_strip
                and strip_round < max_strip_rounds
                and "HTTP 500" in _et
                and (
                    "500410002" in _et
                    or "Invalid request" in _et
                    or "invalidCells" in _et.lower()
                    or "E_INVALID_REQUEST" in _et
                )
            )
            if can_retry:
                cells = _kingsoft_error_decode_invalid_cells(_et, echo_decode_error=False)
                new_keys = _invalid_cells_unique_field_keys(cells or [])
                if new_keys:
                    strip_round += 1
                    for k in new_keys:
                        if k not in all_auto_stripped:
                            all_auto_stripped.append(k)
                    _print_u(
                        u"【{0}】create_records 检测到 E_INVALID_REQUEST，第 {1} 轮自动剔除栏位并重试：{2}".format(
                            to_text(_now()),
                            strip_round,
                            u",".join(new_keys),
                        )
                    )
                    norm_records = _strip_norm_records_by_field_display_keys(norm_records, new_keys)
                    continue
            raise

    if all_auto_stripped:
        _print_u(
            u"【{0}】create_records 本批次曾自动剔除栏位（可写入 KINGSOFT_SKIP_FIELD_NAMES* 固定剔除）：{1}".format(
                to_text(_now()),
                u",".join(all_auto_stripped),
            )
        )

    print("【{0}】create_records 使用接口：{1}".format(_now(), path))
    try:
        resp_txt = json.dumps(resp, ensure_ascii=False) if isinstance(resp, (dict, list)) else to_text(resp)
        print("【{0}】写入完整响应(截断)：{1}".format(_now(), resp_txt[:2000]))
    except Exception:
        pass
    return resp


def create_dbsheet_fields(auth, file_id, sheet_id, fields_payload):
    """
    创建多维表字段（接口文档：POST /openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/fields）。
    fields_payload: [{"name": "创建人", "type": "CreatedBy"}, ...]
    """
    method = "POST"
    path = API_PATH_FILE_SHEET_FIELDS.format(file_id=file_id, sheet_id=sheet_id)
    url = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    body = json.dumps({"fields": fields_payload, "prefer_id": bool(PREFER_ID)}, ensure_ascii=False)
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
    try:
        resp = _http_request(method=method, path=path, headers=req_headers, body=body)
    except Exception as _e:
        if "HTTP 403" in to_text(_e):
            raise RuntimeError(
                "创建字段失败（HTTP 403：无写权限/缺少 kso.dbsheet.readwrite）。"
                "需要给应用或用户授权读写权限，或注入可写 token（KINGSOFT_ACCESS_TOKEN）。原始错误：{0}".format(_e)
            )
        raise
    print("【{0}】create_dbsheet_fields 使用接口：{1}".format(_now(), path))
    try:
        resp_txt = json.dumps(resp, ensure_ascii=False) if isinstance(resp, (dict, list)) else to_text(resp)
        print("【{0}】创建字段响应(截断)：{1}".format(_now(), resp_txt[:2000]))
    except Exception:
        pass
    code = (resp or {}).get("code") if isinstance(resp, dict) else None
    if code is not None and int(code) != 0:
        raise RuntimeError("创建字段接口返回失败：code={0} resp={1}".format(code, resp))
    return resp


def ensure_system_header_fields(auth, file_id, sheet_id):
    """
    插入数据前：检查当前 sheet 是否具备「创建人/最后修改人/创建时间/最后修改时间」；
    若有缺失且允许自动创建（默认允许），则调用创建字段接口补全，并返回最新 schema。
    环境变量 KINGSOFT_AUTO_CREATE_SYSTEM_FIELDS=false 可关闭自动创建（仅拉 schema，不 POST 字段）。
    """
    auto_create = os.getenv("KINGSOFT_AUTO_CREATE_SYSTEM_FIELDS", "true").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    schema_resp = get_file_schema(auth, file_id=str(file_id))
    display_names = _extract_allowed_field_keys(schema_resp, sheet_id=sheet_id, prefer_id=False)
    ok, details = _check_sheet_system_header_fields(display_names)
    if ok:
        _print_u(u"【{0}】系统字段（创建人/最后修改人/创建时间/最后修改时间）已齐全，跳过创建字段".format(to_text(_now())))
        return schema_resp
    if not auto_create:
        _print_u(u"【{0}】系统字段缺失但未开启自动创建（KINGSOFT_AUTO_CREATE_SYSTEM_FIELDS）".format(to_text(_now())))
        return schema_resp

    canon_to_type = {
        u"创建人": "CreatedBy",
        u"最后修改人": "LastModifiedBy",
        u"创建时间": "CreatedTime",
        u"最后修改时间": "LastModifiedTime",
    }
    to_create = []
    for canon, present, _hit in details:
        if not present and canon in canon_to_type:
            to_create.append({"name": canon, "type": canon_to_type[canon]})
    if not to_create:
        return schema_resp

    names_line = u",".join([to_text(f.get("name")) for f in to_create])
    _print_u(u"【{0}】自动创建缺失系统字段：{1}".format(to_text(_now()), names_line))
    create_dbsheet_fields(auth, file_id, sheet_id, to_create)
    try:
        time.sleep(float(os.getenv("KINGSOFT_SCHEMA_REFRESH_SLEEP", "0.6")))
    except Exception:
        time.sleep(0.6)
    return get_file_schema(auth, file_id=str(file_id))


def list_records_by_page(auth, file_id, sheet_id, page_num=1, page_size=20, view_id=""):
    """
    列举记录（用于写入后自检）。
    注意：默认不传 view_id，避免被视图筛选影响；如需按视图查看，可设置环境变量 KINGSOFT_VIEW_ID。
    """
    method = "POST"
    path = API_PATH_FILE_RECORDS_BY_PAGE.format(file_id=file_id, sheet_id=sheet_id)
    url = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)

    body_dict = {
        "page_num": int(page_num),
        "page_size": int(page_size),
        "prefer_id": bool(PREFER_ID),
    }
    if view_id:
        body_dict["view_id"] = view_id

    body = json.dumps(body_dict, ensure_ascii=False)
    # 按接口文档：application/json
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


def _list_records_by_page_with_prefer(auth, file_id, sheet_id, prefer_id=False, page_num=1, page_size=50, view_id=""):
    """
    list_by_page 的 prefer_id 可控版本（不依赖全局 PREFER_ID）。
    主要用于读取“配置 sheet”时强制按栏位展示名取值（prefer_id=false）。
    """
    method = "POST"
    path = API_PATH_FILE_RECORDS_BY_PAGE.format(file_id=file_id, sheet_id=sheet_id)
    url = "http://{0}:{1}{2}".format(API_HOST, API_PORT, path)
    body_dict = {"page_num": int(page_num), "page_size": int(page_size), "prefer_id": bool(prefer_id)}
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


def _sheet_has_any_data(auth, file_id, sheet_id):
    """
    写入前检查目标 sheet 是否已有数据：
    - 有数据：返回 (True, count_hint)
    - 无数据：返回 (False, 0)
    - 检查异常：返回 (None, None)，由上层决定是否继续写入
    """
    try:
        page_size = int(os.getenv("KINGSOFT_PRECHECK_PAGE_SIZE", "1"))
    except Exception:
        page_size = 1
    if page_size <= 0:
        page_size = 1
    view_id = (os.getenv("KINGSOFT_VIEW_ID", "") or "").strip()
    try:
        resp = list_records_by_page(
            auth,
            file_id=file_id,
            sheet_id=sheet_id,
            page_num=1,
            page_size=page_size,
            view_id=view_id,
        )
        data = (resp or {}).get("data", {}) if isinstance(resp, dict) else {}
        recs = []
        if isinstance(data, dict):
            recs = data.get("records") or data.get("items") or data.get("list") or []
        if isinstance(recs, list) and len(recs) > 0:
            return True, len(recs)

        if isinstance(data, dict):
            for k in ("total", "total_count", "totalCount", "count"):
                v = data.get(k)
                try:
                    n = int(v)
                    if n > 0:
                        return True, n
                except Exception:
                    continue
        return False, 0
    except Exception as _pre_e:
        print("【{0}】写入前 sheet 数据检查失败（将继续执行写入）：err={1}".format(_now(), _pre_e))
        return None, None


def _record_fields_dict(rec):
    """
    从 list_by_page 返回的单条 record 中提取 fields dict（key=栏位名/栏位id，取决于 prefer_id）。
    兼容：fields_value（raw json str）/ fields（dict）。
    """
    def _from_one(obj):
        if not isinstance(obj, dict):
            return {}
        # 0) camelCase 兼容
        fv = obj.get("fields_value")
        if fv is None:
            fv = obj.get("fieldsValue")
        # 1) fields_value: dict / json str / list
        if fv is not None:
            if isinstance(fv, dict):
                return fv
            # 有些返回会是 [{name/value}] 或 [{fieldName/value}] 的列表
            if isinstance(fv, list):
                out = {}
                for it in fv:
                    if not isinstance(it, dict):
                        continue
                    k = it.get("name") or it.get("title") or it.get("field_name") or it.get("fieldName") or it.get("key")
                    v = it.get("value") if "value" in it else it.get("val")
                    if k is None:
                        continue
                    kt = to_text(k).strip()
                    if kt:
                        out[kt] = v
                return out
            try:
                parsed = json.loads(fv)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        # 2) fields: dict / list
        f = obj.get("fields")
        if f is None:
            f = obj.get("fieldsMap") or obj.get("fields_map")
        if isinstance(f, dict):
            return f
        # 有些接口返回 fields 为 JSON 字符串（你日志里就是这种）
        # 注意：Py2 下类型可能是 unicode（即 text_type），需要一并兼容，否则会解析成空 dict。
        if isinstance(f, (text_type, str, bytes, bytearray)):
            try:
                ft = f.decode("utf-8", errors="replace") if isinstance(f, (bytes, bytearray)) else to_text(f)
                parsed = json.loads(ft)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        if isinstance(f, list):
            out = {}
            for it in f:
                if not isinstance(it, dict):
                    continue
                k = it.get("name") or it.get("title") or it.get("field_name") or it.get("fieldName") or it.get("key")
                v = it.get("value") if "value" in it else it.get("val")
                if k is None:
                    continue
                kt = to_text(k).strip()
                if kt:
                    out[kt] = v
            return out
        # 3) values/cells: 常见返回为 [{fieldName/name,id,value}]
        for alt in ("values", "cells", "cellValues", "cell_values"):
            vv = obj.get(alt)
            if isinstance(vv, list):
                out = {}
                for it in vv:
                    if not isinstance(it, dict):
                        continue
                    k = it.get("name") or it.get("title") or it.get("field_name") or it.get("fieldName") or it.get("key")
                    if k is None:
                        # 若只有 id，优先返回空，让上层用 prefer_id=true 读取
                        continue
                    v = it.get("value") if "value" in it else it.get("val")
                    kt = to_text(k).strip()
                    if kt:
                        out[kt] = v
                if out:
                    return out
        return {}

    if not isinstance(rec, dict):
        return {}
    # 兼容：record 外包一层
    if "record" in rec and isinstance(rec.get("record"), dict):
        got = _from_one(rec.get("record"))
        if got:
            return got
    return _from_one(rec)


def _get_cfg_field(fields, name, default=""):
    """从配置记录 fields 中取值并做 strip。"""
    if not isinstance(fields, dict):
        return default
    v = fields.get(name)
    if v is None:
        return default
    try:
        return to_text(v).strip()
    except Exception:
        try:
            return str(v).strip()
        except Exception:
            return default


def _get_cfg_field_any(fields, names, default=""):
    """
    从配置记录 fields 中按“多个候选栏位名”取值：
    - 先做精确 key 匹配
    - 再做归一化 key 匹配（去空白/括号/常见符号），兼容栏位名轻微差异
    """
    if not isinstance(fields, dict):
        return default
    if not names:
        return default
    # 1) 精确匹配
    for nm in names:
        if nm is None:
            continue
        key = to_text(nm).strip()
        if not key:
            continue
        if key in fields:
            return _get_cfg_field(fields, key, default)
    # 2) 归一化匹配
    norm_map = {}
    try:
        for k in fields.keys():
            nk = _normalize_key_for_match(k)
            if nk and nk not in norm_map:
                norm_map[nk] = k
    except Exception:
        norm_map = {}
    for nm in names:
        if nm is None:
            continue
        nn = _normalize_key_for_match(nm)
        if nn and nn in norm_map:
            return _get_cfg_field(fields, norm_map[nn], default)
    return default


def _flag_is_yes(v):
    """配置栏位“是否迁移/标识”判定为是：兼容 是/YES/true/1 等。"""
    if v is None:
        return False
    t = to_text(v).strip()
    if not t:
        return False
    u = t.lower()
    return t == u"是" or u in ("y", "yes", "true", "1", "是")


def _set_globals_for_job(database, table_name, doc_lib_name, file_name, sheet_name, field_mapping_json, migration_type, migration_time):
    """
    按单条配置记录设置本次任务所需的全局变量（保持对原逻辑侵入最小）。
    """
    global HIVE_SQL
    global DOC_LIB_NAME, FILE_NAME, SHEET_NAME
    global JOB_CONFIG
    # 复用原逻辑：get_field_mapping_from_arg 读取 JOB_CONFIG["field_mapping_json"]
    JOB_CONFIG["database"] = database
    JOB_CONFIG["table_name"] = table_name
    JOB_CONFIG["field_mapping_json"] = field_mapping_json
    JOB_CONFIG["migration_type"] = migration_type
    JOB_CONFIG["migration_time"] = migration_time

    hive_db = (to_text(database) if database is not None else "").strip()
    tbl_q = _hive_full_table_qualified(hive_db, to_text(table_name))
    ts_col = HIVE_INCREMENT_TIME_COLUMN.replace("`", "")
    # 若用户显式传 HIVE_SQL 环境变量则尊重，否则按表名与迁移模式生成
    env_sql = (os.getenv("HIVE_SQL", "") or "").strip()
    if env_sql:
        HIVE_SQL = env_sql
    else:
        mt = (to_text(migration_type) if migration_type is not None else "").strip().lower()
        mtime = (to_text(migration_time) if migration_time is not None else "").strip()
        mtime_esc = mtime.replace("'", "''")
        if mt == "add":
            HIVE_SQL = u"SELECT * FROM {0} WHERE CAST(`{1}` AS STRING) > '{2}'".format(tbl_q, _safe_hive_ident(ts_col), mtime_esc)
        elif mt == "partially":
            HIVE_SQL = u"SELECT * FROM {0} WHERE CAST(`{1}` AS STRING) <= '{2}'".format(tbl_q, _safe_hive_ident(ts_col), mtime_esc)
        else:
            HIVE_SQL = u"SELECT * FROM {0}".format(tbl_q)

    DOC_LIB_NAME = (to_text(doc_lib_name) if doc_lib_name is not None else "").strip()
    FILE_NAME = (to_text(file_name) if file_name is not None else "").strip()
    SHEET_NAME = (to_text(sheet_name) if sheet_name is not None else "").strip()

def _iter_hive_rows():
    """
    以批次返回 Hive 数据（**仅 PyHive / HS2**，已移除 beeline）。
    每次 yield (columns, rows, col_comments, col_oracle_cats)。
    环境变量 ``HIVE_SQL_FETCH_MODE`` 若设为 beeline/auto 等，仍 **只走 PyHive**（兼容旧调度，忽略 beeline 取值）。
    无法 ``import pyhive``/``thrift`` 时抛出 ``RuntimeError``。
    写入多维表前默认经 ``_iter_hive_rows_for_write()`` 先物化（见 ``HIVE_MATERIALIZE_BEFORE_KINGSOFT_INSERT``）。
    col_comments：当前未从 Hive 拉取列注释，固定为空 dict。
    """
    if not _hive_pyhive_import_available():
        raise RuntimeError(
            u"本脚本已移除 beeline，仅支持 PyHive 拉数；当前 Python 无法加载 PyHive/Thrift。"
            u"import_err={0}".format(to_text(_LAST_PYHIVE_IMPORT_ERROR or u"(无)"))
        )
    for chunk in _iter_hive_rows_pyhive():
        yield chunk


def _materialize_hive_rows():
    """
    将 ``_iter_hive_rows()`` 的全部批次合并为单一内存对象：
    ``{columns, rows, col_comments, col_oracle_cats, chunks_merged}``。
    供写入多维表前固定 Hive 结果集，保证行序与后续 ``_row_to_record`` 循环 1:1。
    """
    columns = None
    merged_rows = []
    col_comments = {}
    col_oracle_cats = None
    n_chunks = 0
    for cols, rows, cc, co in _iter_hive_rows():
        n_chunks += 1
        if columns is None:
            columns = list(cols) if cols is not None else []
            col_comments = cc if isinstance(cc, dict) else {}
            col_oracle_cats = co
        else:
            if list(cols or []) != list(columns or []):
                raise RuntimeError(
                    u"HIVE 分批结果列名/列序不一致（第 {0} 批与首段），中止以防多维表错位写入".format(n_chunks)
                )
        if rows:
            merged_rows.extend(rows)
    if columns is None:
        columns = []
        col_comments = {}
        col_oracle_cats = []
    elif col_oracle_cats is None:
        col_oracle_cats = [u"text"] * len(columns)
    return {
        u"columns": columns,
        u"rows": merged_rows,
        u"col_comments": col_comments,
        u"col_oracle_cats": col_oracle_cats,
        u"chunks_merged": n_chunks,
    }


def _iter_hive_rows_for_write():
    """
    写入多维表用的 Hive 行迭代：默认 **先物化**（见 ``HIVE_MATERIALIZE_BEFORE_KINGSOFT_INSERT``），
    再单次 yield 整块 ``(columns, rows, ...)``；关闭物化时与 ``_iter_hive_rows()`` 等价。
    """
    _mat = (os.getenv("HIVE_MATERIALIZE_BEFORE_KINGSOFT_INSERT", "0") or "").strip().lower()
    if _mat in ("0", "false", "no", "off"):
        for chunk in _iter_hive_rows():
            yield chunk
        return
    obj = _materialize_hive_rows()
    try:
        _print_u(
            u"【{0}】Hive 查询结果已物化到内存：合并上游批次数={1}，列数={2}，逻辑行数={3}；"
            u"随后再按此行集循环 ``_row_to_record`` 写入多维表（1:1）。"
            u"需全量物化时可设 HIVE_MATERIALIZE_BEFORE_KINGSOFT_INSERT=1（默认边拉边写）。".format(
                to_text(_now()),
                int(obj.get(u"chunks_merged") or 0),
                len(obj.get(u"columns") or []),
                len(obj.get(u"rows") or []),
            )
        )
    except Exception:
        pass
    yield obj[u"columns"], obj[u"rows"], obj[u"col_comments"], obj[u"col_oracle_cats"]


def _load_field_mapping():
    mapping = {}

    # 1) 环境变量映射（可选）
    if FIELD_MAPPING_JSON:
        try:
            obj = json.loads(FIELD_MAPPING_JSON)
            if isinstance(obj, dict):
                mapping.update({str(k): str(v) for k, v in obj.items()})
            else:
                raise RuntimeError("KINGSOFT_FIELD_MAPPING_JSON 必须是 JSON 对象（key/value）")
        except Exception as e:
            raise RuntimeError("KINGSOFT_FIELD_MAPPING_JSON 不是合法 JSON 对象：err={0}".format(e))

    # 2) 第6位入参字段映射（可选，覆盖/补充）
    arg_mapping = get_field_mapping_from_arg()
    if arg_mapping:
        mapping.update(arg_mapping)

    return mapping


def _apply_mapping_from_kingsoft_title_paren_codes(schema_resp, sheet_id, mapping):
    """
    多维表栏位展示名常为「中文(字段代码)」，创建记录接口要求 fields 的 key 为完整展示名；
    Oracle 列名多为括号内英文代码（如 XM、ZFDW）。当 Oracle 注释与展示名对不上时，
    用括号内代码与列名做不区分大小写匹配，自动补齐 mapping（不覆盖已有映射）。
    返回：(新增映射条数, 样例列表最多10条, 冲突说明列表最多5条)
    """
    if not schema_resp or sheet_id is None:
        return 0, [], []
    # 非贪婪左侧 + 括号内允许字母数字下划线（与常见 Oracle 列名一致）
    pat = re.compile(r"^\s*(.+?)\s*[\(（]\s*([A-Za-z0-9_]+)\s*[\)）]\s*$")
    added = 0
    samples = []
    conflicts = []
    code_to_target = {}
    try:
        for wk, _f in _iter_sheet_field_entries(schema_resp, sheet_id, prefer_id=False):
            wkt = to_text(wk).strip()
            if not wkt:
                continue
            m = pat.match(wkt)
            if not m:
                continue
            code = (m.group(2) or "").strip()
            if not code:
                continue
            key_norm = code.lower()
            prev = code_to_target.get(key_norm)
            if prev and prev != wkt:
                if len(conflicts) < 5:
                    conflicts.append(u"{0} 与 {1} 括号代码相同({2})".format(prev, wkt, code))
                continue
            code_to_target[key_norm] = wkt
    except Exception:
        return 0, [], []

    def _has_oracle_key(k):
        if not k:
            return False
        if mapping.get(k):
            return True
        try:
            if mapping.get(to_text(k).lower()):
                return True
        except Exception:
            pass
        try:
            if mapping.get(str(k).lower()):
                return True
        except Exception:
            pass
        try:
            lk = to_text(k).lower()
            for mk in mapping.keys():
                if _hive_col_unqualified_name(mk).lower() == lk:
                    return True
        except Exception:
            pass
        return False

    for key_norm, target_title in code_to_target.items():
        if not key_norm:
            continue
        keys_to_set = []
        try:
            lo = key_norm.lower()
            hi = key_norm.upper()
            keys_to_set = list({lo, hi})
        except Exception:
            keys_to_set = [key_norm]
        blocked = False
        for k in keys_to_set:
            if _has_oracle_key(k):
                blocked = True
                break
        if blocked:
            continue
        for k in keys_to_set:
            mapping[k] = target_title
        added += 1
        if len(samples) < 10:
            samples.append(u"{0}->{1}".format(to_text(key_norm).upper(), to_text(target_title)))
    return added, samples, conflicts


def _mapping_lookup_hive_to_sheet(mapping, hive_col):
    """
    将 Hive ``DESCRIBE`` 列名解析为多维表 ``fields`` 的写入 key（与 ``mapping`` 中目标栏位名一致）。
    依次匹配：原名、小写、去反引号、**去库表限定名**（``db.tbl.num`` → ``num``）、去空白规范化名，
    从而 ``num`` / ``NUM`` / ``lgqyqtz_v1.num`` 均能命中括号代码自动映射到 ``序号(NUM)`` 等展示名。
    无命中返回 ``None``，由调用方回退为直写列名。
    """
    if not mapping:
        return None
    col = to_text(hive_col).strip().strip(u"`")
    if not col:
        return None
    m = mapping
    seen = set()
    out_list = []

    def _add(x):
        xt = to_text(x).strip()
        if not xt:
            return
        kl = xt.lower()
        if kl in seen:
            return
        seen.add(kl)
        out_list.append(xt)

    _add(col)
    _add(col.lower())
    uq = _hive_col_unqualified_name(hive_col)
    _add(uq)
    _add(to_text(uq).lower())
    try:
        c_norm = re.sub(r"[\s\u3000]+", "", col)
        if c_norm:
            _add(c_norm)
            _add(c_norm.lower())
    except Exception:
        pass
    try:
        uqn = re.sub(r"[\s\u3000]+", "", uq)
        if uqn:
            _add(uqn)
            _add(uqn.lower())
    except Exception:
        pass
    for k in out_list:
        v = m.get(k)
        if v:
            return to_text(v)
    return None


def _row_to_record(columns, row, mapping, oracle_categories=None, sheet_type_map=None, coercion_accumulator=None):
    """
    将单行 Hive 结果转为一条多维表创建记录载荷（单 dict，含 fields_value）。
    约定：不在此函数内拆分/展开为多行；一行输入至多对应一条输出。
    列名→栏位：``_mapping_lookup_hive_to_sheet`` 支持 ``*.num`` 与 ``num`` 等价；默认再将 Hive ``num`` 列
    覆盖写入括号代码 ``NUM`` 映射到的栏位（如 ``序号(NUM)``）。
    """
    fields = {}
    for i, col in enumerate(columns):
        key = _mapping_lookup_hive_to_sheet(mapping, col)
        if not key:
            key = col
        val = row[i] if i < len(row) else None
        if hasattr(val, "read") and callable(val.read):
            try:
                val = val.read()
            except Exception:
                # 统一走 to_text，避免 Py2 下 str(unicode) 触发 ascii 编码异常
                val = to_text(val)
        if isinstance(val, (bytes, bytearray)):
            try:
                val = val.decode("utf-8", errors="replace")
            except Exception:
                val = to_text(val)
        if val is None:
            continue
        key_t = to_text(key)
        o_cat = "unknown"
        try:
            if oracle_categories and i < len(oracle_categories):
                o_cat = oracle_categories[i] or "unknown"
        except Exception:
            o_cat = "unknown"
        ks_meta = None
        if sheet_type_map:
            ks_meta = sheet_type_map.get(key_t.strip())
        new_val = val
        changed = False
        note = u""
        if ks_meta:
            new_val, changed, note = _coerce_value_for_sheet(val, o_cat, ks_meta)
            sm = _structural_type_mismatch(o_cat, ks_meta)
            ks_cat = (ks_meta or {}).get("category", "unknown")
            if ks_cat == "select" and sm:
                note = u"单选/多选需传选项 id，未自动转换"
            _record_type_coercion_entry(
                coercion_accumulator,
                to_text(col),
                key_t,
                o_cat,
                ks_meta,
                sm,
                changed,
                note,
            )
        # Py2 下 str(unicode) 会走 ascii 编码导致 UnicodeEncodeError，这里统一保持为 unicode key
        fields[key_t] = new_val

    # 强制：Hive 列 ``num`` / ``*.num`` 的值写入 ``num`` 括号代码映射到的多维表栏位（如 ``序号(NUM)``），避免与其它栏位误绑
    try:
        _stn = (os.getenv("KINGSOFT_STRICT_HIVE_NUM_TO_PAREN_NUM_FIELD", "1") or "").strip().lower()
        if mapping and _stn not in ("0", "false", "no", "off"):
            num_kt = _mapping_lookup_hive_to_sheet(mapping, u"num")
            if num_kt:
                key_t = to_text(num_kt).strip()
                for i, col in enumerate(columns):
                    if _hive_col_unqualified_name(col).lower() != u"num":
                        continue
                    val = row[i] if i < len(row) else None
                    if hasattr(val, "read") and callable(val.read):
                        try:
                            val = val.read()
                        except Exception:
                            val = to_text(val)
                    if isinstance(val, (bytes, bytearray)):
                        try:
                            val = val.decode("utf-8", errors="replace")
                        except Exception:
                            val = to_text(val)
                    if val is None:
                        try:
                            fields.pop(key_t, None)
                        except Exception:
                            pass
                    else:
                        o_cat = "unknown"
                        try:
                            if oracle_categories and i < len(oracle_categories):
                                o_cat = oracle_categories[i] or "unknown"
                        except Exception:
                            o_cat = "unknown"
                        new_val = val
                        ks_meta = None
                        if sheet_type_map:
                            ks_meta = sheet_type_map.get(key_t)
                        if ks_meta:
                            new_val, changed, note = _coerce_value_for_sheet(val, o_cat, ks_meta)
                            sm = _structural_type_mismatch(o_cat, ks_meta)
                            ks_cat = (ks_meta or {}).get("category", "unknown")
                            if ks_cat == "select" and sm:
                                note = u"单选/多选需传选项 id，未自动转换"
                            _record_type_coercion_entry(
                                coercion_accumulator,
                                u"num",
                                key_t,
                                o_cat,
                                ks_meta,
                                sm,
                                changed,
                                note,
                            )
                        fields[key_t] = new_val
                    break
    except Exception:
        pass

    fields, _dropped_auto = _strip_auto_managed_bitable_fields(fields)
    global _AUTO_MANAGED_STRIP_LOGGED
    if _dropped_auto and not _AUTO_MANAGED_STRIP_LOGGED:
        _AUTO_MANAGED_STRIP_LOGGED = True
        try:
            uniq = sorted(set([to_text(x) for x in _dropped_auto]))
            _print_u(
                u"【{0}】已剔除系统自动字段（禁止通过创建记录写入，避免 E_DBSheet_ALTER_AUTO_FIELD）：{1}".format(
                    to_text(_now()), u",".join(uniq)
                )
            )
        except Exception:
            pass

    # 用户配置剔除（单选/多选需选项 id、公式/关联等导致 E_INVALID_REQUEST 时可配置跳过）
    skip_names = _load_user_skip_field_names()
    if skip_names:
        be_u, bn_u = _build_block_sets_from_names(skip_names)
        fields, _dropped_u = _strip_fields_by_block_sets(fields, be_u, bn_u)
        global _USER_SKIP_STRIP_LOGGED
        if _dropped_u and not _USER_SKIP_STRIP_LOGGED:
            _USER_SKIP_STRIP_LOGGED = True
            try:
                uniq_u = sorted(set([to_text(x) for x in _dropped_u]))
                _print_u(
                    u"【{0}】已按环境变量剔除栏位（KINGSOFT_SKIP_FIELD_NAMES*）：{1}".format(
                        to_text(_now()), u",".join(uniq_u)
                    )
                )
            except Exception:
                pass

    # 创建记录接口要求 fields/fields_value 为 raw json 字符串（不是对象）
    try:
        fields_json = json.dumps(fields, ensure_ascii=False)
    except Exception:
        # 兜底：保证是字符串，避免整批失败
        fields_json = json.dumps({k: to_text(v) for k, v in fields.items()}, ensure_ascii=False)
    # 按文档：records 内最关键字段是 fields_value（raw json 字符串）
    # 为避免服务端严格校验导致丢弃，这里只发送 fields_value。
    return {"fields_value": to_text(fields_json)}


def _code_fingerprint():
    """
    用于判断调度是否执行到最新脚本版本（不依赖文件名）。
    """
    try:
        import hashlib as _hashlib

        # 取 _row_to_record 的关键片段做指纹
        s = "row_to_record_key_line=fields[to_text(key)]"
        return _hashlib.md5(s.encode("utf-8")).hexdigest()
    except Exception:
        return "na"


def _run_single_job(auth, start_ts=None):
    """
    执行单条迁移作业（Hive -> 目标多维表 sheet）。
    依赖全局变量已被 _set_globals_for_job 设置：
    - HIVE_SQL
    - DOC_LIB_NAME / FILE_NAME / SHEET_NAME
    - JOB_CONFIG["database"] / JOB_CONFIG["table_name"] / JOB_CONFIG["field_mapping_json"]
    """
    start_ts = start_ts or time.time()
    _fmj = u"有值" if (to_text(JOB_CONFIG.get("field_mapping_json")) or u"").strip() else u"无"
    _mt = (to_text(JOB_CONFIG.get("migration_type")) or u"").strip().lower()
    msg_u = u"【{0}】任务参数：database={1}, table_name={2}, doc_lib_name={3}, file_name={4}, sheet_name={5}, field_mapping_json={6}, migration_type={7}, migration_time={8}".format(
        to_text(_now()),
        to_text(JOB_CONFIG.get("database")),
        to_text(JOB_CONFIG.get("table_name")),
        to_text(DOC_LIB_NAME),
        to_text(FILE_NAME),
        to_text(SHEET_NAME),
        to_text(_fmj),
        to_text(JOB_CONFIG.get("migration_type")),
        to_text(JOB_CONFIG.get("migration_time")),
    )
    try:
        sys.stdout.write(msg_u.encode("utf-8") + "\n")
    except Exception:
        print(msg_u)
    print("【{0}】Hive SQL（预览）={1}".format(_now(), to_text(HIVE_SQL)))

    file_id, sheet_id = resolve_file_sheet_ids(auth)
    print(
        "【{0}】目标多维表定位完成：file_id={1}, sheet_id={2}, prefer_id={3}".format(
            _now(), file_id, sheet_id, PREFER_ID
        )
    )
    if _mt == "add":
        print("【{0}】增量迁移模式：写入策略=append_only（仅新增到原有数据后，不清空、不覆盖历史数据）".format(_now()))

    mapping = _load_field_mapping()
    allowed_keys = set()
    allowed_keys_norm_map = {}
    schema_resp = None
    try:
        schema_resp = ensure_system_header_fields(auth, file_id, sheet_id)
        allowed_keys = _extract_allowed_field_keys(schema_resp, sheet_id=sheet_id, prefer_id=bool(PREFER_ID))
        try:
            for ak in allowed_keys:
                n = _normalize_key_for_match(ak)
                if n and n not in allowed_keys_norm_map:
                    allowed_keys_norm_map[n] = ak
        except Exception:
            pass
        if allowed_keys:
            print("【{0}】schema 字段校验启用：allowed_keys_count={1}（prefer_id={2}）".format(
                _now(), len(list(allowed_keys)), PREFER_ID
            ))
            try:
                sample = ",".join(sorted([to_text(x) for x in list(allowed_keys)])[:10])
                _print_u(u"【{0}】schema 字段样例（前10）：{1}".format(to_text(_now()), to_text(sample)))
            except Exception:
                pass
        else:
            print("【{0}】schema 字段校验未启用：未解析到字段列表（将不做过滤）".format(_now()))
    except Exception as _se:
        print("【{0}】schema 拉取/解析失败（将不做过滤）：err={1}".format(_now(), _se))

    # 插入前：检查系统表头
    try:
        display_names = set()
        if schema_resp:
            display_names = _extract_allowed_field_keys(schema_resp, sheet_id=sheet_id, prefer_id=False)
        hdr_ok, hdr_details = _check_sheet_system_header_fields(display_names)
        line_parts = []
        for canon, ok, hit in hdr_details:
            if ok:
                line_parts.append(u"{0}:OK({1})".format(canon, to_text(hit)))
            else:
                line_parts.append(u"{0}:缺失".format(canon))
        _print_u(
            u"【{0}】多维表表头检查（系统字段）：全部存在={1} 明细={2}".format(
                to_text(_now()), u"是" if hdr_ok else u"否", u" | ".join(line_parts)
            )
        )
        require_hdr = os.getenv("KINGSOFT_REQUIRE_SYSTEM_HEADER_FIELDS", "").strip().lower() in ("1", "true", "yes")
        if require_hdr and not hdr_ok:
            miss = [to_text(c) for c, ok, _h in hdr_details if not ok]
            raise RuntimeError(u"缺少必选系统表头字段：{0}".format(u",".join(miss)))
    except RuntimeError:
        raise
    except Exception as _he:
        print("【{0}】表头检查异常（忽略继续）：err={1}".format(_now(), _he))

    # 写入前幂等检查：
    # - migrationType=add：始终执行增量追加，不因目标已有数据而跳过
    # - migrationType=all/partially：若目标已有数据则跳过，避免重复导入
    skip_if_target_has_data = (_mt in ("all", "partially", ""))
    has_data, data_hint = _sheet_has_any_data(auth, file_id=file_id, sheet_id=sheet_id)
    if has_data is True:
        if skip_if_target_has_data:
            print(
                "【{0}】检测到目标 sheet 已有数据（count_hint={1}），migrationType={2} 命中跳过策略：本次跳过插入。".format(
                    _now(), data_hint, (_mt or "all")
                )
            )
            cost = round(time.time() - start_ts, 2)
            print("【{0}】单作业跳过完成，耗时 {1} 秒。".format(_now(), cost))
            return
        print(
            "【{0}】检测到目标 sheet 已有数据（count_hint={1}），migrationType=add：继续执行增量追加插入。".format(
                    _now(), data_hint
            )
        )
    elif has_data is False:
        print("【{0}】写入前检查：目标 sheet 当前无数据，将继续执行插入。".format(_now()))

    total_rows = 0
    batch = []
    sent_batches = 0
    prepared_records = 0
    filtered_batches = 0
    type_coercion_log = {}
    type_coercion_on = (os.getenv("KINGSOFT_TYPE_COERCION", "true") or "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    sheet_field_types = {}
    if type_coercion_on and schema_resp:
        try:
            sheet_field_types = _extract_sheet_field_type_map(
                schema_resp, sheet_id=sheet_id, prefer_id=bool(PREFER_ID)
            )
        except Exception as _tm_e:
            print("【{0}】多维表字段类型映射解析失败（将不做类型转换）：err={1}".format(_now(), _tm_e))
            sheet_field_types = {}
    if type_coercion_on and sheet_field_types:
        print("【{0}】已启用 Hive→多维表类型转换（以多维表类型为准），字段类型条目数={1}".format(
            _now(), len(list(sheet_field_types.keys()))
        ))

    print("【{0}】开始写入多维表数据".format(_now()))
    try:
        _base_bs = max(1, int(RECORDS_BATCH_SIZE))
    except Exception:
        _base_bs = 200
    _one_http = (os.getenv("KINGSOFT_ONE_RECORD_PER_HTTP_REQUEST", "") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    write_batch_size = 1 if _one_http else _base_bs
    try:
        _print_u(
            u"【{0}】写入约定：每个 Hive 行最多生成 1 条多维表记录（不拆行）；单次 create 合并条数 write_batch_size={1}（"
            u"KINGSOFT_RECORDS_BATCH_SIZE={2}，KINGSOFT_ONE_RECORD_PER_HTTP_REQUEST={3}）".format(
                to_text(_now()),
                write_batch_size,
                RECORDS_BATCH_SIZE,
                u"1" if _one_http else u"0",
            )
        )
    except Exception:
        pass
    auto_mapping_built = False
    for cols, rows, col_comments, col_oracle_cats in _iter_hive_rows_for_write():
        if not auto_mapping_built:
            try:
                if allowed_keys and isinstance(col_comments, dict) and col_comments:
                    added = 0
                    added_samples = []
                    debug_samples = []
                    for c in cols or []:
                        if c is None:
                            continue
                        c_txt = to_text(c)
                        if mapping.get(c_txt) or mapping.get(c_txt.lower()):
                            continue
                        cm = col_comments.get(c_txt) or col_comments.get(c_txt.upper()) or col_comments.get(c_txt.lower())
                        cm_txt = (to_text(cm) or u"").strip() if cm is not None else u""
                        if not cm_txt:
                            continue
                        target_field = None
                        if cm_txt in allowed_keys:
                            target_field = cm_txt
                        else:
                            n = _normalize_key_for_match(cm_txt)
                            target_field = allowed_keys_norm_map.get(n)
                        if len(debug_samples) < 10:
                            try:
                                debug_samples.append("{0} 注释={1} 命中={2}".format(
                                    c_txt, cm_txt, to_text(target_field) if target_field else "None"
                                ))
                            except Exception:
                                pass
                        if target_field:
                            mapping[c_txt] = target_field
                            mapping[c_txt.lower()] = target_field
                            c_norm = re.sub(r"[\s\u3000]+", "", c_txt)
                            if c_norm:
                                mapping[c_norm] = target_field
                                mapping[c_norm.lower()] = target_field
                            added += 1
                            if len(added_samples) < 10:
                                try:
                                    added_samples.append("{0}->{1}".format(c_txt, to_text(target_field)))
                                except Exception:
                                    pass
                    if added:
                        print("【{0}】按源库字段注释自动匹配多维表栏位：新增映射数={1}".format(_now(), added))
                        if added_samples:
                            _print_u(
                                u"【{0}】自动映射样例（前10）：{1}".format(
                                    to_text(_now()), to_text(" | ".join(added_samples))
                                )
                            )
                    if debug_samples:
                        _print_u(u"【{0}】注释匹配探测（前10列）：{1}".format(to_text(_now()), to_text(" || ".join(debug_samples))))
                if allowed_keys and schema_resp:
                    try:
                        code_added, code_samples, code_conf = _apply_mapping_from_kingsoft_title_paren_codes(
                            schema_resp, sheet_id, mapping
                        )
                        if code_added:
                            print(
                                "【{0}】按多维表栏位括号内代码自动匹配源列名：新增映射数={1}".format(
                                    _now(), code_added
                                )
                            )
                            if code_samples:
                                _print_u(
                                    u"【{0}】括号代码映射样例（前10）：{1}".format(
                                        to_text(_now()), to_text(" | ".join(code_samples))
                                    )
                                )
                            if code_conf:
                                _print_u(
                                    u"【{0}】括号代码映射冲突（忽略后者，最多5条）：{1}".format(
                                        to_text(_now()), to_text(" || ".join(code_conf))
                                    )
                                )
                    except Exception as _cm2:
                        print("【{0}】括号代码自动匹配失败（忽略继续）：err={1}".format(_now(), _cm2))
            except Exception as _am:
                print("【{0}】按注释自动匹配失败（将仅使用显式映射/列名直写）：err={1}".format(_now(), _am))
            auto_mapping_built = True

        for row in rows:
            rec = _row_to_record(
                cols,
                row,
                mapping,
                oracle_categories=col_oracle_cats if type_coercion_on else None,
                sheet_type_map=sheet_field_types if type_coercion_on else None,
                coercion_accumulator=type_coercion_log if type_coercion_on else None,
            )
            if rec.get("fields_value") or rec.get("fields"):
                batch.append(rec)
                prepared_records += 1
            total_rows += 1
            if len(batch) >= write_batch_size:
                send_batch = batch
                if allowed_keys:
                    send_batch, dropped_fields, dropped_records = _filter_records_by_allowed_keys(batch, allowed_keys)
                    filtered_batches += 1
                    if dropped_fields:
                        try:
                            ds = ",".join(sorted(list(dropped_fields))[:30])
                        except Exception:
                            ds = "?"
                        print("【{0}】字段过滤：dropped_fields_count={1} sample={2}".format(
                            _now(), len(list(dropped_fields)), ds
                        ))
                    if dropped_records:
                        print("【{0}】字段过滤：dropped_empty_records={1}".format(_now(), dropped_records))
                if not send_batch:
                    print("【{0}】本批次过滤后无可写字段：跳过写入（原batch={1}）".format(_now(), len(batch)))
                    batch = []
                    continue
                resp = create_records(auth, file_id=file_id, sheet_id=sheet_id, records=send_batch)
                sent_batches += 1
                print(
                    "【{0}】已写入 batch={1}，累计抽取行数={2}，响应code={3}".format(
                        _now(), len(send_batch), total_rows, (resp or {}).get("code")
                    )
                )
                batch = []

    if batch:
        send_batch = batch
        if allowed_keys:
            send_batch, dropped_fields, dropped_records = _filter_records_by_allowed_keys(batch, allowed_keys)
            filtered_batches += 1
            if dropped_fields:
                try:
                    ds = ",".join(sorted(list(dropped_fields))[:30])
                except Exception:
                    ds = "?"
                print("【{0}】字段过滤：dropped_fields_count={1} sample={2}".format(
                    _now(), len(list(dropped_fields)), ds
                ))
            if dropped_records:
                print("【{0}】字段过滤：dropped_empty_records={1}".format(_now(), dropped_records))
        if not send_batch:
            print("【{0}】最后一批过滤后无可写字段：跳过写入（原batch={1}）".format(_now(), len(batch)))
        else:
            resp = create_records(auth, file_id=file_id, sheet_id=sheet_id, records=send_batch)
            sent_batches += 1
            print(
                "【{0}】已写入 batch={1}（最后一批），累计抽取行数={2}，响应code={3}".format(
                    _now(), len(send_batch), total_rows, (resp or {}).get("code")
                )
            )

    insert_end = _now()
    print("【{0}】写入结束时间：{0}".format(insert_end))
    print("【{0}】本次写入批次数={1}（write_batch_size={2}）".format(_now(), sent_batches, write_batch_size))
    if allowed_keys:
        print("【{0}】本次过滤批次数={1}（schema 字段校验）".format(_now(), filtered_batches))
    print("【{0}】本次准备写入记录数={1}，Hive累计抽取行数={2}".format(_now(), prepared_records, total_rows))

    # 写入后自检
    try:
        verify = os.getenv("KINGSOFT_VERIFY_AFTER_INSERT", "true").lower() == "true"
        view_id = (os.getenv("KINGSOFT_VIEW_ID", "") or "").strip()
        if verify:
            vr = list_records_by_page(auth, file_id=file_id, sheet_id=sheet_id, page_num=1, page_size=5, view_id=view_id)
            data = (vr or {}).get("data", {}) if isinstance(vr, dict) else {}
            recs = []
            if isinstance(data, dict):
                recs = data.get("records") or data.get("items") or data.get("list") or []
            if not isinstance(recs, list):
                recs = []
            print("【{0}】写入后自检：list_by_page(page_size=5, view_id={1}) 返回记录数={2}".format(
                _now(), (view_id or "''"), len(recs)
            ))
            try:
                keys = []
                if isinstance(data, dict):
                    keys = sorted(list(data.keys()))
                print("【{0}】自检响应 data keys：{1}".format(_now(), ",".join([to_text(k) for k in keys])))
            except Exception:
                pass
    except Exception as _ve:
        print("【{0}】写入后自检失败（不影响主流程）：err={1}".format(_now(), _ve))

    cost = round(time.time() - start_ts, 2)
    if type_coercion_on:
        _print_type_coercion_summary(type_coercion_log)
    else:
        _print_u(u"【{0}】类型转换与对照汇总已关闭（KINGSOFT_TYPE_COERCION=false）".format(to_text(_now())))
    print("【{0}】单作业完成，耗时 {1} 秒，Hive累计抽取行数={2}".format(_now(), cost, total_rows))


def _maybe_reexec_self_with_python3():
    """
    调度误用 ``python``（Py2）启动时，PyHive/thrift 常在 **python3** 环境；若检测到 python3 可 import，则 **exec 换解释器** 重启本进程（参数不变）。
    关闭：KINGSOFT_AUTO_REEXEC_PYTHON3=0
    """
    if sys.version_info[0] >= 3:
        return
    if (os.getenv("KINGSOFT_AUTO_REEXEC_PYTHON3", "1") or "").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        return
    py3 = None
    try:
        import shutil

        if getattr(shutil, "which", None):
            py3 = shutil.which("python3")
    except Exception:
        py3 = None
    if not py3:
        for cand in (u"/usr/bin/python3", u"/usr/local/bin/python3"):
            try:
                if os.path.isfile(cand):
                    py3 = cand
                    break
            except Exception:
                pass
    if not py3:
        return
    py3_t = to_text(py3).strip()
    ex_t = to_text(sys.executable).strip()
    try:
        if os.path.abspath(py3_t) == os.path.abspath(ex_t):
            return
    except Exception:
        if py3_t == ex_t:
            return
    try:
        chk = [
            py3_t,
            u"-c",
            u"import sys; assert sys.version_info[0] >= 3; import thrift.Thrift; from pyhive import hive",
        ]
        p = subprocess.Popen(
            chk,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )
        _o, _e = p.communicate()
        if p.returncode != 0:
            return
    except Exception:
        return
    try:
        _print_u(
            u"【{0}】当前为 Python2（{1}），已检测到 {2} 可加载 thrift/pyhive；"
            u"自动用 python3 重新执行本脚本（关闭请设 KINGSOFT_AUTO_REEXEC_PYTHON3=0）".format(
                to_text(_now()), ex_t, py3_t
            )
        )
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
    except Exception:
        pass
    try:
        argv0 = sys.argv[0] if sys.argv else __file__
        new_argv = [py3_t, to_text(argv0)] + [to_text(x) for x in sys.argv[1:]]
        os.execvp(new_argv[0], new_argv)
    except Exception as _ex:
        try:
            _print_u(u"【{0}】exec 切换 python3 失败：{1}".format(to_text(_now()), to_text(_ex)[:500]))
        except Exception:
            pass


def main():
    start = time.time()
    try:
        _maybe_reexec_self_with_python3()
    except Exception as _rex:
        try:
            _print_u(u"【{0}】自动切换 python3 前置检查异常（忽略）：{1}".format(to_text(_now()), to_text(_rex)[:400]))
        except Exception:
            pass
    print("=" * 80)
    print("【{0}】作业开始：Hive -> 金山多维表（创建记录）".format(_now()))
    print("=" * 80)
    try:
        print("【{0}】SCRIPT_BUILD_ID={1}，__file__={2}，fp={3}".format(_now(), SCRIPT_BUILD_ID, __file__, _code_fingerprint()))
    except Exception:
        pass
    try:
        if sys.version_info[0] < 3:
            _print_u(
                u"【{0}】重要：当前以 **Python 2** 运行（sys.executable={1}）。"
                u"系统默认 python 常为 2.7 且无 ``pip`` 模块，与 ``python3 -m pip`` 不是同一环境。"
                u"Hive 仅支持 PyHive/HS2，请调度改为：``python3 …/lgbs-data-insert-kingsoft-prod-all.py …``；"
                u"并确保同一解释器可 ``import pyhive``、``thrift``。".format(
                    to_text(_now()), to_text(sys.executable)
                )
            )
    except Exception:
        pass
    try:
        _log_runtime_host_and_pip_diags()
    except Exception as _rd_e:
        try:
            _print_u(u"【{0}】运行环境诊断打印异常（忽略）：{1}".format(to_text(_now()), to_text(_rd_e)[:500]))
        except Exception:
            pass
    msg_u = u"【{0}】入参（定位配置sheet）：doc_lib_name={1}, file_name={2}, sheet_name={3}".format(
        to_text(_now()),
        to_text(JOB_CONFIG.get("config_doc_lib_name")),
        to_text(JOB_CONFIG.get("config_file_name")),
        to_text(JOB_CONFIG.get("config_sheet_name")),
    )
    try:
        # py2: 以 utf-8 输出，避免默认 ascii 编码异常
        sys.stdout.write(msg_u.encode("utf-8") + "\n")
    except Exception:
        print(msg_u)
    migration_type, migration_time = _normalize_and_validate_migration_args()
    print(
        "【{0}】迁移模式参数：migrationType={1}, migrationTime={2}".format(
            _now(), migration_type, (migration_time or "")
        )
    )
    auth = get_auth()
    # 1) 定位配置 sheet（此时 DOC_LIB_NAME/FILE_NAME/SHEET_NAME 指向 config_*）
    cfg_file_id, cfg_sheet_id = resolve_file_sheet_ids(auth, for_config=True)
    print("【{0}】配置sheet定位完成：file_id={1}, sheet_id={2}".format(_now(), cfg_file_id, cfg_sheet_id))

    # 2) 读取配置 sheet（强制 prefer_id=false）
    # 配置栏位名（主字段是“是否迁移”，同时兼容少量别名）
    CFG_FLAG_NAMES = [u"是否迁移", u"迁移", u"迁移标识", u"是否迁移标识"]
    CFG_DB_NAMES = [u"上报库名", u"库名", u"database", u"hive_database", u"HIVE_DATABASE"]
    CFG_TBL_NAMES = [u"上报表名称", u"上报表名", u"表名", u"table_name"]
    # 目标多维表文档库名称：由配置 sheet 直接提供（不再用“部门名称+后缀”拼接）
    CFG_DOCLIB_NAMES = [u"文档库名称", u"文档库", u"doc_lib_name", u"doclib"]
    CFG_RES_NAMES = [u"上报资源名", u"资源名", u"file_name"]
    CFG_MAP_NAMES = [u"特殊字段转换配置", u"字段转换配置", u"field_mapping_json", u"字段映射"]

    jobs = []
    page_num = 1
    page_size = int(os.getenv("KINGSOFT_CONFIG_PAGE_SIZE", "100"))
    debug_first_fields = []
    debug_first_record_keys = []
    debug_flag_samples = []
    while True:
        resp = _list_records_by_page_with_prefer(auth, cfg_file_id, cfg_sheet_id, prefer_id=False, page_num=page_num, page_size=page_size)
        data = (resp or {}).get("data", {}) if isinstance(resp, dict) else {}
        recs = []
        if isinstance(data, dict):
            recs = data.get("records") or data.get("items") or data.get("list") or []
        if not isinstance(recs, list) or not recs:
            break
        for r in recs:
            f = _record_fields_dict(r)
            if len(debug_first_fields) < 3:
                try:
                    # 打印解析后的 fields（截断），用于确认 fields JSON 字符串是否成功解码成 dict
                    ftxt = json.dumps(f, ensure_ascii=False) if isinstance(f, dict) else to_text(f)
                    _print_u(u"【{0}】配置sheet解析后fields示例(截断)：{1}".format(to_text(_now()), to_text(ftxt[:400])))
                except Exception:
                    pass
            if len(debug_first_fields) < 3:
                try:
                    debug_first_fields.append(sorted([to_text(k) for k in f.keys()])[:30])
                except Exception:
                    pass
            if len(debug_first_record_keys) < 3:
                try:
                    debug_first_record_keys.append(sorted([to_text(k) for k in (r or {}).keys()])[:30])
                except Exception:
                    pass
            flag = _get_cfg_field_any(f, CFG_FLAG_NAMES, "")
            if len(debug_flag_samples) < 12:
                try:
                    # 打印“是否迁移”值样例，确认解析是否正确
                    debug_flag_samples.append(u"{0}".format(to_text(flag)))
                except Exception:
                    pass
            if not _flag_is_yes(flag):
                continue
            tbv = _get_cfg_field_any(f, CFG_TBL_NAMES, "")
            db_raw = _get_cfg_field_any(f, CFG_DB_NAMES, "")
            hive_db = (to_text(db_raw).strip() if db_raw else "") or HIVE_DATABASE_DEFAULT
            doclib = _get_cfg_field_any(f, CFG_DOCLIB_NAMES, "")
            res = _get_cfg_field_any(f, CFG_RES_NAMES, "")
            fmap = _get_cfg_field_any(f, CFG_MAP_NAMES, "")
            if not tbv or not doclib or not res:
                continue
            target_doclib = doclib
            target_file = res
            target_sheet = tbv
            jobs.append((hive_db, tbv, target_doclib, target_file, target_sheet, fmap, migration_type, migration_time))
        page_num += 1

    print("【{0}】配置sheet解析完成：待执行作业数={1}".format(_now(), len(jobs)))
    if not jobs:
        if debug_first_fields:
            try:
                _print_u(u"【{0}】配置sheet记录字段样例（前3条，每条最多30个key）：{1}".format(
                    to_text(_now()), to_text(" || ".join([",".join(x) for x in debug_first_fields]))
                ))
            except Exception:
                pass
        if debug_flag_samples:
            try:
                _print_u(u"【{0}】配置sheet“是否迁移”取值样例（前12条）：{1}".format(
                    to_text(_now()), to_text(",".join(debug_flag_samples))
                ))
            except Exception:
                pass
        if debug_first_record_keys:
            try:
                _print_u(u"【{0}】配置sheet记录原始key样例（前3条，每条最多30个key）：{1}".format(
                    to_text(_now()), to_text(" || ".join([",".join(x) for x in debug_first_record_keys]))
                ))
            except Exception:
                pass
        # 额外输出一次原始响应结构（截断），便于定位 list_by_page 返回差异
        try:
            dbg = _list_records_by_page_with_prefer(auth, cfg_file_id, cfg_sheet_id, prefer_id=False, page_num=1, page_size=min(5, page_size))
            dbg_txt = json.dumps(dbg, ensure_ascii=False) if isinstance(dbg, (dict, list)) else to_text(dbg)
            _print_u(u"【{0}】配置sheet首屏原始响应(截断)：{1}".format(to_text(_now()), to_text(dbg_txt[:2000])))
        except Exception as _dbg_e:
            _print_u(u"【{0}】配置sheet原始响应调试失败（忽略）：err={1}".format(to_text(_now()), to_text(_dbg_e)))
        print("【{0}】无“是否迁移=是”的记录，结束。".format(_now()))
        return

    ok_cnt = 0
    fail_cnt = 0
    for idx, (dbv, tbv, t_doc, t_file, t_sheet, fmap, mt, mtime) in enumerate(jobs, start=1):
        print("=" * 80)
        print("【{0}】开始执行第 {1}/{2} 条作业".format(_now(), idx, len(jobs)))
        try:
            _set_globals_for_job(dbv, tbv, t_doc, t_file, t_sheet, fmap, mt, mtime)
            _run_single_job(auth, start_ts=time.time())
            ok_cnt += 1
        except Exception as e:
            fail_cnt += 1
            _print_u(u"【{0}】ERROR: 第 {1} 条作业失败：{2}".format(
                to_text(_now()), idx, to_text(e)
            ))

    cost = round(time.time() - start, 2)
    print("=" * 80)
    print("【{0}】作业完成，总耗时 {1} 秒，成功={2}，失败={3}".format(_now(), cost, ok_cnt, fail_cnt))
    print("=" * 80)


if __name__ == "__main__":
    main()

