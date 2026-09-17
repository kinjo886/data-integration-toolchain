# -*- coding: utf-8 -*-
## PYTHON
## ******************************************************************** ##
## author: zhoushuai
## create time: 2026/04/23 09:43:01 GMT+08:00
## ******************************************************************** ##
"""批量创建 DataArts Studio 中调用 Python 脚本的开发作业。

入参支持以下格式（命令行第一个参数，或以 @路径 从文件读取）：
1) JSON 数组字符串（多条任务，推荐）：[{...},{...}]
2) JSON 对象字符串（单条任务）：{...}
3) {"jobs":[...]} 包装格式（兼容旧用法，不推荐）。
每项必填：project_id、workspace_id、directory、node_name、cdm_job_name；
cron_expression 可选，默认 0 20 1 * * ?。
params 可选（字符串）：映射到 Python 节点 arguments（作业参数输入框），支持 |||...||| 包裹内层 JSON。
params_b64 可选（字符串）：兼容字段名，按普通字符串处理，优先级低于 params。

node_properties 可选：若传入则直接作为节点 properties 使用（对象数组，每项含 name/value）；
未传 node_properties 时，脚本按 Python 作业导出模板自动生成节点属性。

流程与 create-kingsoft-data-insert-hive-jod-prod.py 一致：按 project 获取 IAM Token →
POST /v1/{project_id}/jobs 创建 → 成功后 POST .../jobs/{name}/start 启动。
"""

from __future__ import print_function

import json
import logging
import os
import ast
import re
import ssl
import sys
from datetime import datetime, timedelta
try:
    import urllib.request as urllib_request
    import urllib.parse as urllib_parse
    import urllib.error as urllib_error
except ImportError:  # Python 2
    import urllib2 as urllib_request
    import urllib as urllib_parse
    import urllib2 as urllib_error

LOGGER = logging.getLogger(__name__)
PARSER_BUILD = "2026-04-24-1728"
ssl._create_default_https_context = ssl._create_unverified_context
try:
    JSONDecodeError = json.JSONDecodeError
except AttributeError:  # Python 2
    JSONDecodeError = ValueError

# ========== IAM 鉴权配置（与 create-kingsoft-data-insert-hive-jod-prod.py 一致）==========
IAM_URL = "https://<INTERNAL_IAM_HOST>/v3/auth/tokens"
IAM_USERNAME = "admin_user"
IAM_PASSWORD = "<YOUR_IAM_PASSWORD>"
IAM_DOMAIN_NAME = "政务大数据治理平台"

# ========== DataArts Studio API 配置（与 create-kingsoft-data-insert-hive-jod-prod.py 一致）==========
DATAARTS_BASE_URL = "https://dayu-dlf.<INTERNAL_REGION>.example.gov.cn"

DEFAULT_CRON_EXPRESSION = "0 20 1 * * ?"
DEFAULT_INTERVAL_TYPE = "days"
DEFAULT_OWNER = "liguozhuang"
FIXED_PYTHON_SCRIPT_PATH = "kingsoft-data-insert-hive-prod-all.py"
def _default_start_time():
    """默认调度开始时间：当前时间后 5 分钟（Asia/Shanghai, +08）。"""
    now = datetime.utcnow() + timedelta(hours=8, minutes=5)
    return now.strftime("%Y-%m-%dT%H:%M:%S+08")


def _start_time_from_cron_or_default(cron_expression):
    """尽量按 cron 生成下一次触发时间，避免周期调度校验失败。"""
    expr = str(cron_expression or "").strip()
    m = re.match(r"^(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s+\*\s+\*\s+\?$", expr)
    if not m:
        return _default_start_time()
    sec = int(m.group(1))
    minute = int(m.group(2))
    hour = int(m.group(3))
    if sec > 59 or minute > 59 or hour > 23:
        return _default_start_time()
    now = datetime.utcnow() + timedelta(hours=8)
    candidate = now.replace(hour=hour, minute=minute, second=sec, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate.strftime("%Y-%m-%dT%H:%M:%S+08")
DEFAULT_CONNECTION_NAME = "python_or_shell"
DEFAULT_CONNECTION_ID = "70e3e454ced94eef9ae80e83df113183"
PARAMS_MAP = {
    "lgdsj_left_bracket_cn": "（",
    "lgdsj_right_bracket_cn": "）",
    "lgdsj_left_bracket_en": "(",
    "lgdsj_right_bracket_en": ")",
    "lgdsj_strikethrough": "-",
    "lgdsj_underline": "_",
    "lgdsj_dot_mark": "."
}


def get_x_auth_token(project_id, timeout_sec=30.0):
    """通过 IAM 鉴权接口获取 X-Auth-Token。"""
    token_body = {
        "auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "name": IAM_USERNAME,
                        "password": IAM_PASSWORD,
                        "domain": {"name": IAM_DOMAIN_NAME},
                    }
                },
            },
            "scope": {"project": {"id": project_id}},
        }
    }
    headers = {"Content-Type": "application/json;charset=utf8"}
    # 兼容 Py2 运行环境：请求体统一走 ASCII 安全 JSON，避免 UnicodeDecodeError
    data = json.dumps(token_body, ensure_ascii=True).encode("utf-8")
    request = urllib_request.Request(IAM_URL, data=data, headers=headers)
    with urllib_request.urlopen(request, timeout=timeout_sec) as response:
        token = (response.getheader("X-Subject-Token") or "").strip()
        if not token:
            raise ValueError("鉴权成功但未获取到 X-Subject-Token。")
        return token


def build_create_dev_job_url(project_id):
    """构建创建开发任务作业接口 URL。"""
    return "{}/v1/{}/jobs".format(DATAARTS_BASE_URL, project_id)


def build_start_dev_job_url(project_id, job_name):
    """构建启动作业接口 URL。"""
    encoded_job_name = urllib_parse.quote(job_name, safe="")
    return "{}/v1/{}/jobs/{}/start".format(DATAARTS_BASE_URL, project_id, encoded_job_name)


def create_dev_job(
    project_id, workspace_id, x_auth_token, job_body, timeout_sec=120.0
):
    """调用 DataArts Studio 创建作业接口。"""
    url = build_create_dev_job_url(project_id)
    headers = {
        "X-Auth-Token": x_auth_token.strip(),
        "Content-Type": "application/json;charset=UTF-8",
        "X-Language": "zh-cn",
    }
    if workspace_id:
        headers["workspace"] = workspace_id

    # 兼容 Py2 运行环境：请求体统一走 ASCII 安全 JSON，避免 UnicodeDecodeError
    data = json.dumps(job_body, ensure_ascii=True).encode("utf-8")
    request = urllib_request.Request(url, data=data, headers=headers)

    LOGGER.info("[CreateDevJob] 正在创建作业 URL=%s name=%s", url, job_body.get("name"))

    try:
        with urllib_request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(response.getcode())
            parsed = None
            try:
                parsed = json.loads(raw) if raw.strip() else None
            except JSONDecodeError:
                parsed = None
            LOGGER.info("[CreateDevJob] HTTP %s", code)
            return code, raw, parsed
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed = None
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except JSONDecodeError:
            parsed = None
        LOGGER.error(
            "[CreateDevJob] HTTP 错误 code=%s url=%s body=%s",
            exc.code, url, raw[:2000],
        )
        return exc.code, raw, parsed


def start_dev_job(
    project_id, workspace_id, job_name, x_auth_token, timeout_sec=120.0
):
    """调用 DataArts Studio 启动作业接口。"""
    url = build_start_dev_job_url(project_id, job_name)
    headers = {
        "X-Auth-Token": x_auth_token.strip(),
        "Content-Type": "application/json;charset=UTF-8",
        "X-Language": "zh-cn",
    }
    if workspace_id:
        headers["workspace"] = workspace_id

    # 兼容 Python2：不传 data 时会被当作 GET，这里显式给空 body 以触发 POST
    request = urllib_request.Request(url, data=b"", headers=headers)
    LOGGER.info("[StartDevJob] 正在启动 URL=%s", url)

    try:
        with urllib_request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(response.getcode())
            parsed = None
            try:
                parsed = json.loads(raw) if raw.strip() else None
            except JSONDecodeError:
                parsed = None
            LOGGER.info("[StartDevJob] HTTP %s", code)
            return code, raw, parsed
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed = None
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except JSONDecodeError:
            parsed = None
        LOGGER.error(
            "[StartDevJob] HTTP 错误 code=%s url=%s body=%s",
            exc.code, url, raw[:2000],
        )
        return exc.code, raw, parsed


def normalize_workspace_id(workspace_raw):
    """与 jod 脚本一致：去掉 workspace_id 中的连字符。"""
    if workspace_raw is None:
        return ""
    s = str(workspace_raw).strip()
    return s.replace("-", "") if s else ""


def _param_item_to_property(item, index):
    """将属性元素转为节点 property 对象。"""
    if isinstance(item, dict):
        name = item.get("name")
        if name is None or str(name).strip() == "":
            raise ValueError("params 每一项对象必须包含非空 name")
        value = item.get("value", "")
        if value is None:
            value = ""
        return {
            "name": str(name),
            "value": value if isinstance(value, str) else str(value),
            "value_is_sensitive": bool(item.get("value_is_sensitive", False)),
        }

    value = "" if item is None else item
    return {
        "name": "p%d" % index,
        "value": value if isinstance(value, str) else str(value),
        "value_is_sensitive": False,
    }


def build_node_properties(node_properties):
    """将用户传入的节点属性规范化。"""
    if node_properties is None:
        return []
    if not isinstance(node_properties, list):
        raise ValueError("node_properties 必须是数组")

    properties = []
    for idx, item in enumerate(node_properties, start=1):
        prop = _param_item_to_property(item, idx)
        properties.append(prop)
    return properties


def _to_script_name(python_script_path):
    """将脚本路径转成 Python 作业 scriptName（去目录、去.py）。"""
    base = os.path.basename(str(python_script_path).strip())
    if base.lower().endswith(".py"):
        return base[:-3]
    return base


def build_default_python_shell_properties(
    python_script_path,
    connection_name=DEFAULT_CONNECTION_NAME,
    connection_id=DEFAULT_CONNECTION_ID,
    arguments="",
    script_name_override=None,
):
    """按导出 .job 模板构建 Python 节点 properties。"""
    script_name = str(script_name_override).strip() if script_name_override else _to_script_name(python_script_path)
    return [
        {"name": "scriptName", "value": script_name, "value_is_sensitive": False},
        {"name": "connectionName", "value": str(connection_name), "value_is_sensitive": False},
        {"name": "connectionId", "value": str(connection_id), "value_is_sensitive": False},
        {"name": "arguments", "value": str(arguments or ""), "value_is_sensitive": False},
        {"name": "statementOrScript", "value": "SCRIPT", "value_is_sensitive": False},
        {"name": "pythonVersion", "value": "Python 3", "value_is_sensitive": False},
        {"name": "emptyRunningJob", "value": "0", "value_is_sensitive": False},
        {"name": "taskWorkGroupId", "value": "-1", "value_is_sensitive": False},
    ]


def generate_node_candidates(python_script_path, item):
    """生成候选节点配置（以导出模板为准，仅 Python）。"""
    base = os.path.basename(str(python_script_path).strip())
    no_ext = _to_script_name(python_script_path)
    return [
        {
            "node_type": "Python",
            "single_node_job_type": "Python",
            "properties": build_default_python_shell_properties(
                python_script_path=python_script_path,
                connection_name=item.get("connection_name", DEFAULT_CONNECTION_NAME),
                connection_id=item.get("connection_id", DEFAULT_CONNECTION_ID),
                arguments=item.get("_resolved_params", item.get("params", item.get("arguments", ""))),
                script_name_override=no_ext,
            ),
            "tag": "Python(script-no-ext)",
        },
        {
            "node_type": "Python",
            "single_node_job_type": "Python",
            "properties": build_default_python_shell_properties(
                python_script_path=python_script_path,
                connection_name=item.get("connection_name", DEFAULT_CONNECTION_NAME),
                connection_id=item.get("connection_id", DEFAULT_CONNECTION_ID),
                arguments=item.get("_resolved_params", item.get("params", item.get("arguments", ""))),
                script_name_override=base,
            ),
            "tag": "Python(script-with-ext)",
        },
    ]


def build_batch_cdm_job_body(
    directory,
    node_name,
    cdm_job_name,
    cron_expression,
    node_properties,
    node_type="Python",
    single_node_job_type="Python",
    interval_type=DEFAULT_INTERVAL_TYPE,
    owner=DEFAULT_OWNER,
    job_description="",
    location_x="705",
    location_y="636",
    node_location_x="-207.0",
    node_location_y="-253.0",
    start_time=None,
):
    """构建创建单节点作业请求体（按导出 .job 模板结构）。"""
    expr = str(cron_expression).strip()
    effective_start_time = str(start_time).strip() if start_time else _start_time_from_cron_or_default(expr)
    body = {
        "basicConfig": {
            "agency": "",
            "customFields": {},
            "encrypt": False,
            "executeUser": "",
            "instanceTimeout": 0,
            "isIgnoreWaiting": 0,
            "jobDescription": job_description or "",
            "owner": owner,
            "priority": 0,
            "tags": [],
            "taskPriority": 0,
        },
        "cleanOverdueDays": 60,
        "cleanWaitingJob": "cleanup",
        "directory": directory,
        "emptyRunningJob": "0",
        "lastUpdateUser": owner,
        "location": {"x": location_x, "y": location_y},
        "maskedParams": [],
        "name": cdm_job_name,
        "nodes": [
            {
                "execTimeOutRetry": "false",
                "failPolicy": "FAIL_CHILD",
                "location": {"x": node_location_x, "y": node_location_y},
                "maxExecutionTime": 360,
                "name": node_name,
                "pollingInterval": 20,
                "preNodeName": [],
                "properties": node_properties,
                "resouces": [],
                "retryInterval": 120,
                "retryTimes": 0,
                "type": node_type,
            }
        ],
        "processType": "BATCH",
        "resouces": [],
        "runAtOnceAfterIncubate": "0",
        "schedule": {
            "cron": {
                "calendarScheduling": "false",
                "concurrent": 1,
                "dependJobs": {
                    "dependFailPolicy": "FAIL",
                    "dependPeriod": "SAME_PERIOD",
                    "jobs": [],
                    "sameWorkSpaceJobs": [],
                },
                "dependPrePeriod": False,
                "expression": expr,
                "expressionTimeZone": "Asia/Shanghai",
                "intervalType": interval_type,
                "isSkipSelfDepJob": "false",
                "monitorObsPath": False,
                "scanDuration": 0,
                "scanInterval": 0,
                "startTime": effective_start_time,
            },
            "requireManualConfirmBeforeExecute": False,
            "scheduleOffset": 1,
            "type": "CRON",
        },
        "singleNodeJobFlag": False,
        "taskWorkGroupId": "",
        "useCdmCache": False,
        "version": "2",
    }
    if job_description:
        body["description"] = job_description
    return body


REQUIRED_KEYS = (
    "project_id",
    "workspace_id",
    "directory",
    "node_name",
    "cdm_job_name",
)


def validate_job_item(item, index):
    """校验单条任务配置；异常时抛出 ValueError。"""
    if not isinstance(item, dict):
        raise ValueError("第 %d 条：必须是 JSON 对象" % (index + 1))
    item = _normalize_item_keys(item)
    item = _ensure_required_keys(item)
    LOGGER.info("第 %d 条解析后键名: %s", index + 1, ",".join(sorted([str(k) for k in item.keys()])))
    for key in REQUIRED_KEYS:
        if key not in item:
            raise ValueError("第 %d 条：缺少必填字段 %s（已解析键: %s）" % (
                index + 1, key, ",".join(sorted([str(k) for k in item.keys()]))
            ))
    if item["project_id"] is None or str(item["project_id"]).strip() == "":
        raise ValueError("第 %d 条：project_id 不能为空" % (index + 1))
    if item["workspace_id"] is None or str(item["workspace_id"]).strip() == "":
        raise ValueError("第 %d 条：workspace_id 不能为空" % (index + 1))
    if item["directory"] is None or str(item["directory"]).strip() == "":
        raise ValueError("第 %d 条：directory 不能为空" % (index + 1))
    if item["node_name"] is None or str(item["node_name"]).strip() == "":
        raise ValueError("第 %d 条：node_name 不能为空" % (index + 1))
    if item["cdm_job_name"] is None or str(item["cdm_job_name"]).strip() == "":
        raise ValueError("第 %d 条：cdm_job_name 不能为空" % (index + 1))
    if "node_properties" in item and not isinstance(item["node_properties"], list):
        raise ValueError("第 %d 条：node_properties 必须是数组" % (index + 1))
    if "params" in item and item["params"] is not None and not isinstance(item["params"], str):
        raise ValueError("第 %d 条：params 必须是字符串" % (index + 1))
    if "params_b64" in item and item["params_b64"] is not None and not isinstance(item["params_b64"], str):
        raise ValueError("第 %d 条：params_b64 必须是字符串" % (index + 1))
    if "connection_name" in item and (item["connection_name"] is None or str(item["connection_name"]).strip() == ""):
        raise ValueError("第 %d 条：connection_name 不能为空" % (index + 1))
    if "connection_id" in item and (item["connection_id"] is None or str(item["connection_id"]).strip() == ""):
        raise ValueError("第 %d 条：connection_id 不能为空" % (index + 1))
    # 关键字符串字段清洗，防止在线执行器拼接污染（如尾部 ] ,）
    for key in (
        "project_id", "workspace_id", "directory", "node_name",
        "cdm_job_name", "cron_expression"
    ):
        if key in item and isinstance(item[key], str):
            if key in ("project_id", "workspace_id"):
                item[key] = _normalize_id(item[key])
            elif key == "cron_expression":
                item[key] = _normalize_cron_expression(item[key])
            else:
                item[key] = _clean_scalar(item[key])
    for key in ("params", "params_b64", "arguments"):
        if key in item and isinstance(item[key], str):
            item[key] = _clean_param_tail(item[key])
    return item


def load_json_jobs_arg(argv1):
    """第一个参数：JSON 字符串，或以 @ 开头的文件路径，统一返回任务列表。"""
    if argv1 is None:
        return None
    raw = argv1.strip()
    if raw.startswith("@"):
        path = raw[1:].strip()
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
    raw = _normalize_raw_input(raw)
    if not raw:
        raise ValueError("入参为空，请粘贴 JSON 字符串或使用 @文件路径")
    # 二维数组优先：[[project_id, workspace_id, directory, node_name, cdm_job_name, cron_expression, params], ...]
    matrix_items = _parse_strict_2d_array_input(raw)
    if matrix_items:
        return _cleanup_parsed_items([_normalize_item_keys(x) for x in matrix_items])
    # 新增：兼容按位置传参格式（每项按固定顺序，不带字段名）
    positional_items = _parse_positional_jobs_input(raw)
    if positional_items:
        normalized_list = [_normalize_item_keys(x) if isinstance(x, dict) else x for x in positional_items]
        return _cleanup_parsed_items(normalized_list)
    # 先尝试归一化引号后直接 JSON 解析（避免误走 key:value 解析）
    try:
        parsed = json.loads(_repair_loose_json(_normalize_json_quotes(raw)))
    except Exception:
        try:
            parsed = _parse_json_with_fallback(raw)
        except ValueError:
            # 兼容平台把多条任务拼成 [k:v,...] [k:v,...] 这种非 JSON 形式
            seq = _parse_kv_bracket_sequence(raw)
            if seq:
                parsed = seq
            else:
                # 兼容在线平台的 key:value 文本格式（仅在不像 JSON 时启用）
                stripped = raw.lstrip()
                if stripped.startswith("{"):
                    raise
                # 最后兜底：把 []{} 等包裹符抹平后按全局 key:value 解析
                if stripped.startswith("["):
                    flat = re.sub(r"[\[\]\{\}\"]+", " ", raw)
                    flat = re.sub(r"\s+", " ", flat).strip()
                    kv_item = _parse_plain_kv_input(flat) or _parse_loose_kv_item(flat)
                    if kv_item:
                        parsed = kv_item
                    else:
                        raise
                    # 走到这里表示已命中最后兜底，直接跳过后续分支
                    if isinstance(parsed, list):
                        return [_normalize_item_keys(x) if isinstance(x, dict) else x for x in parsed]
                    if isinstance(parsed, dict):
                        return [_normalize_item_keys(parsed)]
                kv_item = _parse_plain_kv_input(raw)
                if kv_item:
                    parsed = kv_item
                else:
                    raise
    if isinstance(parsed, list):
        normalized_list = [_normalize_item_keys(x) if isinstance(x, dict) else x for x in parsed]
        return _cleanup_parsed_items(normalized_list)
    if isinstance(parsed, dict):
        if "jobs" in parsed:
            jobs = parsed.get("jobs")
            if not isinstance(jobs, list):
                raise ValueError("jobs 字段必须是数组")
            normalized_jobs = [_normalize_item_keys(x) if isinstance(x, dict) else x for x in jobs]
            return _cleanup_parsed_items(normalized_jobs)
        return [_normalize_item_keys(parsed)]
    raise ValueError("入参必须是 JSON 对象、JSON 数组，或带 jobs 字段的对象")


def _parse_positional_jobs_input(raw):
    """解析按位置传参的数组格式。

    支持：
    1) [[project_id, workspace_id, directory, node_name, cdm_job_name, cron_expression, params], ...]
    2) [{"project_id","workspace_id","directory","node_name","cdm_job_name","cron_expression","params"}, ...]
    """
    if not raw:
        return None
    s = str(raw).strip()
    if not s or not s.startswith("["):
        return None

    ordered_keys = [
        "project_id",
        "workspace_id",
        "directory",
        "node_name",
        "cdm_job_name",
        "cron_expression",
        "params",
    ]

    # 先尝试标准 JSON 的“二维数组”形式
    try:
        parsed = json.loads(_repair_loose_json(_normalize_json_quotes(s)))
        if isinstance(parsed, list) and parsed and all(isinstance(x, list) for x in parsed):
            items = []
            for arr in parsed:
                if len(arr) < 5:
                    continue
                obj = {}
                for i, key in enumerate(ordered_keys):
                    if i < len(arr):
                        obj[key] = arr[i]
                items.append(obj)
            return items or None
    except Exception:
        pass

    # 再兼容 {"v1","v2",...} 这种非 JSON 写法（按出现顺序映射字段）
    if "{" not in s or "}" not in s:
        # 兼容平台压扁成多个方括号值分片： [v1,v2] [v1,v2] ...
        return _parse_positional_from_bracket_chunks(s, ordered_keys)
    chunks = re.findall(r"\{([^{}]*)\}", s, flags=re.S)
    if not chunks:
        return _parse_positional_from_bracket_chunks(s, ordered_keys)

    items = []
    for chunk in chunks:
        body = chunk.strip()
        # 如果像标准对象（含 key:value）则不在这里处理
        if re.search(r"[A-Za-z_][A-Za-z0-9_]*\s*:", body):
            return None
        vals = re.findall(r'"((?:\\.|[^"\\])*)"', body)
        if not vals:
            vals = re.findall(r"'((?:\\.|[^'\\])*)'", body)
        if not vals:
            parts = [x.strip().strip('"').strip("'") for x in body.split(",")]
            vals = [x for x in parts if x]
        if len(vals) < 5:
            continue
        obj = {}
        for i, key in enumerate(ordered_keys):
            if i < len(vals):
                obj[key] = vals[i]
        items.append(obj)
    if items:
        return items
    return _parse_positional_from_bracket_chunks(s, ordered_keys)


def _parse_strict_2d_array_input(raw):
    """严格解析二维数组入参，避免字段错位。

    期望格式：
    [[project_id, workspace_id, directory, node_name, cdm_job_name, cron_expression, params], ...]
    """
    if not raw:
        return None
    s = str(raw).strip()
    if not (s.startswith("[[") and s.endswith("]")):
        return None
    try:
        arr = json.loads(_repair_loose_json(_normalize_json_quotes(s)))
    except Exception:
        return None
    if not isinstance(arr, list) or not arr:
        return None
    if not all(isinstance(x, list) for x in arr):
        return None

    keys = [
        "project_id",
        "workspace_id",
        "directory",
        "node_name",
        "cdm_job_name",
        "cron_expression",
        "params",
    ]
    out = []
    for row in arr:
        if len(row) < 5:
            raise ValueError("二维数组每行至少需要 5 个元素（project_id~cdm_job_name）")
        item = {}
        for i, key in enumerate(keys):
            if i < len(row):
                item[key] = row[i]
        out.append(item)
    return out


def _parse_positional_from_bracket_chunks(text, ordered_keys):
    """解析平台压扁后的方括号值分片，支持行式/列式两种组织。"""
    if not text or "[" not in text or "]" not in text:
        return None
    chunks = re.findall(r"\[([^\]]*)\]", str(text), flags=re.S)
    if not chunks:
        return None

    def _split_vals(body):
        b = body.strip()
        if not b:
            return []
        vals = re.findall(r'"((?:\\.|[^"\\])*)"', b)
        if not vals:
            vals = re.findall(r"'((?:\\.|[^'\\])*)'", b)
        if not vals:
            vals = [x.strip().strip('"').strip("'") for x in b.split(",")]
        vals = [v for v in vals if str(v).strip() != ""]
        return vals

    rows = [_split_vals(c) for c in chunks]
    rows = [r for r in rows if r]
    if not rows:
        return None
    # 若包含 key:value，说明不是位置模式
    if any(any(":" in v for v in r) for r in rows):
        return None

    # 模式 A：每个分片本身就是一条记录（>=5 列）
    row_items = []
    for r in rows:
        if len(r) >= 5:
            obj = {}
            for i, key in enumerate(ordered_keys):
                if i < len(r):
                    obj[key] = r[i]
            row_items.append(obj)
    if row_items:
        return row_items

    # 模式 B：列式分片（每个分片是一列，元素个数为记录数）
    if len(rows) < 5:
        return None

    def _build_col_items(col_rows):
        n = max(len(r) for r in col_rows) if col_rows else 0
        if n <= 0:
            return []
        out = []
        for idx in range(n):
            obj = {}
            for k_idx, key in enumerate(ordered_keys):
                if k_idx >= len(col_rows):
                    break
                col = col_rows[k_idx]
                if idx < len(col):
                    obj[key] = col[idx]
            if len(obj) >= 5:
                out.append(obj)
        return out

    def _score_item(obj):
        if not isinstance(obj, dict):
            return -10
        score = 0
        pid = str(obj.get("project_id", "")).strip()
        wid = str(obj.get("workspace_id", "")).strip()
        cdm = str(obj.get("cdm_job_name", "")).strip()
        cron = str(obj.get("cron_expression", "")).strip()
        node = str(obj.get("node_name", "")).strip()
        # project/workspace 像 32 位 id
        if re.match(r"^[0-9a-fA-F]{32}$", pid):
            score += 2
        if re.match(r"^[0-9a-fA-F]{32}$", wid):
            score += 2
        if pid and wid and pid == wid:
            score -= 2
        # node/job 名通常不是 32 位 id
        if node and not re.match(r"^[0-9a-fA-F]{32}$", node):
            score += 1
        if cdm and not re.match(r"^[0-9a-fA-F]{32}$", cdm):
            score += 2
        if cdm and cdm == pid:
            score -= 3
        # cron 应包含空格和调度符号
        if cron and (" " in cron) and any(x in cron for x in ("*", "?", "/")):
            score += 2
        elif cron:
            score -= 1
        return score

    def _is_hex32(x):
        return bool(re.match(r"^[0-9a-fA-F]{32}$", str(x).strip()))

    def _looks_cron(x):
        s = str(x).strip()
        return bool(s) and (" " in s) and any(ch in s for ch in ("*", "?", "/"))

    def _looks_path(x):
        s = str(x).strip()
        return s.startswith("/")

    def _looks_node_token(x):
        s = str(x).strip().lower()
        return s.startswith(("ods_", "dwd_", "dim_", "tmp_"))

    def _looks_cdm_token(x):
        s = str(x).strip().lower()
        if s.startswith(("dev_job", "prod_job", "test_job")):
            return True
        return ("job" in s) and (not s.startswith(("ods_", "dwd_", "dim_", "tmp_")))

    def _is_bad_items(cand):
        if not cand:
            return True
        for it in cand:
            if not isinstance(it, dict):
                return True
            if _is_hex32(it.get("cdm_job_name", "")):
                return True
            if _is_hex32(it.get("cron_expression", "")):
                return True
            if _is_hex32(it.get("node_name", "")):
                return True
            if _looks_node_token(it.get("cdm_job_name", "")):
                return True
            if not _looks_cron(it.get("cron_expression", "")):
                return True
        return False

    # 优先尝试前 7 列；若存在脏前缀列，使用滑窗选得分最高的一组 7 列
    best_items = []
    best_score = -10**9
    max_start = max(0, len(rows) - 5)
    for start in range(0, max_start):
        window = rows[start : start + len(ordered_keys)]
        if len(window) < 5:
            continue
        cand = _build_col_items(window)
        if not cand:
            continue
        score = sum(_score_item(x) for x in cand)
        pids = [str(x.get("project_id", "")).strip() for x in cand if isinstance(x, dict)]
        pids = [x for x in pids if x]
        if pids and len(set(pids)) == 1:
            score += 2
        wids = [str(x.get("workspace_id", "")).strip() for x in cand if isinstance(x, dict)]
        wids = [x for x in wids if x]
        if wids and len(set(wids)) == 1:
            score += 1
        if score > best_score:
            best_score = score
            best_items = cand
    if best_items and not _is_bad_items(best_items):
        return best_items

    # 语义重建兜底：按列内容特征识别字段，修复首条错位问题
    n = max(len(r) for r in rows) if rows else 0
    if n <= 0:
        return best_items or None
    used = set()
    picked = {}

    # 1) 先识别 project_id/workspace_id 列
    id_cols = []
    for i, col in enumerate(rows):
        vals = [str(v).strip() for v in col if str(v).strip()]
        if vals and all(_is_hex32(v) for v in vals):
            id_cols.append((i, vals))
    if id_cols:
        # project_id: 取最稳定且出现最早的 id 列
        project_idx, project_vals = id_cols[0]
        picked["project_id"] = (project_idx, project_vals)
        used.add(project_idx)
        # workspace_id: 取与 project_id 不同且“值稳定”的 id 列
        workspace_pick = None
        for idx, vals in id_cols[1:]:
            uniq = list(dict.fromkeys(vals))
            if uniq and uniq[0] != project_vals[0]:
                workspace_pick = (idx, vals)
                break
        if workspace_pick is None and len(id_cols) >= 2:
            workspace_pick = id_cols[-1]
        if workspace_pick is not None:
            picked["workspace_id"] = workspace_pick
            used.add(workspace_pick[0])

    # 2) 识别 cron/directory/params
    def _pick_best_col(score_fn):
        best = None
        best_score_local = -10**9
        for i, col in enumerate(rows):
            if i in used:
                continue
            vals = [str(v).strip() for v in col if str(v).strip()]
            if not vals:
                continue
            sc = sum(score_fn(v) for v in vals)
            if sc > best_score_local:
                best_score_local = sc
                best = (i, vals)
        return best

    cron_col = _pick_best_col(lambda v: 2 if _looks_cron(v) else -1)
    if cron_col:
        picked["cron_expression"] = cron_col
        used.add(cron_col[0])
    dir_col = _pick_best_col(lambda v: 2 if _looks_path(v) else -1)
    if dir_col:
        picked["directory"] = dir_col
        used.add(dir_col[0])
    params_col = _pick_best_col(lambda v: 2 if (" " in v or len(v) > 20) else 0)
    if params_col:
        picked["params"] = params_col
        used.add(params_col[0])

    # 3) 剩余列识别 node_name / cdm_job_name
    rem = []
    for i, col in enumerate(rows):
        if i in used:
            continue
        vals = [str(v).strip() for v in col if str(v).strip()]
        if not vals:
            continue
        rem.append((i, vals))
    node_col = None
    cdm_col = None
    for i, vals in rem:
        if sum(1 for v in vals if _looks_node_token(v)) >= max(1, len(vals) // 2):
            node_col = (i, vals)
            break
    for i, vals in rem:
        if node_col and i == node_col[0]:
            continue
        if sum(1 for v in vals if _looks_cdm_token(v)) >= max(1, len(vals) // 2):
            cdm_col = (i, vals)
            break
    # 次优兜底：若未命中 cdm 列，选“非 cron/非路径/非id 且不似 node”的列
    if cdm_col is None:
        for i, vals in rem:
            if node_col and i == node_col[0]:
                continue
            if not any(_looks_cron(v) or _looks_path(v) or _is_hex32(v) for v in vals) and not any(_looks_node_token(v) for v in vals):
                cdm_col = (i, vals)
                break
    if node_col:
        picked["node_name"] = node_col
    if cdm_col:
        picked["cdm_job_name"] = cdm_col

    rebuilt = []
    for idx in range(n):
        obj = {}
        for key in ordered_keys:
            if key not in picked:
                continue
            vals = picked[key][1]
            if idx < len(vals):
                obj[key] = vals[idx]
        if len(obj) >= 5:
            rebuilt.append(obj)
    if rebuilt and not _is_bad_items(rebuilt):
        return rebuilt
    return best_items or rebuilt or None


def _parse_kv_bracket_sequence(raw):
    """解析形如 [k:v,...] [k:v,...] 的多条任务（平台偶发把 JSON 数组破坏成这种格式）。"""
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # 只处理外层看起来像“方括号包裹的 kv 片段序列”
    if "[" not in s or "]" not in s:
        return None
    chunks = re.findall(r"\[([^\]]*)\]", s, flags=re.S)
    # 兜底：若平台把末尾 ] 截断，至少保留可见片段继续解析
    if not chunks:
        rough = re.split(r"\]\s*\[|\[|\]", s)
        chunks = [x for x in rough if x and x.strip()]
    items = []
    for chunk in chunks:
        chunk = chunk.strip().strip(",").strip()
        if not chunk:
            continue
        # 单段 kv
        item = _parse_plain_kv_input(chunk)
        if not item:
            item = _parse_loose_kv_item(chunk)
        if not item:
            return None
        if isinstance(item, list):
            items.extend(item)
        else:
            items.append(item)
    if not items:
        return None
    # 清理平台重复拼接导致的“空壳任务”（仅 project_id），避免后续必填校验失败
    if len(items) > 1:
        cleaned = []
        for i, it in enumerate(items):
            if not isinstance(it, dict):
                cleaned.append(it)
                continue
            keys = set(it.keys())
            if keys == {"project_id"}:
                pid = str(it.get("project_id", "")).strip()
                has_richer_same_pid = False
                for j, other in enumerate(items):
                    if i == j or not isinstance(other, dict):
                        continue
                    if str(other.get("project_id", "")).strip() == pid and len(other.keys()) > 1:
                        has_richer_same_pid = True
                        break
                if has_richer_same_pid:
                    continue
            cleaned.append(it)
        if cleaned:
            items = cleaned
    return items


def _parse_loose_kv_item(text):
    """宽松解析 k:v 片段，兼容异常符号/未知键名。"""
    if not text:
        return None
    s = str(text).strip().replace("：", ":")
    if not s:
        return None
    # 仅保留本脚本可识别字段，避免噪声污染
    key_pat = (
        r"(project_id|workspace_id|directory|node_name|cdm_job_name|cron_expression|"
        r"params|params_b64|interval_type|owner|description|location_x|location_y|"
        r"node_location_x|node_location_y|start_time)"
    )
    matches = list(re.finditer(key_pat + r"\s*:\s*", s, flags=re.I))
    if not matches:
        return None
    item = {}
    for idx, m in enumerate(matches):
        key = m.group(1)
        v_start = m.end()
        v_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(s)
        value = s[v_start:v_end].strip().strip(",").strip()
        if value:
            item[key.lower()] = value
    if not item:
        return None
    return _normalize_item_keys(item)


def _cleanup_parsed_items(items):
    """清理平台脏分片：仅 project_id 项、以及被完整项覆盖的残缺子集项。"""
    if not isinstance(items, list) or not items:
        return items
    # 先去掉仅 project_id 的尾巴项（当存在更完整任务时）
    has_full_item = any(
        isinstance(it, dict)
        and (
            str(it.get("workspace_id", "")).strip()
            or str(it.get("cdm_job_name", "")).strip()
            or str(it.get("node_name", "")).strip()
            or str(it.get("directory", "")).strip()
        )
        for it in items
    )
    cleaned = []
    for it in items:
        if isinstance(it, dict) and has_full_item:
            keys = {k for k, v in it.items() if str(v).strip() != ""}
            if keys == {"project_id"}:
                continue
        cleaned.append(it)

    # 再去掉“被覆盖的残缺子集项”：同一任务身份下，字段更少且值完全可被更完整项覆盖
    result = []
    for i, cur in enumerate(cleaned):
        if not isinstance(cur, dict):
            result.append(cur)
            continue
        cur_non_empty = {k: str(v).strip() for k, v in cur.items() if str(v).strip() != ""}
        if not cur_non_empty:
            continue
        ident = (
            cur_non_empty.get("project_id", ""),
            cur_non_empty.get("workspace_id", ""),
            cur_non_empty.get("directory", ""),
            cur_non_empty.get("node_name", ""),
        )
        covered = False
        for j, other in enumerate(cleaned):
            if i == j or not isinstance(other, dict):
                continue
            other_non_empty = {k: str(v).strip() for k, v in other.items() if str(v).strip() != ""}
            other_ident = (
                other_non_empty.get("project_id", ""),
                other_non_empty.get("workspace_id", ""),
                other_non_empty.get("directory", ""),
                other_non_empty.get("node_name", ""),
            )
            if ident != other_ident:
                continue
            if len(other_non_empty) <= len(cur_non_empty):
                continue
            if all(other_non_empty.get(k) == v for k, v in cur_non_empty.items()):
                covered = True
                break
        if not covered:
            result.append(cur)
    return result


def _normalize_json_quotes(text):
    """将常见弯引号/全角引号替换为 ASCII 双引号，提升 json.loads 成功率。"""
    if text is None:
        return ""
    s = str(text)
    # Unicode 弯引号
    s = s.replace("\u201c", '"').replace("\u201d", '"')
    s = s.replace("\u2018", "'").replace("\u2019", "'")
    # 全角引号
    s = s.replace("\uff02", '"')
    return s


def _normalize_raw_input(raw):
    """清洗在线输入框常见包裹格式。"""
    if raw is None:
        return ""
    text = str(raw).strip()
    if not text:
        return ""
    # 去 BOM
    text = text.lstrip("\ufeff")
    # 去掉常见前缀（平台会把说明文字一起粘进来）
    text = re.sub(
        r"^(?:入参|参数|input|Input)\s*[:：]\s*",
        "",
        text,
        flags=re.I,
    )
    # 去 markdown 代码块
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    # 去掉最外层成对引号（整段被引号包起来）
    if len(text) >= 2 and ((text[0] == "'" and text[-1] == "'") or (text[0] == '"' and text[-1] == '"')):
        text = text[1:-1].strip()
    return text


def _repair_loose_json(text):
    """修复常见非严格 JSON（如对象/数组末尾多余逗号）。"""
    if not text:
        return text
    s = str(text)
    # 去掉 },] 或 ],] 前的尾随逗号（保守替换，适用于本脚本入参场景）
    s = re.sub(r",\s*}", "}", s)
    s = re.sub(r",\s*]", "]", s)
    return s


def _parse_json_with_fallback(raw):
    """先按 JSON 解析，失败则回退 Python 字面量解析。"""
    norm = _repair_loose_json(_normalize_json_quotes(raw))
    try:
        return json.loads(norm)
    except Exception:
        parsed = _decode_first_json_value(norm)
        if parsed is not None:
            return parsed
        extracted = _extract_balanced_json(norm)
        if extracted and extracted != norm:
            try:
                return json.loads(_normalize_json_quotes(extracted))
            except Exception:
                pass
            try:
                return ast.literal_eval(extracted)
            except Exception:
                pass
        extracted2 = _extract_json_fragment(norm)
        if extracted2 and extracted2 != norm:
            try:
                return json.loads(_normalize_json_quotes(extracted2))
            except Exception:
                pass
            try:
                return ast.literal_eval(extracted2)
            except Exception:
                pass
        # 兼容单引号字典/数组（如 {'a':1}）
        try:
            return ast.literal_eval(norm)
        except Exception:
            raise ValueError("Expecting value: line 1 column 1 (char 0)")


def _decode_first_json_value(raw):
    """从文本中解出首个完整 JSON 值（对象或数组）。"""
    if not raw:
        return None
    decoder = json.JSONDecoder()
    s = str(raw).strip()
    for i, ch in enumerate(s):
        if ch not in ("{", "["):
            continue
        try:
            obj, _ = decoder.raw_decode(s[i:])
            return obj
        except Exception:
            continue
    return None


def _extract_balanced_json(text):
    """从文本中提取首个完整的 JSON 对象或数组（括号栈匹配）。"""
    if not text:
        return ""
    s = str(text).strip()
    start = None
    for i, ch in enumerate(s):
        if ch in ("{", "["):
            start = i
            break
    if start is None:
        return ""
    stack = []
    in_str = False
    esc = False
    quote = None
    for i in range(start, len(s)):
        ch = s[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
                quote = None
            continue
        if ch in ("'", '"'):
            in_str = True
            quote = ch
            continue
        if ch == "{":
            stack.append("}")
        elif ch == "[":
            stack.append("]")
        elif ch in ("}", "]"):
            if not stack or ch != stack[-1]:
                return ""
            stack.pop()
            if not stack:
                return s[start : i + 1]
    return ""


def _extract_json_fragment(text):
    """从混杂文本里提取首段 JSON（对象或数组）。"""
    if not text:
        return ""
    s = text.strip()
    # 先提取所有可能的对象/数组片段，再按“是否包含关键字段”优先
    candidates = re.findall(r"\{[^{}]*\}", s, flags=re.S)
    arr_candidates = re.findall(r"\[[^\[\]]*\]", s, flags=re.S)
    candidates.extend(arr_candidates)
    if not candidates:
        return s
    key_hints = ("project_id", "workspace_id", "cdm_job_name", "python_script_path")
    hinted = [c for c in candidates if any(k in c for k in key_hints)]
    if hinted:
        hinted.sort(key=lambda x: len(x), reverse=True)
        return hinted[0]
    candidates.sort(key=lambda x: len(x), reverse=True)
    return candidates[0]


def _parse_plain_kv_input(raw):
    """解析纯文本 key:value 格式，返回单条任务 dict 或多条任务 list。

    示例:
    project_id:xxx workspace_id:yyy directory:/a/b node_name:n1 cdm_job_name:j1 python_script_path:a.py
    """
    if not raw:
        return None
    text = str(raw).strip().replace("：", ":")
    if not text:
        return None
    # 明显是 JSON 结构时不要走 key:value 解析，避免误匹配 project_id 等片段
    # 但对 [k:v,...] 这类伪 JSON 需要放行（含 : 且不含 { }）
    stripped = text.lstrip()
    if stripped.startswith("{"):
        return None
    if stripped.startswith("[") and ("{" in text or "}" in text):
        return None
    if stripped.startswith("[") and ":" in text:
        text = text.strip("[] \t\r\n")
    known_keys = [
        "project_id",
        "workspace_id",
        "directory",
        "node_name",
        "cdm_job_name",
        "cron_expression",
        "params",
        "params_b64",
        "interval_type",
        "owner",
        "description",
        "location_x",
        "location_y",
        "node_location_x",
        "node_location_y",
        "start_time",
    ]
    pattern = r"(?P<key>" + "|".join(known_keys) + r")\s*:\s*"
    matches = list(re.finditer(pattern, text))
    if not matches:
        return None

    items = []
    item = {}
    boundary_keys = {"project_id", "workspace_id", "directory", "node_name", "cdm_job_name", "python_script_path"}
    for idx, m in enumerate(matches):
        key = m.group("key")
        value_start = m.end()
        value_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        value = text[value_start:value_end].strip()
        if value.endswith(","):
            value = value[:-1].strip()
        # 当同一条记录的关键键再次出现时，判定为下一条记录开始
        if key in boundary_keys and key in item and item.get("project_id"):
            prev = item.get(key)
            # 平台偶发重复拼接同一键值（如 project_id 写两遍且值相同），不应拆成多条任务
            if prev is not None and str(prev).strip() == str(value).strip():
                item[key] = value
                continue
            items.append(_normalize_item_keys(item))
            item = {}
        item[key] = value
    if item:
        items.append(_normalize_item_keys(item))
    if len(items) == 1:
        return items[0]
    return items


def _normalize_item_keys(item):
    """字段名归一化，兼容常见别名写法。"""
    if not isinstance(item, dict):
        return item
    alias_map = {
        "projectid": "project_id",
        "project-id": "project_id",
        "workspaceid": "workspace_id",
        "workspace-id": "workspace_id",
        "work_space_id": "workspace_id",
        "workspce_id": "workspace_id",
        "dirname": "directory",
        "job_name": "cdm_job_name",
        "cdmjobname": "cdm_job_name",
        "pythonscriptpath": "python_script_path",
        "python_script": "python_script_path",
        "cron": "cron_expression",
        "params": "params",
        "paramsraw": "params",
        "paramsb64": "params_b64",
        "connectionname": "connection_name",
        "connectionid": "connection_id",
    }
    normalized = {}
    for k, v in item.items():
        key = str(k).strip()
        # 去掉常见包裹符和不可见字符
        key = key.strip("'\"`")
        key = key.replace("\ufeff", "").replace("\u200b", "")
        key_lower = key.lower()
        key_lower = key_lower.replace(" ", "").replace("\t", "").replace("\n", "").replace("\r", "")
        # 保留字母数字下划线，增强对异常符号键名的容错
        canonical = re.sub(r"[^a-z0-9_]", "", key_lower)
        mapped = alias_map.get(key_lower) or alias_map.get(canonical) or key
        # 智能兜底：包含 workspace+id / project+id 等字样也自动映射
        if mapped == key:
            if "workspace" in canonical and "id" in canonical:
                mapped = "workspace_id"
            elif "project" in canonical and "id" in canonical:
                mapped = "project_id"
            elif "python" in canonical and "script" in canonical and "path" in canonical:
                mapped = "python_script_path"
            elif canonical == "cron" or ("cron" in canonical and "expression" in canonical):
                mapped = "cron_expression"
            elif "job" in canonical and "name" in canonical and "cdm" in canonical:
                mapped = "cdm_job_name"
        normalized[mapped] = v
    return normalized


def _ensure_required_keys(item):
    """再次兜底补齐关键字段。"""
    if not isinstance(item, dict):
        return item
    fixed = dict(item)
    if "workspace_id" not in fixed:
        for k, v in fixed.items():
            kc = re.sub(r"[^a-z0-9_]", "", str(k).lower())
            if "workspace" in kc and "id" in kc:
                fixed["workspace_id"] = v
                break
    if "project_id" not in fixed:
        for k, v in fixed.items():
            kc = re.sub(r"[^a-z0-9_]", "", str(k).lower())
            if "project" in kc and "id" in kc:
                fixed["project_id"] = v
                break
    return fixed


def _clean_scalar(value):
    """清洗被平台拼接污染的字符串边界。"""
    s = str(value).strip()
    # 去掉常见尾部污染符号（仅边界）
    s = s.strip(" \t\r\n")
    s = s.rstrip("],")
    # 去掉平台偶发拼接污染（例如值末尾带 " jobs:[")
    s = re.sub(r"\s+jobs:\[$", "", s, flags=re.I)
    s = re.sub(r"\s+\}$", "", s)
    s = s.strip()
    return s


def _normalize_cron_expression(value):
    """规范化 cron 表达式：支持使用 '-' 作为分隔符。"""
    s = _clean_scalar(value)
    if not s:
        return s
    if (" " not in s) and ("-" in s):
        parts = [p.strip() for p in s.split("-")]
        parts = [p for p in parts if p != ""]
        if len(parts) >= 6:
            s = " ".join(parts[:6])
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_param_tail(value):
    """清洗参数字符串尾部被拼接的 JSON 结束符。"""
    s = str(value)
    s = s.strip()
    # 仅清理尾部孤立结束符，避免破坏内容
    s = re.sub(r"\s*[\]\}]+\s*$", "", s)
    return s.strip()


def _normalize_id(value):
    """提取并规范化 32 位十六进制 ID。"""
    s = _clean_scalar(value)
    m = re.search(r"[0-9a-fA-F]{32}", s)
    if m:
        return m.group(0).lower()
    return s


def _restore_params_raw(text):
    """将 params_raw 中的 |||json||| 还原成 'json' 形式。"""
    if text is None:
        return ""
    s = str(text)
    return re.sub(r"\|\|\|(.+?)\|\|\|", r"'\1'", s)


def _apply_params_map(text):
    """将 params 中的占位键替换为固定符号。"""
    if text is None:
        return ""
    s = str(text)
    for k, v in PARAMS_MAP.items():
        if k in s:
            s = s.replace(k, v)
    return s


def _normalize_params_tokens(text):
    """将空格分隔参数规范为逐项双引号包裹，避免括号被 shell 误解析。"""
    if text is None:
        return ""
    s = str(text).strip()
    if not s:
        return s
    # 已包含明显成对引号时不重复处理
    if '"' in s:
        return s
    # 仅在看起来是“多段参数”时处理，避免影响单段普通文本
    parts = [p for p in re.split(r"\s+", s) if p]
    if len(parts) >= 2:
        return " ".join(['"%s"' % p.replace('"', '\\"') for p in parts])
    return s


def _resolve_params_input(argv):
    """解析 params：优先命令行余量，其次环境变量（支持 URL 编码）。"""
    params = ""
    if len(argv) > 7:
        params = " ".join(argv[7:]).strip()
    if not params:
        params = (os.environ.get("JOB_PARAMS") or os.environ.get("PARAMS") or "").strip()
    if params and "%" in params:
        try:
            params = urllib_parse.unquote(params)
        except Exception:
            pass
    params = _apply_params_map(params)
    return _normalize_params_tokens(params)


def run_one_job(item, token, index):
    """创建并启动一条作业。返回 (ok: bool, message: str)。"""
    project_id = str(item["project_id"]).strip()
    workspace_id = normalize_workspace_id(item["workspace_id"])
    directory = str(item["directory"]).strip()
    node_name = str(item["node_name"]).strip()
    cdm_job_name = str(item["cdm_job_name"]).strip()
    # 防错位兜底：作业名不应是 32 位 project/workspace id
    if re.match(r"^[0-9a-fA-F]{32}$", cdm_job_name):
        fallback = ""
        if node_name and not re.match(r"^[0-9a-fA-F]{32}$", node_name):
            fallback = node_name
        elif item.get("job_name"):
            fallback = str(item.get("job_name")).strip()
        if fallback:
            LOGGER.warning(
                "[%d] 检测到 cdm_job_name=%s 疑似错位，已回退为 %s",
                index + 1, cdm_job_name, fallback
            )
            cdm_job_name = fallback
    python_script_path = FIXED_PYTHON_SCRIPT_PATH
    cron_expression = item.get("cron_expression") or DEFAULT_CRON_EXPRESSION
    if isinstance(cron_expression, str):
        cron_expression = cron_expression.strip() or DEFAULT_CRON_EXPRESSION
    else:
        cron_expression = str(cron_expression)

    interval_type = item.get("interval_type") or DEFAULT_INTERVAL_TYPE
    owner = item.get("owner") or DEFAULT_OWNER
    description = item.get("description") or ""
    params_text = item.get("params", "")
    if params_text:
        params_text = _restore_params_raw(params_text)
        params_text = _apply_params_map(params_text)
        params_text = _normalize_params_tokens(params_text)
    if (not params_text) and item.get("params_b64"):
        # 按普通字符串处理（不再要求 base64 编码）
        params_text = str(item.get("params_b64"))
        params_text = _apply_params_map(params_text)
        params_text = _normalize_params_tokens(params_text)
    item["_resolved_params"] = params_text
    location_x = item.get("location_x", "705")
    location_y = item.get("location_y", "636")
    node_location_x = item.get("node_location_x", "-207.0")
    node_location_y = item.get("node_location_y", "-253.0")
    start_time = item.get("start_time") or _start_time_from_cron_or_default(cron_expression)

    label = "[%d] project=%s job=%s" % (index + 1, project_id, cdm_job_name)
    try:
        fixed_node_properties = build_node_properties(item.get("node_properties"))
    except ValueError as e:
        return False, str(e)

    create_ok = False
    create_fail_msg = ""
    used_tag = ""
    if fixed_node_properties:
        candidates = [
            {
                "node_type": "Python",
                "single_node_job_type": "Python",
                "properties": fixed_node_properties,
                "tag": "custom(node_properties)",
            }
        ]
    else:
        candidates = generate_node_candidates(python_script_path, item)

    for candidate in candidates:
        job_body = build_batch_cdm_job_body(
            directory=directory,
            node_name=node_name,
            cdm_job_name=cdm_job_name,
            cron_expression=cron_expression,
            node_properties=candidate["properties"],
            node_type=candidate["node_type"],
            single_node_job_type=candidate["single_node_job_type"],
            interval_type=str(interval_type),
            owner=str(owner),
            job_description=description,
            location_x=location_x,
            location_y=location_y,
            node_location_x=node_location_x,
            node_location_y=node_location_y,
            start_time=start_time,
        )

        try:
            code, raw, parsed = create_dev_job(
                project_id=project_id,
                workspace_id=workspace_id,
                x_auth_token=token,
                job_body=job_body,
            )
        except Exception as e:
            LOGGER.exception("%s 创建请求异常 candidate=%s", label, candidate["tag"])
            create_fail_msg = "%s 创建异常: %s" % (label, e)
            continue

        if 200 <= code < 300:
            create_ok = True
            used_tag = candidate["tag"]
            break

        detail = json.dumps(parsed, ensure_ascii=False) if parsed else raw[:500]
        LOGGER.error(
            "%s 创建失败 candidate=%s HTTP=%s %s",
            label, candidate["tag"], code, detail
        )
        err_code = ""
        if isinstance(parsed, dict):
            err_code = str(parsed.get("error_code", "")).strip()
        # 幂等处理：作业已存在则直接走启动
        if code == 400 and err_code == "DLF.0102":
            LOGGER.warning("%s 作业已存在，跳过创建并尝试启动", label)
            create_ok = True
            used_tag = candidate["tag"] + "(exists)"
            break
        if code == 400:
            try:
                sched = job_body.get("schedule", {}).get("cron", {})
                LOGGER.error(
                    "%s 调度参数 expression=%s startTime=%s intervalType=%s",
                    label, sched.get("expression"), sched.get("startTime"), sched.get("intervalType")
                )
            except Exception:
                pass
        create_fail_msg = (
            "%s 创建失败 candidate=%s HTTP=%s detail=%s"
            % (label, candidate["tag"], code, detail)
        )

    if not create_ok:
        return False, create_fail_msg or ("%s 创建失败（所有候选均失败）" % label)

    try:
        code2, raw2, parsed2 = start_dev_job(
            project_id=project_id,
            workspace_id=workspace_id,
            job_name=cdm_job_name,
            x_auth_token=token,
        )
    except Exception as e:
        LOGGER.exception("%s 启动请求异常（已创建）", label)
        return False, "%s 启动异常: %s" % (label, e)

    if not (200 <= code2 < 300):
        detail = json.dumps(parsed2, ensure_ascii=False) if parsed2 else raw2[:500]
        err_code2 = ""
        err_msg2 = ""
        if isinstance(parsed2, dict):
            err_code2 = str(parsed2.get("error_code", "")).strip()
            err_msg2 = str(parsed2.get("error_msg", "")).strip()
        # 幂等处理：作业已在运行，视为成功
        if code2 == 400 and err_code2 == "DLF.3051" and "is running" in err_msg2:
            LOGGER.warning("%s 已在运行，跳过重复启动", label)
            return True, "%s 成功 candidate=%s(already-running)" % (label, used_tag)
        LOGGER.error("%s 启动失败 HTTP=%s %s", label, code2, detail)
        return False, "%s 启动失败 HTTP=%s detail=%s" % (label, code2, detail)

    LOGGER.info("%s 创建并启动成功 candidate=%s", label, used_tag)
    return True, "%s 成功 candidate=%s" % (label, used_tag)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    LOGGER.info("参数解析器版本: %s", PARSER_BUILD)

    if len(sys.argv) < 7:
        print(
            "用法: python create-kingsoft-data-insert-hive-jobs-prod.py "
            "<project_id> <workspace_id> <directory> <node_name> <cdm_job_name> <cron_expression> [params]\n"
            "\n"
            "仅支持单条位置参数模式：\n"
            "project_id workspace_id directory node_name cdm_job_name cron_expression [params]\n"
            "若平台不便给 params 加引号，可不传第7参，改用环境变量 JOB_PARAMS/PARAMS（支持 URL 编码）。",
            file=sys.stderr,
        )
        sys.exit(2)

    data = [{
        "project_id": sys.argv[1],
        "workspace_id": sys.argv[2],
        "directory": sys.argv[3],
        "node_name": sys.argv[4],
        "cdm_job_name": sys.argv[5],
        "cron_expression": sys.argv[6],
        "params": _resolve_params_input(sys.argv),
    }]

    if len(data) == 0:
        LOGGER.error("JSON 数组为空")
        sys.exit(2)

    # 主流程再清洗一次：同 project/workspace/node 下，优先保留带 cdm_job_name 的完整记录
    if isinstance(data, list) and len(data) > 1:
        rich_keys = set()
        for it in data:
            if not isinstance(it, dict):
                continue
            pid = str(it.get("project_id", "")).strip()
            wid = str(it.get("workspace_id", "")).strip()
            node = str(it.get("node_name", "")).strip()
            cdm = str(it.get("cdm_job_name", "")).strip()
            if pid and wid and node and cdm:
                rich_keys.add((pid, wid, node))
        if rich_keys:
            filtered = []
            for idx0, it in enumerate(data):
                if not isinstance(it, dict):
                    filtered.append(it)
                    continue
                pid = str(it.get("project_id", "")).strip()
                wid = str(it.get("workspace_id", "")).strip()
                node = str(it.get("node_name", "")).strip()
                cdm = str(it.get("cdm_job_name", "")).strip()
                if (pid, wid, node) in rich_keys and not cdm:
                    LOGGER.warning(
                        "检测到同一节点存在完整记录，第 %s 条缺少 cdm_job_name，已跳过",
                        idx0 + 1,
                    )
                    continue
                filtered.append(it)
            data = filtered

    normalized_data = []
    for i, item in enumerate(data):
        # 终极兜底：平台脏分片会生成“仅 project_id”的尾巴项，直接跳过
        if isinstance(item, dict):
            non_empty_keys = {k for k, v in item.items() if str(v).strip() != ""}
            if non_empty_keys == {"project_id"} and len(data) > 1:
                LOGGER.warning("第 %d 条仅包含 project_id，判定为脏分片，已跳过", i + 1)
                continue
        try:
            normalized_item = validate_job_item(item, i)
            normalized_data.append(normalized_item)
        except ValueError as e:
            LOGGER.error("%s", e)
            sys.exit(2)

    token_cache = {}
    failures = []

    for i, item in enumerate(normalized_data):
        pid = str(item["project_id"]).strip()
        if pid not in token_cache:
            try:
                token_cache[pid] = get_x_auth_token(pid)
            except (urllib_error.URLError, ValueError) as e:
                LOGGER.exception("获取 project_id=%s 的 Token 失败", pid)
                failures.append((i, "Token 失败: %s" % e))
                continue

        ok, msg = run_one_job(item, token_cache[pid], i)
        print(msg)
        if not ok:
            failures.append((i, msg))

    if failures:
        LOGGER.error("完成：共 %d 条，失败 %d 条", len(normalized_data), len(failures))
        sys.exit(1)
    LOGGER.info("完成：共 %d 条，全部成功", len(normalized_data))
    sys.exit(0)


def _read_stdin_text():
    """读取 stdin 文本（兼容在线执行器交互输入）。"""
    try:
        if hasattr(sys.stdin, "isatty") and sys.stdin.isatty():
            return ""
        text = sys.stdin.read()
        return text.strip() if text else ""
    except Exception:
        return ""


def _input_preview(text, limit=160):
    """日志预览：避免整段入参刷屏。"""
    if text is None:
        return "<None>"
    s = str(text).replace("\n", "\\n").replace("\r", "\\r")
    if len(s) <= limit:
        return s
    return s[:limit] + "...(truncated)"


if __name__ == "__main__":
    main()
