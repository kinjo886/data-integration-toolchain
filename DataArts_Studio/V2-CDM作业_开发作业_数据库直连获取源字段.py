# -*- coding: utf-8 -*-
"""合并脚本：CDM作业创建 + 开发任务创建。

先执行CDM作业完整流程，然后执行开发任务创建流程。

命令行参数说明（共22个位置参数）：
    === CDM作业参数（脚本1，11个参数）===
    1. project_id      - 项目 ID（必填）
    2. cluster_id      - CDM集群 ID（必填）
    3. group_id        - 作业分组ID（可选，默认: "1"）
    4. group_name      - 作业分组名称（可选，默认: "DEFAULT"）
    5. job_name        - CDM作业名称（必填）
    6. from_link_name  - 源连接名称（必填）
    7. schema_name     - 源数据库schema名称（必填）
    8. from_table_name - 源数据库表名（必填）
    9. to_database     - 目标Hive数据库名称（必填）
    10. to_table_name   - 目标Hive表名（必填）
    11. data_source    - 数据来源标识（可选，默认: "<示例委办局>-i<示例业务场景>-i本市智慧养老系统"）

    === 开发任务参数（脚本2，11个去重后参数）===
    12. workspace_id   - 工作空间 ID（可选，传空字符串""表示不使用）
    13. dev_name       - 开发任务名称（必填）
    14. directory     - 作业目录路径（必填）
    15. node_name     - 节点名称（必填）
    16. cluster_name  - CDM集群名称（必填）
    17. cdm_job_name  - CDM作业名称（节点属性中的jobName，必填）
    18. cron_expression - 调度Cron表达式（必填）
    19. interval_type  - 调度间隔类型（可选，默认: days）
    20. owner          - 作业所有者（可选，默认: liguozhuang）
    21. description    - 作业描述（可选，默认: 空）
    22. location_x     - 画布X坐标（可选，默认: 705）
    23. location_y     - 画布Y坐标（可选，默认: 636）

完整流程：
    阶段1 - CDM作业流程：
        1. 创建CDM作业
        2. 执行CDM作业（第一次）
        3. 查询作业执行状态（轮询直到SUCCEEDED）
        4. 从Hive获取建表语句
        5. 修改建表语句（添加默认字段和分区）
        6. 删除表并重建（执行修改后的DDL）
        7. 调用修改作业接口（更新字段列表）
        8. 再次执行CDM作业
        9. 查询作业执行状态（轮询直到SUCCEEDED）

    阶段2 - 开发任务流程：
        10. 创建开发任务作业
        11. 启动作业

示例：
    python 3CDM作业+开发作业.py \
        "2d06fe3f78324f7d9b45abdc4db9e2a8" \
        "3bdb7eec-5a76-46f9-a0ee-61cbecf20962" \
        "1" "DEFAULT" \
        "ONM_SZLG_MSSQ_CASE_APPEAL_0108" \
        "DM" "LGYWTG" \
        "ONM_SZLG_MSSQ_CASE_APPEAL" \
        "ods_lgbs" "ONM_SZLG_MSSQ_CASE_APPEAL_0108" \
        "<示例委办局>-i<示例业务场景>-i本市智慧养老系统" \
        "" \
        "dev_job_ods_test" "/测试目录" "test_node" \
        "cdm-7551" "ONM_SZLG_MSSQ_CASE_APPEAL_0108" \
        "0 20 1 * * ?"
"""

from __future__ import print_function

import importlib
import json
import logging
import os
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# ========== 日志配置 ==========
LOGGER = logging.getLogger(__name__)
ssl._create_default_https_context = ssl._create_unverified_context

# ========== IAM 鉴权配置 ==========
IAM_URL = "https://<INTERNAL_IAM_HOST>/v3/auth/tokens"
IAM_USERNAME = "admin_user"
IAM_PASSWORD = "<YOUR_IAM_PASSWORD>"
IAM_DOMAIN_NAME = "政务大数据治理平台"

# ========== CDM API 配置 ==========
CDM_BASE_URL = "https://cdm.example.gov.cn"

# ========== DataArts Studio API 配置 ==========
DATAARTS_BASE_URL = "https://dayu-dlf.<INTERNAL_REGION>.example.gov.cn"

# ========== Hive配置 ==========
HIVE_CONFIG = {
    "host": "<INTERNAL_HIVE_HOST>",
    "port": 21066,
    "username": "admin_user",
    "database": None,
    "auth": "KERBEROS",
    "kerberos_service_name": "hive",
    "krbhost": "<INTERNAL_HIVE_HOSTNAME>",
}

# 新增默认字段配置
DEFAULT_FIELDS = [
    {"name": "lgdsj_timeflag", "type": "timestamp", "comment": None},
    {"name": "lgdsj_data_source", "type": "string", "comment": "来源部门与系统名称"},
    {"name": "lgdsj_load_time", "type": "timestamp", "comment": "写入时间戳"},
]

# 分区字段配置
PARTITION_FIELD = {
    "name": "<INTERNAL_DATASET>",
    "type": "string",
    "comment": "分区字段YYYYMMDD"
}

# 源数据库连接配置文件（运行环境为Linux服务器）
DATA_SOURCE_CONFIG_PATH = "/data/data_source/data.json"


# ==================== CDM 作业相关函数（来自脚本1）====================

def build_detail_url(project_id, cluster_id):
    """构建 CDM 作业接口 URL。"""
    return "{}/v1.1/{}/clusters/{}/cdm/job".format(CDM_BASE_URL, project_id, cluster_id)


def build_start_job_url(project_id, cluster_id, job_name):
    """构建 CDM 执行作业接口 URL。"""
    encoded_job_name = urllib.parse.quote(job_name, safe='')
    return "{}/v1.1/{}/clusters/{}/cdm/job/{}/start".format(
        CDM_BASE_URL, project_id, cluster_id, encoded_job_name
    )


def build_update_job_url(project_id, cluster_id, job_name):
    """构建 CDM 修改作业接口 URL。"""
    encoded_job_name = urllib.parse.quote(job_name, safe='')
    return "{}/v1.1/{}/clusters/{}/cdm/job/{}".format(
        CDM_BASE_URL, project_id, cluster_id, encoded_job_name
    )


def build_status_job_url(project_id, cluster_id, job_name):
    """构建 CDM 查询作业状态接口 URL。"""
    encoded_job_name = urllib.parse.quote(job_name, safe='')
    return "{}/v1.1/{}/clusters/{}/cdm/job/{}/status".format(
        CDM_BASE_URL, project_id, cluster_id, encoded_job_name
    )


def build_cdm_job_payload(
    project_id,
    from_table_name,
    to_table_name,
    from_link_name,
    job_name,
    schema_name,
    to_database,
    column_list=None,
    is_update=False,
    group_id="1",
    group_name="DEFAULT",
    data_source=None,
):
    """构建 CDM 作业请求体。"""
    if data_source is None:
        data_source = "<示例委办局>-i<示例业务场景>-i本市智慧养老系统"

    default_extra_fields = ["lgdsj_timeflag", "lgdsj_data_source", "lgdsj_load_time", "<INTERNAL_DATASET>"]

    to_extended_fields_create = "<INTERNAL_B64>"
    to_extended_fields_update = "<INTERNAL_B64>"

    to_extended_fields_value = to_extended_fields_update if is_update else to_extended_fields_create

    from_job_config_inputs = [
        {"name": "fromJobConfig.useSql", "value": "false"},
        {"name": "fromJobConfig.schemaName", "value": schema_name},
        {"name": "fromJobConfig.tableName", "value": from_table_name},
        {"name": "fromJobConfig.incrMigration", "value": "false"},
        {"name": "fromJobConfig.keyAtLeastOneZero", "value": "false"},
        {"name": "fromJobConfig.allowNullValueInPartitionColumn", "value": "true"},
        {"name": "fromJobConfig.cdc", "value": "false"},
        {"name": "fromJobConfig.createOutTable", "value": "false"},
        {"name": "fromJobConfig.enableWriteLobToString", "value": "false"},
        {"name": "fromJobConfig.writeLobDataAsFile", "value": "false"},
        {"name": "fromJobConfig.encodingForBinary", "value": "ISO_8859_1"},
        {"name": "fromJobConfig.usePattern", "value": "ORACLE"},
    ]

    to_job_config_inputs = [
        {"name": "toJobConfig.hive", "value": "hive"},
        {"name": "toJobConfig.database", "value": to_database},
        {"name": "toJobConfig.table", "value": to_table_name},
        {"name": "toJobConfig.tablePreparation", "value": "CREATE_WHEN_NOT_EXIST"},
        {"name": "toJobConfig.convertNull", "value": "TO_NULL"},
        {"name": "toJobConfig.csvDelimPolicy", "value": "DROP"},
    ]

    to_extended_config = None

    if is_update:
        to_job_config_inputs.append({"name": "toJobConfig.shouldClearTable", "value": "true"})
        to_job_config_inputs.append({"name": "toJobConfig.clearDataMode", "value": "TRUNCATE"})

        to_extended_config = {
            "name": "toJobConfig.extendedFields",
            "value": to_extended_fields_value,
        }

        if column_list is None:
            LOGGER.error("[cdm_job] 调用修改任务接口时column_list不能为空")
            raise ValueError("修改作业时必须提供column_list参数，不能为None")
        else:
            existing_fields = set(column_list.split("&"))
            missing_fields = [f for f in default_extra_fields if f not in existing_fields]
            if missing_fields:
                column_list = column_list + "&" + "&".join(missing_fields)
                LOGGER.info("[cdm_job] 字段列表中自动添加缺失字段: %s", "&".join(missing_fields))

        from_job_config_inputs.append({"name": "fromJobConfig.columnList", "value": column_list})
        from_job_config_inputs.append({
            "name": "fromJobConfig.sampleValueColumn",
            "value": "lgdsj_timeflag:${dateformat(yyyy-MM-dd HH:mm:ss)}&lgdsj_data_source:"+data_source+"&lgdsj_load_time:${dateformat(yyyy-MM-dd HH:mm:ss)}&<INTERNAL_DATASET>:'${dateformat(yyyyMMdd,-1,DAY)}'"
        })

        to_job_config_inputs.append({"name": "toJobConfig.columnList", "value": column_list})
    else:
        to_job_config_inputs.append({"name": "toJobConfig.shouldClearTable", "value": "false"})

    to_config_values = {
        "configs": [
            {
                "inputs": to_job_config_inputs,
                "name": "toJobConfig",
            }
        ],
    }
    LOGGER.info("[cdm_job] to_config_values：to_extended_config: %s", to_extended_config)
    if to_extended_config:
        to_config_values["extended-configs"] = to_extended_config

    LOGGER.info("[cdm_job] to_config_values: %s", to_config_values)
    return {
        "jobs": [
            {
                "job_type": "NORMAL_JOB",
                "to-config-values": to_config_values,
                "from-config-values": {
                    "configs": [
                        {
                            "inputs": from_job_config_inputs,
                            "name": "fromJobConfig",
                        }
                    ],
                    "extended-configs": {
                        "name": "fromJobConfig.extendedFields",
                        "value": "<INTERNAL_B64>",
                    },
                },
                "from-connector-name": "generic-jdbc-connector",
                "to-link-name": "MRS-Hive",
                "driver-config-values": {
                    "configs": [
                        {
                            "inputs": [
                                {"name": "throttlingConfig.concurrentSubJobs", "value": "10"},
                                {"name": "throttlingConfig.numExtractors", "value": "1"},
                                {"name": "throttlingConfig.numSplits", "value": "1"},
                                {"name": "throttlingConfig.splitRetryTime", "value": "0"},
                                {"name": "throttlingConfig.submitToCluster", "value": "false"},
                                {"name": "throttlingConfig.numLoaders", "value": "1"},
                                {"name": "throttlingConfig.recordDirtyData", "value": "false"},
                                {"name": "throttlingConfig.maxErrorRecords", "value": "10"},
                                {"name": "throttlingConfig.throttling", "value": "false"},
                                {"name": "throttlingConfig.byteRate", "value": "10"},
                                {"name": "throttlingConfig.channelCapacityMb", "value": "64"},
                                {"name": "throttlingConfig.recordRate", "value": "100000"},
                            ],
                            "name": "throttlingConfig",
                        },
                        {"inputs": [], "name": "jarConfig"},
                        {
                            "inputs": [
                                {"name": "schedulerConfig.isSchedulerJob", "value": "false"},
                                {"name": "schedulerConfig.disposableType", "value": "NONE"},
                            ],
                            "name": "schedulerConfig",
                        },
                        {
                            "inputs": [],
                            "name": "transformConfig",
                        },
                        {
                            "inputs": [
                                {"name": "smnConfig.isNeedNotification", "value": "false"}
                            ],
                            "name": "smnConfig",
                        },
                        {
                            "inputs": [
                                {"name": "retryJobConfig.retryJobType", "value": "NONE"}
                            ],
                            "name": "retryJobConfig",
                        },
                        {
                            "inputs": [
                                {"name": "groupJobConfig.groupId", "value": group_id},
                                {"name": "groupJobConfig.groupName", "value": group_name},
                            ],
                            "name": "groupJobConfig",
                        },
                        {"inputs": [], "name": "partitionConfig"},
                    ]
                },
                "to-connector-name": "hive-connector",
                "from-link-name": from_link_name,
                "name": job_name,
            }
        ]
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
    data = json.dumps(token_body, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(IAM_URL, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        token = (response.getheader("X-Subject-Token") or "").strip()
        if not token:
            raise ValueError("鉴权成功但未获取到 X-Subject-Token。")
        return token


def create_cdm_job(body, x_auth_token, detail_url, project_id, timeout_sec=120.0):
    """POST 创建 CDM 作业。"""
    detail_headers = {
        "X-Auth-Token": x_auth_token.strip(),
        "workspace": project_id,
        "Content-Type": "application/json;charset=UTF-8",
        "X-Language": "zh-cn",
    }

    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        detail_url, data=data, headers=detail_headers, method="POST"
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(response.getcode())
            parsed = None
            try:
                parsed = json.loads(raw) if raw.strip() else None
            except json.JSONDecodeError:
                parsed = None
            return code, raw, parsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed = None
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = None
        LOGGER.error(
            "[cdm_job] HTTP 错误 code=%s url=%s body=%s",
            exc.code, detail_url, raw[:2000],
        )
        return int(exc.code), raw, parsed


def start_cdm_job(x_auth_token, start_url, project_id, timeout_sec=120.0):
    """POST 执行 CDM 作业。"""
    detail_headers = {
        "X-Auth-Token": x_auth_token.strip(),
        "workspace": project_id,
        "Content-Type": "application/json;charset=UTF-8",
        "X-Language": "zh-cn",
    }

    request = urllib.request.Request(
        start_url, headers=detail_headers, method="PUT"
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(response.getcode())
            parsed = None
            try:
                parsed = json.loads(raw) if raw.strip() else None
            except json.JSONDecodeError:
                parsed = None
            return code, raw, parsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed = None
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = None
        LOGGER.error(
            "[cdm_job] HTTP 错误 code=%s url=%s body=%s",
            exc.code, start_url, raw[:2000],
        )
        return int(exc.code), raw, parsed


def update_cdm_job(body, x_auth_token, update_url, project_id, timeout_sec=120.0):
    """PUT 修改 CDM 作业。"""
    detail_headers = {
        "X-Auth-Token": x_auth_token.strip(),
        "workspace": project_id,
        "Content-Type": "application/json;charset=UTF-8",
        "X-Language": "zh-cn",
    }

    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    LOGGER.info("[update_cdm_job] 参数: %s", data)
    request = urllib.request.Request(
        update_url, data=data, headers=detail_headers, method="PUT"
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(response.getcode())
            parsed = None
            try:
                parsed = json.loads(raw) if raw.strip() else None
            except json.JSONDecodeError:
                parsed = None
            return code, raw, parsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed = None
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = None
        LOGGER.error(
            "[cdm_job] HTTP 错误 code=%s url=%s body=%s",
            exc.code, update_url, raw[:2000],
        )
        return int(exc.code), raw, parsed


def get_cdm_job_status(x_auth_token, status_url, project_id, timeout_sec=30.0):
    """GET 查询 CDM 作业执行状态。"""
    detail_headers = {
        "X-Auth-Token": x_auth_token.strip(),
        "workspace": project_id,
        "Content-Type": "application/json;charset=UTF-8",
        "X-Language": "zh-cn",
    }

    request = urllib.request.Request(
        status_url, headers=detail_headers, method="GET"
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(response.getcode())
            parsed = None
            try:
                parsed = json.loads(raw) if raw.strip() else None
            except json.JSONDecodeError:
                parsed = None
            return code, raw, parsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed = None
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = None
        LOGGER.error(
            "[cdm_job] HTTP 错误 code=%s url=%s body=%s",
            exc.code, status_url, raw[:2000],
        )
        return int(exc.code), raw, parsed


def wait_for_job_completion(
    x_auth_token, status_url, project_id, job_name,
    check_interval=10.0, max_wait_time=3600.0
):
    """循环查询CDM作业执行状态，直到任务完成或失败。"""
    start_time = time.time()
    check_count = 0

    LOGGER.info("[cdm_job] 开始轮询作业执行状态: %s", job_name)

    while True:
        check_count += 1
        elapsed_time = time.time() - start_time

        if elapsed_time > max_wait_time:
            LOGGER.error(
                "[cdm_job] 作业执行超过最大等待时间 %.0f 秒，停止轮询",
                max_wait_time
            )
            return 3

        try:
            code, raw, parsed = get_cdm_job_status(
                x_auth_token=x_auth_token,
                status_url=status_url,
                project_id=project_id,
            )
        except urllib.error.URLError as exc:
            LOGGER.exception("[cdm_job] 查询作业状态网络请求失败 url=%s", status_url)
            return 1

        if not (200 <= code < 300):
            LOGGER.error("[cdm_job] 查询作业状态失败 HTTP %s", code)
            return 1

        status = None
        if parsed and isinstance(parsed, dict) and "submissions" in parsed:
            submissions = parsed.get("submissions", [])
            if submissions and len(submissions) > 0:
                latest_submission = submissions[0]
                status = latest_submission.get("status")
                progress = latest_submission.get("progress", 0)
                LOGGER.info(
                    "[cdm_job] 作业状态查询 #%d: status=%s, progress=%s%%",
                    check_count, status, progress * 100
                )

        if status is None:
            LOGGER.warning("[cdm_job] 无法从响应中解析作业状态，原始响应: %s", raw[:500])
        elif status in ("RUNNING", "BOOTING", "PENDING"):
            LOGGER.info("[cdm_job] 作业正在%s中，%.0f秒后再次查询...",
                       "启动" if status == "BOOTING" else "执行", check_interval)
            time.sleep(check_interval)
            continue
        elif status == "SUCCEEDED":
            LOGGER.info("[cdm_job] 作业执行成功，总耗时 %.0f 秒", elapsed_time)
            return 0
        else:
            LOGGER.error(
                "[cdm_job] 作业执行异常，状态=%s，终止流程。完整响应: %s",
                status,
                json.dumps(parsed, ensure_ascii=False, indent=2) if parsed else raw
            )
            return 2


def init_env_and_auth():
    """初始化环境变量+Kerberos认证。"""
    LOGGER.info("[hive] 开始执行：环境变量加载 + Kerberos认证")

    LOGGER.info("[hive] 加载Hadoop环境变量（/opt/hadoopclient/bigdata_env）")
    env_result = subprocess.run(
        "source /opt/hadoopclient/bigdata_env && env",
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
        text=True,
    )
    if env_result.returncode != 0:
        raise RuntimeError(
            "Hadoop环境变量加载失败，返回码：{}".format(env_result.returncode)
        )
    os.environ.update(
        dict(
            line.split("=", 1)
            for line in env_result.stdout.split("\n")
            if "=" in line and not line.startswith("#")
        )
    )
    LOGGER.info("[hive] Hadoop环境变量加载完成，共加载 %d 个环境变量", len(os.environ))

    username = HIVE_CONFIG.get("username", "admin_user")
    LOGGER.info("[hive] 执行Kerberos认证（用户：%s）", username)

    password = os.environ.get("KERBEROS_PASSWORD", "<YOUR_IAM_PASSWORD>")

    if password:
        kinit_result = subprocess.run(
            "echo '{}' | kinit {}".format(password, username),
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    else:
        LOGGER.warning("[hive] 未设置KERBEROS_PASSWORD环境变量，尝试直接kinit")
        kinit_result = subprocess.run(
            "kinit {}".format(username),
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    if kinit_result.returncode != 0:
        error_msg = kinit_result.stderr.strip()
        LOGGER.error("[hive] Kerberos认证失败: %s", error_msg)
        raise RuntimeError(
            "Kerberos认证失败。请设置环境变量KERBEROS_PASSWORD后重试，"
            "或手动执行: kinit {}\n错误: {}".format(username, error_msg)
        )

    LOGGER.info("[hive] Kerberos认证成功")


def get_hive_ddl(table_name, to_database):
    """从Hive库中获取指定表的建表语句。"""
    try:
        from pyhive import hive
    except ImportError:
        raise ImportError(
            "获取Hive DDL需要PyHive库。请执行: pip install pyhive[hive] thrift sasl"
        )

    conn = None
    cursor = None
    try:
        init_env_and_auth()

        LOGGER.info("[hive] 正在连接Hive: %s:%s", HIVE_CONFIG["host"], HIVE_CONFIG["port"])

        conn_params = {
            "host": HIVE_CONFIG["host"],
            "port": HIVE_CONFIG["port"],
            "username": HIVE_CONFIG["username"],
            "database": to_database,
        }

        if HIVE_CONFIG.get("auth") == "KERBEROS":
            conn_params["auth"] = "KERBEROS"
            conn_params["kerberos_service_name"] = HIVE_CONFIG["kerberos_service_name"]
            if HIVE_CONFIG.get("krbhost"):
                conn_params["krbhost"] = HIVE_CONFIG["krbhost"]
        LOGGER.info("[hive] 连接参数: %s", conn_params)
        conn = hive.Connection(**conn_params)
        LOGGER.info("[hive] 连接成功")
        cursor = conn.cursor()

        sql = "SHOW CREATE TABLE {}".format(table_name)
        LOGGER.info("[hive] 执行SQL: %s", sql)
        cursor.execute(sql)

        result = cursor.fetchall()
        ddl = "\n".join([row[0] for row in result])

        LOGGER.info("[hive] 成功获取表 %s 的建表语句", table_name)
        return ddl

    except Exception as exc:
        error_msg = str(exc)
        LOGGER.error("[hive] 获取建表语句失败: %s", error_msg)

        hive_host = HIVE_CONFIG["host"]
        krb_host = HIVE_CONFIG.get("krbhost", hive_host)

        if "Name or service not known" in error_msg or "failed to resolve" in error_msg:
            LOGGER.error("[hive] DNS解析错误诊断:")
            LOGGER.error("[hive] 无法解析域名: %s", krb_host)
            LOGGER.error("[hive] 当前配置: host=%s (IP), krbhost=%s (Kerberos域名)", hive_host, krb_host)
            LOGGER.error("[hive] 解决方案:")
            LOGGER.error("[hive] 1. 在服务器上配置hosts解析，执行以下命令:")
            LOGGER.error('[hive]    sudo sh -c \'echo "<INTERNAL_HIVE_HOST> %s" >> /etc/hosts\'', krb_host)
            LOGGER.error("[hive] 2. 或者联系网络管理员配置DNS解析")
            LOGGER.error("[hive] 3. 验证连接: ping %s", krb_host)

        if "GSSAPI" in error_msg or "SASL" in error_msg or "serverFQDN" in error_msg:
            LOGGER.error("[hive] Kerberos认证错误诊断:")
            LOGGER.error("[hive] 当前配置: host=%s (IP), krbhost=%s (Kerberos域名)", hive_host, krb_host)
            LOGGER.error("[hive] 错误原因: Kerberos认证需要正确的域名解析")
            LOGGER.error("[hive] 解决方案:")
            LOGGER.error("[hive] 1. 确保 /etc/hosts 配置正确:")
            LOGGER.error('[hive]    <INTERNAL_HIVE_HOST> %s', krb_host)
            LOGGER.error("[hive] 2. 验证Kerberos ticket已获取: klist")
            LOGGER.error("[hive] 3. 重新获取ticket: kinit -kt /path/to/keytab %s", HIVE_CONFIG["username"])
            LOGGER.error("[hive] 4. 确保/etc/krb5.conf配置正确")
            LOGGER.error("[hive] 5. 验证principal: kinit后执行 'kvno %s/%s'",
                       HIVE_CONFIG["kerberos_service_name"], krb_host)

        if "No Kerberos credentials available" in error_msg or "KCM server found" in error_msg:
            LOGGER.error("[hive] Kerberos Ticket错误诊断:")
            LOGGER.error("[hive] 错误原因: 没有有效的Kerberos ticket")
            LOGGER.error("[hive] 解决方案:")
            LOGGER.error("[hive] 1. 手动执行kinit获取ticket:")
            LOGGER.error("[hive]    kinit %s", HIVE_CONFIG["username"])
            LOGGER.error("[hive] 2. 或使用keytab文件:")
            LOGGER.error("[hive]    kinit -kt /path/to/%s.keytab %s",
                       HIVE_CONFIG["username"], HIVE_CONFIG["username"])
            LOGGER.error("[hive] 3. 验证ticket是否获取成功:")
            LOGGER.error("[hive]    klist")
            LOGGER.error("[hive] 4. 如果kinit失败，检查/etc/krb5.conf配置:")
            LOGGER.error("[hive]    - 确认default_realm配置正确")
            LOGGER.error("[hive]    - 确认KDC服务器地址正确")

        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def execute_hive_ddl(to_database, table_name, create_ddl):
    """在Hive中执行DDL操作：删除表并重新创建。"""
    try:
        from pyhive import hive
    except ImportError:
        raise ImportError(
            "执行Hive DDL需要PyHive库。请执行: pip install pyhive[hive] thrift sasl"
        )

    conn = None
    cursor = None
    try:
        init_env_and_auth()

        LOGGER.info("[hive] 正在连接Hive执行DDL: %s:%s", HIVE_CONFIG["host"], HIVE_CONFIG["port"])

        conn_params = {
            "host": HIVE_CONFIG["host"],
            "port": HIVE_CONFIG["port"],
            "username": HIVE_CONFIG["username"],
            "database": to_database,
        }

        if HIVE_CONFIG.get("auth") == "KERBEROS":
            conn_params["auth"] = "KERBEROS"
            conn_params["kerberos_service_name"] = HIVE_CONFIG["kerberos_service_name"]
            if HIVE_CONFIG.get("krbhost"):
                conn_params["krbhost"] = HIVE_CONFIG["krbhost"]

        conn = hive.Connection(**conn_params)
        cursor = conn.cursor()

        drop_sql = "DROP TABLE IF EXISTS {}".format(table_name)
        LOGGER.info("[hive] 执行DDL: %s", drop_sql)
        cursor.execute(drop_sql)
        LOGGER.info("[hive] 表 %s 删除成功", table_name)

        LOGGER.info("[hive] 执行创建表DDL...")
        cursor.execute(create_ddl)
        LOGGER.info("[hive] 表 %s 创建成功", table_name)

        return True

    except Exception as exc:
        LOGGER.error("[hive] 执行DDL失败: %s", exc)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def modify_hive_ddl(ddl):
    """修改Hive建表语句，添加默认字段和分区。"""
    ddl = ddl.strip().rstrip(';').strip()

    default_fields_sql = []
    for field in DEFAULT_FIELDS:
        field_def = "{} {}".format(field["name"], field["type"])
        if field["comment"]:
            field_def += " COMMENT '{}'".format(field["comment"])
        default_fields_sql.append(field_def)

    default_fields_str = ",\n".join(default_fields_sql)

    create_match = re.match(r'(CREATE\s+TABLE\s+[^\(]+\()', ddl, re.IGNORECASE)
    if not create_match:
        raise ValueError("无法匹配CREATE TABLE语句")

    first_paren_pos = create_match.end() - 1

    paren_count = 0
    last_paren_pos = -1
    in_single_quote = False
    i = first_paren_pos
    while i < len(ddl):
        current_char = ddl[i]

        # Hive字符串字面量内的括号不参与结构匹配，避免 COMMENT 文本误触发截断。
        if current_char == "'":
            if in_single_quote:
                # Hive中单引号转义通常是两个单引号 ''。
                if i + 1 < len(ddl) and ddl[i + 1] == "'":
                    i += 1
                else:
                    in_single_quote = False
            else:
                in_single_quote = True
            i += 1
            continue

        if not in_single_quote:
            if current_char == '(':
                paren_count += 1
            elif current_char == ')':
                paren_count -= 1
                if paren_count == 0:
                    last_paren_pos = i
                    break

        i += 1

    if last_paren_pos == -1:
        raise ValueError("无法找到匹配的右括号")

    columns_section = ddl[first_paren_pos + 1:last_paren_pos]

    columns_section = columns_section.rstrip()
    if not columns_section.endswith(','):
        columns_section += ','

    new_columns_section = columns_section + '\n' + default_fields_str

    modified_ddl = ddl[:first_paren_pos + 1] + '\n' + new_columns_section + '\n)'

    partition_sql = "PARTITIONED BY ({} {}".format(
        PARTITION_FIELD["name"], PARTITION_FIELD["type"]
    )
    if PARTITION_FIELD["comment"]:
        partition_sql += " COMMENT '{}'".format(PARTITION_FIELD["comment"])
    partition_sql += ")"

    modified_ddl += "\n" + partition_sql + "\nSTORED AS ORC"

    return modified_ddl


def load_data_source_configs(config_path=None):
    """读取数据源连接配置文件。

    Args:
        config_path: 配置文件路径。默认使用DATA_SOURCE_CONFIG_PATH，
            并支持通过环境变量DATA_SOURCE_CONFIG_PATH覆盖。

    Returns:
        数据源配置字典，key为连接名称（from_link_name）。

    Raises:
        ValueError: 配置内容不是JSON对象。
        IOError: 配置文件读取失败。
    """
    path = config_path or os.environ.get("DATA_SOURCE_CONFIG_PATH", DATA_SOURCE_CONFIG_PATH)
    LOGGER.info("[data_source] 读取数据源配置文件: %s", path)
    try:
        with open(path, "r") as file_obj:
            data = json.load(file_obj)
    except Exception:
        LOGGER.exception("[data_source] 读取配置文件失败: path=%s", path)
        raise IOError("读取数据源配置文件失败: {}".format(path))

    if not isinstance(data, dict):
        LOGGER.error("[data_source] 配置格式错误，根节点必须是对象: path=%s", path)
        raise ValueError("数据源配置文件格式错误，根节点必须是对象")

    return data


def get_connection_info_by_link_name(from_link_name, config_path=None):
    """根据from_link_name匹配数据源连接信息。

    Args:
        from_link_name: 源连接名称。
        config_path: 配置文件路径，可选。

    Returns:
        单个连接配置字典。

    Raises:
        KeyError: 未找到对应连接名称。
        ValueError: 连接信息结构不合法。
    """
    configs = load_data_source_configs(config_path=config_path)
    connection_info = configs.get(from_link_name)
    if connection_info is None:
        LOGGER.error("[data_source] 未找到连接配置: from_link_name=%s", from_link_name)
        raise KeyError("未找到数据源连接配置: {}".format(from_link_name))
    if not isinstance(connection_info, dict):
        LOGGER.error("[data_source] 连接配置格式错误: from_link_name=%s", from_link_name)
        raise ValueError("连接配置格式错误: {}".format(from_link_name))
    return connection_info


def get_oracle_table_columns(connection_info, schema_name, table_name):
    """查询Oracle表结构，按字段顺序返回字段名列表和实际表名。

    Args:
        connection_info: Oracle连接配置，包含ip、port、database、username、password。
        schema_name: 源schema名称。
        table_name: 源表名。

    Returns:
        二元组(columns, actual_table_name)：
            columns: 字段名列表，按COLUMN_ID排序。
            actual_table_name: 数据库中查询到的实际表名（保留数据库中的大小写）。

    Raises:
        ImportError: 未安装oracledb/cx_Oracle驱动。
        RuntimeError: 查询失败或结果为空。
    """
    ip = connection_info.get("ip")
    port = str(connection_info.get("port", "1521"))
    service_name = connection_info.get("database")
    username = connection_info.get("username")
    password = connection_info.get("password")
    if not all([ip, port, service_name, username, password]):
        raise ValueError("Oracle连接配置缺少必要字段(ip/port/database/username/password)")

    try:
        db_module = importlib.import_module("oracledb")
    except ImportError:
        try:
            db_module = importlib.import_module("cx_Oracle")
        except ImportError:
            raise ImportError("请安装 Oracle 驱动：pip install oracledb 或 pip install cx_Oracle")

    dsn = db_module.makedsn(ip, int(port), service_name=service_name)
    sql = (
        "SELECT COLUMN_NAME, TABLE_NAME "
        "FROM ALL_TAB_COLUMNS "
        "WHERE UPPER(OWNER) = UPPER(:owner) AND UPPER(TABLE_NAME) = UPPER(:table_name) "
        "ORDER BY COLUMN_ID"
    )

    connection = None
    cursor = None
    try:
        connection = db_module.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()
        cursor.execute(
            sql,
            owner=schema_name.upper(),
            table_name=table_name.upper(),
        )
        rows = cursor.fetchall()
        columns = [row[0] for row in rows if row and row[0]]
        actual_table_name = rows[0][1] if rows and len(rows[0]) > 1 else table_name
        if not columns:
            raise RuntimeError(
                "Oracle未查询到字段，请检查schema/table是否正确: {}.{}".format(schema_name, table_name)
            )
        return columns, actual_table_name
    except Exception:
        LOGGER.exception(
            "[data_source] Oracle查询表结构失败: ip=%s port=%s schema=%s table=%s",
            ip, port, schema_name, table_name
        )
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_mysql_table_columns(connection_info, schema_name, table_name):
    """查询MySQL表结构，按字段顺序返回字段名列表和实际表名。

    Args:
        connection_info: MySQL连接配置，包含ip、port、database(可选)、username、password。
        schema_name: 源库名（TABLE_SCHEMA）；若为空则回退使用connection_info中的database。
        table_name: 源表名。

    Returns:
        二元组(columns, actual_table_name)：
            columns: 字段名列表，按ORDINAL_POSITION排序。
            actual_table_name: 数据库中查询到的实际表名（保留数据库中的大小写）。

    Raises:
        ImportError: 未安装pymysql驱动。
        RuntimeError: 查询失败或结果为空。
        ValueError: 连接配置或库名不完整。
    """
    ip = connection_info.get("ip")
    port = str(connection_info.get("port", "3306"))
    username = connection_info.get("username")
    password = connection_info.get("password")
    if not all([ip, port, username, password]):
        raise ValueError("MySQL连接配置缺少必要字段(ip/port/username/password)")

    effective_schema = (schema_name or "").strip()
    if not effective_schema:
        effective_schema = (connection_info.get("database") or "").strip()
    if not effective_schema:
        raise ValueError("MySQL需要schema_name或连接配置中的database作为库名(TABLE_SCHEMA)")

    try:
        db_module = importlib.import_module("pymysql")
    except ImportError:
        raise ImportError("请安装 MySQL 驱动：pip install pymysql")

    # 与Oracle侧UPPER匹配类似，使用LOWER避免大小写/排序规则差异导致查不到列。
    sql = (
        "SELECT COLUMN_NAME, TABLE_NAME "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE LOWER(TABLE_SCHEMA) = LOWER(%s) AND LOWER(TABLE_NAME) = LOWER(%s) "
        "ORDER BY ORDINAL_POSITION"
    )

    connection = None
    cursor = None
    try:
        connection = db_module.connect(
            host=ip,
            port=int(port),
            user=username,
            password=password,
            charset="utf8mb4",
        )
        cursor = connection.cursor()
        cursor.execute(sql, (effective_schema, table_name))
        rows = cursor.fetchall()
        columns = [row[0] for row in rows if row and row[0]]
        actual_table_name = rows[0][1] if rows and len(rows[0]) > 1 else table_name
        if not columns:
            raise RuntimeError(
                "MySQL未查询到字段，请检查库名/表名是否正确: {}.{}".format(
                    effective_schema, table_name
                )
            )
        return columns, actual_table_name
    except Exception:
        LOGGER.exception(
            "[data_source] MySQL查询表结构失败: ip=%s port=%s schema=%s table=%s",
            ip, port, effective_schema, table_name
        )
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_dameng_table_columns(connection_info, schema_name, table_name):
    """查询达梦(DM)表结构，按字段顺序返回字段名列表和实际表名。

    元数据查询方式与Oracle一致，使用ALL_TAB_COLUMNS。

    Args:
        connection_info: 达梦连接配置，包含ip、port、database(可选实例名)、username、password。
        schema_name: 源模式名(OWNER)。
        table_name: 源表名。

    Returns:
        二元组(columns, actual_table_name)：
            columns: 字段名列表，按COLUMN_ID排序。
            actual_table_name: 数据库中查询到的实际表名（保留数据库中的大小写）。

    Raises:
        ImportError: 未安装dmPython驱动。
        RuntimeError: 查询失败或结果为空。
        ValueError: 连接配置不完整。
    """
    ip = connection_info.get("ip")
    port = str(connection_info.get("port", "5236"))
    username = connection_info.get("username")
    password = connection_info.get("password")
    if not all([ip, port, username, password]):
        raise ValueError("达梦连接配置缺少必要字段(ip/port/username/password)")

    try:
        db_module = importlib.import_module("dmPython")
    except ImportError:
        raise ImportError("请安装达梦驱动：pip install dmPython")

    server = "{}:{}".format(ip, int(port))
    sql = (
        "SELECT COLUMN_NAME, TABLE_NAME "
        "FROM ALL_TAB_COLUMNS "
        "WHERE UPPER(OWNER) = UPPER(?) AND UPPER(TABLE_NAME) = UPPER(?) "
        "ORDER BY COLUMN_ID"
    )

    connection = None
    cursor = None
    try:
        connection = db_module.connect(
            user=username,
            password=password,
            server=server,
        )
        cursor = connection.cursor()
        cursor.execute(sql, (schema_name, table_name))
        rows = cursor.fetchall()
        columns = [row[0] for row in rows if row and row[0]]
        actual_table_name = rows[0][1] if rows and len(rows[0]) > 1 else table_name
        if not columns:
            raise RuntimeError(
                "达梦未查询到字段，请检查schema/table是否正确: {}.{}".format(
                    schema_name, table_name
                )
            )
        return columns, actual_table_name
    except Exception:
        LOGGER.exception(
            "[data_source] 达梦查询表结构失败: ip=%s port=%s schema=%s table=%s",
            ip, port, schema_name, table_name
        )
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def query_table_columns_by_connection_type(connection_info, schema_name, table_name):
    """按数据源类型分发查询表字段和实际表名。

    Args:
        connection_info: 连接信息字典，必须包含type字段。
        schema_name: 源schema名称。
        table_name: 源表名。

    Returns:
        二元组(columns, actual_table_name)。

    Raises:
        ValueError: type缺失或不支持。
    """
    source_type = str(connection_info.get("type", "")).strip().lower()
    if not source_type:
        raise ValueError("数据源连接配置缺少type字段")

    query_handlers = {
        "oracle": get_oracle_table_columns,
        "mysql": get_mysql_table_columns,
        "dameng": get_dameng_table_columns,
    }
    handler = query_handlers.get(source_type)
    if handler is None:
        raise ValueError("暂不支持的数据源类型: {}".format(source_type))
    return handler(connection_info, schema_name, table_name)


def get_column_list_from_data_source(from_link_name, schema_name, table_name, config_path=None):
    """从数据源数据库查询字段并拼接为CDM所需格式。

    Args:
        from_link_name: 源连接名称，用于匹配data.json中的连接配置。
        schema_name: 源schema名称。
        table_name: 源表名。
        config_path: 数据源配置文件路径，可选。

    Returns:
        二元组(column_list, actual_table_name)：
            column_list: 字段列表字符串，格式为 "field1&field2&field3"。
            actual_table_name: 数据库中查询到的实际表名。

    Raises:
        Exception: 连接配置读取失败或数据库查询失败。
    """
    connection_info = get_connection_info_by_link_name(
        from_link_name=from_link_name,
        config_path=config_path,
    )
    source_type = str(connection_info.get("type", "")).strip().lower()
    LOGGER.info(
        "[data_source] 使用连接信息查询字段: from_link_name=%s type=%s schema=%s table=%s",
        from_link_name, source_type, schema_name, table_name
    )
    columns, actual_table_name = query_table_columns_by_connection_type(
        connection_info=connection_info,
        schema_name=schema_name,
        table_name=table_name,
    )
    return "&".join(columns), actual_table_name


# ==================== DataArts Studio 开发任务相关函数（来自脚本2）====================

def build_create_dev_job_url(project_id):
    """构建创建开发任务作业接口 URL。"""
    return "{}/v1/{}/jobs".format(DATAARTS_BASE_URL, project_id)


def build_start_dev_job_url(project_id, job_name):
    """构建启动作业接口 URL。"""
    encoded_job_name = urllib.parse.quote(job_name, safe="")
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

    data = json.dumps(job_body, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method="POST")

    LOGGER.info("[CreateDevJob] 正在创建开发任务，URL: %s", url)

    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(response.getcode())
            parsed = None
            try:
                parsed = json.loads(raw) if raw.strip() else None
            except json.JSONDecodeError:
                parsed = None
            LOGGER.info("[CreateDevJob] 创建成功，HTTP 状态码: %d", code)
            return code, raw, parsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed = None
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
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

    request = urllib.request.Request(url, headers=headers, method="POST")

    LOGGER.info("[StartDevJob] 正在启动作业，URL: %s", url)

    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")
            code = int(response.getcode())
            parsed = None
            try:
                parsed = json.loads(raw) if raw.strip() else None
            except json.JSONDecodeError:
                parsed = None
            LOGGER.info("[StartDevJob] 作业启动成功，HTTP 状态码: %d", code)
            return code, raw, parsed
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed = None
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = None
        LOGGER.error(
            "[StartDevJob] HTTP 错误 code=%s url=%s body=%s",
            exc.code, url, raw[:2000],
        )
        return exc.code, raw, parsed


def build_dev_job_body(
    name, directory, node_name, cluster_name, cluster_id, cdm_job_name,
    cron_expression, interval_type="days", owner="liguozhuang",
    job_description="", location_x="705", location_y="636",
    node_location_x="-207.0", node_location_y="-253.0",
    start_time="2026-03-23T00:00:00+08", **kwargs
):
    """构建创建开发任务的完整请求体。"""
    body = {
        "basicConfig": {
            "agency": "",
            "customFields": {},
            "encrypt": False,
            "executeUser": "",
            "instanceTimeout": 0,
            "isIgnoreWaiting": 0,
            "jobDescription": job_description,
            "owner": owner,
            "priority": 0,
            "tags": [],
            "taskPriority": 0
        },
        "cleanOverdueDays": 60,
        "cleanWaitingJob": "cleanup",
        "description": job_description,
        "directory": directory,
        "emptyRunningJob": "0",
        "lastUpdateUser": owner,
        "location": {
            "x": location_x,
            "y": location_y
        },
        "maskedParams": [],
        "name": name,
        "nodes": [
            {
                "execTimeOutRetry": "false",
                "failPolicy": "FAIL_CHILD",
                "lineageInfo": "[{\"outputs\":[],\"inputs\":[]}]",
                "location": {
                    "x": node_location_x,
                    "y": node_location_y
                },
                "maxExecutionTime": 360,
                "name": node_name,
                "pollingInterval": 20,
                "preNodeName": [],
                "properties": [
                    {
                        "name": "jobType",
                        "value": "existsJob",
                        "value_is_sensitive": False
                    },
                    {
                        "name": "clusterName",
                        "value": cluster_name,
                        "value_is_sensitive": False
                    },
                    {
                        "name": "clusterId",
                        "value": cluster_id,
                        "value_is_sensitive": False
                    },
                    {
                        "name": "jobName",
                        "value": cdm_job_name,
                        "value_is_sensitive": False
                    },
                    {
                        "name": "emptyRunningJob",
                        "value": "0",
                        "value_is_sensitive": False
                    },
                    {
                        "name": "taskWorkGroupId",
                        "value": "-1",
                        "value_is_sensitive": False
                    }
                ],
                "resouces": [],
                "retryInterval": 120,
                "retryTimes": 0,
                "type": "CDMJob"
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
                    "sameWorkSpaceJobs": []
                },
                "dependPrePeriod": False,
                "expression": cron_expression.replace("-", " "),
                "expressionTimeZone": "Asia/Shanghai",
                "intervalType": interval_type,
                "isSkipSelfDepJob": "false",
                "monitorObsPath": False,
                "scanDuration": 0,
                "scanInterval": 0,
                "startTime": start_time
            },
            "requireManualConfirmBeforeExecute": False,
            "scheduleOffset": 1,
            "type": "CRON"
        },
        "singleNodeJobFlag": False,
        "taskWorkGroupId": "",
        "useCdmCache": False,
        "version": "1"
    }

    for key, value in kwargs.items():
        if key in body:
            if isinstance(value, dict) and isinstance(body[key], dict):
                body[key].update(value)
            else:
                body[key] = value
        else:
            body[key] = value

    return body


# ==================== 合并后的主流程 ====================

def run_cdm_job_phase(args):
    """执行CDM作业阶段（脚本1的逻辑）。

    Args:
        args: 解析后的参数字典

    Returns:
        tuple: (exit_code, x_auth_token)
            exit_code: 0成功，1失败
            x_auth_token: 认证token（供后续使用）
    """
    LOGGER.info("========== 阶段1: CDM作业流程 ==========")

    project_id = args["project_id"]
    cluster_id = args["cluster_id"]
    group_id = args["group_id"]
    group_name = args["group_name"]
    job_name = args["job_name"]
    from_link_name = args["from_link_name"]
    schema_name = args["schema_name"]
    from_table_name = args["from_table_name"]
    to_database = args["to_database"]
    to_table_name = args["to_table_name"]
    data_source = args["data_source"]

    LOGGER.info("[cdm_job] 作业分组配置: group_id=%s, group_name=%s", group_id, group_name)
    if data_source:
        LOGGER.info("[cdm_job] 数据来源标识: data_source=%s", data_source)

    # 构建URL
    detail_url = build_detail_url(project_id, cluster_id)
    LOGGER.info("[detail_url] 创建作业链接: %s", detail_url)
    start_url = build_start_job_url(project_id, cluster_id, job_name)
    LOGGER.info("[start_url] 执行作业链接: %s", start_url)
    update_url = build_update_job_url(project_id, cluster_id, job_name)
    LOGGER.info("[update_url] 修改作业链接: %s", update_url)
    status_url = build_status_job_url(project_id, cluster_id, job_name)
    LOGGER.info("[status_url] 查询状态链接: %s", status_url)

    # 获取认证Token
    try:
        x_auth_token = get_x_auth_token(project_id)
    except urllib.error.URLError:
        LOGGER.exception("[cdm_job] 获取 token 失败 iam_url=%s", IAM_URL)
        return 1, None

    LOGGER.info("[cdm_job] token 获取成功")

    # 步骤1：创建作业
    LOGGER.info("[cdm_job] === 步骤1: 创建作业 ===")
    job_payload = build_cdm_job_payload(
        project_id=project_id,
        from_table_name=from_table_name,
        to_table_name=to_table_name,
        from_link_name=from_link_name,
        job_name=job_name,
        schema_name=schema_name,
        to_database=to_database,
        is_update=False,
        group_id=group_id,
        group_name=group_name,
        data_source=data_source,
    )

    try:
        code, raw, parsed = create_cdm_job(
            job_payload,
            x_auth_token=x_auth_token,
            detail_url=detail_url,
            project_id=project_id,
        )
    except urllib.error.URLError:
        LOGGER.exception("[cdm_job] 创建作业网络请求失败 url=%s", detail_url)
        return 1, x_auth_token

    if parsed is not None:
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    else:
        print(raw)

    if not (200 <= code < 300):
        LOGGER.error("[cdm_job] 创建作业失败 HTTP %s", code)
        return 1, x_auth_token

    LOGGER.info("[cdm_job] 创建作业成功 HTTP %s", code)

    # 步骤2：执行作业（第一次）
    LOGGER.info("[cdm_job] === 步骤2: 第一次执行作业 ===")
    try:
        code, raw, parsed = start_cdm_job(
            x_auth_token=x_auth_token,
            start_url=start_url,
            project_id=project_id,
        )
    except urllib.error.URLError:
        LOGGER.exception("[cdm_job] 第一次执行作业网络请求失败 url=%s", start_url)
        return 1, x_auth_token

    if parsed is not None:
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    else:
        print(raw)

    if not (200 <= code < 300):
        LOGGER.error("[cdm_job] 第一次执行作业失败 HTTP %s", code)
        return 1, x_auth_token

    LOGGER.info("[cdm_job] 第一次执行作业成功 HTTP %s", code)

    # 步骤2.5：轮询查询作业执行状态
    LOGGER.info("[cdm_job] === 步骤2.5: 查询作业执行状态 ===")
    wait_result = wait_for_job_completion(
        x_auth_token=x_auth_token,
        status_url=status_url,
        project_id=project_id,
        job_name=job_name,
        check_interval=10.0,
        max_wait_time=3600.0,
    )

    if wait_result != 0:
        if wait_result == 2:
            LOGGER.error("[cdm_job] 作业执行状态异常，终止流程")
        elif wait_result == 3:
            LOGGER.error("[cdm_job] 作业执行超时，终止流程")
        else:
            LOGGER.error("[cdm_job] 查询作业状态失败，终止流程")
        return 1, x_auth_token

    LOGGER.info("[cdm_job] 作业执行完成，继续下一步")

    # 步骤3：从Hive获取建表语句
    LOGGER.info("[cdm_job] === 步骤3: 从Hive获取建表语句 ===")
    try:
        original_ddl = get_hive_ddl(to_table_name, to_database)
        LOGGER.info("[cdm_job] 原始建表语句:\n%s", original_ddl[:500] + "..." if len(original_ddl) > 500 else original_ddl)
    except ImportError as exc:
        LOGGER.error("[cdm_job] %s", exc)
        return 1, x_auth_token
    except Exception as exc:
        LOGGER.error("[cdm_job] 获取建表语句失败: %s", exc)
        return 1, x_auth_token

    # 步骤4：修改建表语句
    LOGGER.info("[cdm_job] === 步骤4: 修改建表语句 ===")
    try:
        modified_ddl = modify_hive_ddl(original_ddl)
        LOGGER.info("[cdm_job] 修改后的建表语句:\n%s", modified_ddl)
    except Exception as exc:
        LOGGER.error("[cdm_job] 修改建表语句失败: %s", exc)
        return 1, x_auth_token

    # 步骤4.5：删除当前表并执行修改后的建表语句
    LOGGER.info("[cdm_job] === 步骤4.5: 删除表并重建 ===")
    try:
        execute_hive_ddl(to_database, to_table_name, modified_ddl)
        LOGGER.info("[cdm_job] 表 %s 删除并重建成功", to_table_name)
    except ImportError as exc:
        LOGGER.error("[cdm_job] %s", exc)
        return 1, x_auth_token
    except Exception as exc:
        LOGGER.error("[cdm_job] 删除并重建表失败: %s", exc)
        return 1, x_auth_token

    # 步骤5：调用修改作业接口
    LOGGER.info("[cdm_job] === 步骤5: 调用修改作业接口 ===")
    try:
        column_list, actual_from_table_name = get_column_list_from_data_source(
            from_link_name=from_link_name,
            schema_name=schema_name,
            table_name=from_table_name,
        )
        LOGGER.info(
            "[cdm_job] 查询得到的字段列表: %s",
            column_list[:100] + "..." if len(column_list) > 100 else column_list,
        )
        LOGGER.info(
            "[cdm_job] 数据库实际表名: input_table=%s actual_table=%s",
            from_table_name,
            actual_from_table_name,
        )

        updated_payload = build_cdm_job_payload(
            project_id=project_id,
            from_table_name=actual_from_table_name,
            to_table_name=to_table_name,
            from_link_name=from_link_name,
            job_name=job_name,
            schema_name=schema_name,
            to_database=to_database,
            column_list=column_list,
            is_update=True,
            group_id=group_id,
            group_name=group_name,
            data_source=data_source,
        )
        code, raw, parsed = update_cdm_job(
            updated_payload,
            x_auth_token=x_auth_token,
            update_url=update_url,
            project_id=project_id,
        )
    except urllib.error.URLError:
        LOGGER.exception("[cdm_job] 修改作业网络请求失败 url=%s", update_url)
        return 1, x_auth_token
    except Exception as exc:
        LOGGER.error("[cdm_job] 修改作业失败: %s", exc)
        return 1, x_auth_token

    if parsed is not None:
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    else:
        print(raw)

    if not (200 <= code < 300):
        LOGGER.error("[cdm_job] 修改作业失败 HTTP %s", code)
        return 1, x_auth_token

    LOGGER.info("[cdm_job] 修改作业成功 HTTP %s", code)

    # 步骤6：再次执行作业
    LOGGER.info("[cdm_job] === 步骤6: 再次执行作业 ===")
    try:
        code, raw, parsed = start_cdm_job(
            x_auth_token=x_auth_token,
            start_url=start_url,
            project_id=project_id,
        )
    except urllib.error.URLError:
        LOGGER.exception("[cdm_job] 第二次执行作业网络请求失败 url=%s", start_url)
        return 1, x_auth_token

    if parsed is not None:
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    else:
        print(raw)

    if not (200 <= code < 300):
        LOGGER.error("[cdm_job] 第二次执行作业失败 HTTP %s", code)
        return 1, x_auth_token

    LOGGER.info("[cdm_job] 第二次执行作业成功 HTTP %s", code)

    # 步骤6.5：轮询查询作业执行状态
    LOGGER.info("[cdm_job] === 步骤6.5: 查询作业执行状态 ===")
    wait_result = wait_for_job_completion(
        x_auth_token=x_auth_token,
        status_url=status_url,
        project_id=project_id,
        job_name=job_name,
        check_interval=10.0,
        max_wait_time=3600.0,
    )

    if wait_result != 0:
        if wait_result == 2:
            LOGGER.error("[cdm_job] 作业执行状态异常，终止流程")
        elif wait_result == 3:
            LOGGER.error("[cdm_job] 作业执行超时，终止流程")
        else:
            LOGGER.error("[cdm_job] 查询作业状态失败，终止流程")
        return 1, x_auth_token

    LOGGER.info("[cdm_job] 作业执行完成")
    LOGGER.info("[cdm_job] === CDM作业流程执行成功 ===")

    return 0, x_auth_token


def run_dev_job_phase(args, x_auth_token):
    """执行开发任务阶段（脚本2的逻辑）。

    Args:
        args: 解析后的参数字典
        x_auth_token: 认证token（复用CDM阶段的token）

    Returns:
        int: 0成功，1失败
    """
    LOGGER.info("========== 阶段2: 开发任务流程 ==========")
    LOGGER.info("=开发任务阶段参数= %s",args)
    project_id = args["project_id"]
    workspace_id = args["workspace_id"]
    dev_name = args["dev_name"]
    directory = args["directory"]
    node_name = args["node_name"]
    cluster_name = args["cluster_name"]
    cluster_id = args["cluster_id"]
    cdm_job_name = args["cdm_job_name"]
    cron_expression = args["cron_expression"]
    interval_type = args["interval_type"]
    owner = args["owner"]
    description = args["description"]
    location_x = args["location_x"]
    location_y = args["location_y"]

    LOGGER.info("[dev_job] 接收参数 - project_id: %s, dev_name: %s, directory: %s",
                project_id, dev_name, directory)

    # 复用CDM阶段的token，无需再次获取
    LOGGER.info("[dev_job] 复用CDM阶段获取的Token")

    # 步骤 1：构建完整作业请求体
    job_body = build_dev_job_body(
        name=dev_name,
        directory=directory,
        node_name=node_name,
        cluster_name=cluster_name,
        cluster_id=cluster_id,
        cdm_job_name=cdm_job_name,
        cron_expression=cron_expression,
        interval_type=interval_type,
        owner=owner,
        job_description=description if description else None,
        location_x=location_x,
        location_y=location_y,
    )

    LOGGER.info("[dev_job] 作业请求体构建完成，目录: %s, 名称: %s", directory, dev_name)

    # 步骤 2：调用创建作业接口
    try:
        code, raw, parsed = create_dev_job(
            project_id=project_id,
            workspace_id=workspace_id,
            x_auth_token=x_auth_token,
            job_body=job_body,
        )

        print("\n========== 创建开发任务结果 ==========")
        print("HTTP 状态码: {}".format(code))
        if parsed:
            print("响应内容:\n{}".format(json.dumps(parsed, indent=2, ensure_ascii=False)))
        else:
            print("原始响应:\n{}".format(raw))

        if not (200 <= code < 300):
            LOGGER.error("[dev_job] 作业创建失败，HTTP 状态码: %d", code)
            return 1

        LOGGER.info("[dev_job] 作业创建成功")

    except Exception as e:
        LOGGER.error("[dev_job] 调用创建作业接口失败: %s", e)
        return 1

    # 步骤 3：创建成功后启动作业
    try:
        code, raw, parsed = start_dev_job(
            project_id=project_id,
            workspace_id=workspace_id,
            job_name=dev_name,
            x_auth_token=x_auth_token,
        )

        print("\n========== 启动开发任务结果 ==========")
        print("HTTP 状态码: {}".format(code))
        if parsed:
            print("响应内容:\n{}".format(json.dumps(parsed, indent=2, ensure_ascii=False)))
        else:
            print("原始响应:\n{}".format(raw))

        if 200 <= code < 300:
            LOGGER.info("[dev_job] 作业启动成功")
            return 0
        else:
            LOGGER.error("[dev_job] 作业启动失败，HTTP 状态码: %d", code)
            return 1

    except Exception as e:
        LOGGER.error("[dev_job] 调用启动作业接口失败: %s", e)
        return 1


def parse_args():
    """解析命令行参数。

    Returns:
        dict: 参数字典，解析失败返回None
    """
    # 支持18个必需参数 + 5个可选参数 = 最多23个参数
    # 最少需要18个参数（脚本1的8个必需 + 脚本2的9个必需中除去重复的2个 = 15个？）
    # 重新计算：
    # 脚本1必需：project_id, cluster_id, job_name, from_link_name, schema_name, from_table_name, to_database, to_table_name = 8个
    # 脚本2必需：project_id, name, directory, node_name, cluster_name, cluster_id, cdm_job_name, cron_expression = 8个
    # 合并后必需（去重）：project_id, cluster_id, job_name, from_link_name, schema_name, from_table_name, to_database, to_table_name,
    #                    dev_name, directory, node_name, cluster_name, cdm_job_name, cron_expression = 14个
    # 可选参数：group_id, group_name, data_source, workspace_id, interval_type, owner, description, location_x, location_y = 9个

    min_args = 14
    max_args = 23
    actual_args = len(sys.argv) - 1

    LOGGER.info("所有的参数: %s", sys.argv)
    if actual_args < min_args or actual_args > max_args:
        LOGGER.error(
            "参数数量错误，期望 %d-%d 个参数，实际 %d 个。",
            min_args, max_args, actual_args,
        )
        print_usage()
        return None

    args = {}

    # === CDM作业参数（脚本1）===
    # 1-2: 必填
    args["project_id"] = sys.argv[1]
    args["cluster_id"] = sys.argv[2]

    # 3-4: 可选（group_id, group_name）
    args["group_id"] = sys.argv[3] if actual_args >= 3 else "1"
    args["group_name"] = sys.argv[4] if actual_args >= 4 else "DEFAULT"

    # 5-11: 必填和可选（job_name到data_source）
    # 注意：需要动态计算偏移量，因为group_id/group_name是可选的
    # 新的理解：参数是固定的顺序，可选的有默认值

    # 重新设计参数解析逻辑
    # 位置固定的参数（基于脚本1的11个参数 + 脚本2去重后的12个参数 = 23个）
    # 脚本1: project_id[1], cluster_id[2], group_id[3], group_name[4], job_name[5], from_link_name[6],
    #        schema_name[7], from_table_name[8], to_database[9], to_table_name[10], data_source[11]
    # 脚本2: workspace_id[12], dev_name[13], directory[14], node_name[15], cluster_name[16],
    #        cdm_job_name[17], cron_expression[18], interval_type[19], owner[20], description[21], location_x[22], location_y[23]

    # 为了向后兼容，使用索引访问
    idx = 1

    # 脚本1参数
    args["project_id"] = sys.argv[idx]; idx += 1
    args["cluster_id"] = sys.argv[idx]; idx += 1
    args["group_id"] = sys.argv[idx] if idx <= actual_args else "1"; idx += 1 if idx <= actual_args else 0
    args["group_name"] = sys.argv[idx] if idx <= actual_args else "DEFAULT"; idx += 1 if idx <= actual_args else 0
    args["job_name"] = sys.argv[idx] if idx <= actual_args else ""; idx += 1
    args["from_link_name"] = sys.argv[idx] if idx <= actual_args else ""; idx += 1
    args["schema_name"] = sys.argv[idx] if idx <= actual_args else ""; idx += 1
    args["from_table_name"] = sys.argv[idx] if idx <= actual_args else ""; idx += 1
    args["to_database"] = sys.argv[idx] if idx <= actual_args else ""; idx += 1
    args["to_table_name"] = sys.argv[idx] if idx <= actual_args else ""; idx += 1
    args["data_source"] = sys.argv[idx] if idx <= actual_args else None

    # 检查脚本1的必填参数
    cdm_required = ["project_id", "cluster_id", "job_name", "from_link_name",
                    "schema_name", "from_table_name", "to_database", "to_table_name"]
    for param in cdm_required:
        if not args.get(param):
            LOGGER.error("缺少CDM作业必填参数: %s", param)
            return None

    # 脚本2参数（继续从当前idx开始）
    # 脚本2参数顺序：workspace_id, dev_name, directory, node_name, cluster_name,
    #               cdm_job_name, cron_expression, interval_type, owner, description, location_x, location_y

    # 重新定位到正确的索引（脚本2参数从第12个位置开始）
    # 更清晰的实现：直接按位置取值

    # 脚本2参数（基于23个参数的完整设计）
    # 12. workspace_id (可选)
    args["workspace_id"] = sys.argv[12].replace("-", "") if actual_args >= 12 else ""
    # 13. dev_name (必填)
    args["dev_name"] = sys.argv[13] if actual_args >= 13 else ""
    # 14. directory (必填)
    args["directory"] = sys.argv[14] if actual_args >= 14 else ""
    # 15. node_name (必填)
    args["node_name"] = sys.argv[15] if actual_args >= 15 else ""
    # 16. cluster_name (必填)
    args["cluster_name"] = sys.argv[16] if actual_args >= 16 else ""
    # 17. cdm_job_name (必填)
    # args["cdm_job_name"] = sys.argv[17] if actual_args >= 17 else ""
    args["cdm_job_name"] = args["job_name"]
    # 18. cron_expression (必填)
    args["cron_expression"] = sys.argv[17] if actual_args >= 17 else ""
    # 19. interval_type (可选)
    args["interval_type"] = sys.argv[18] if actual_args >= 18 else "days"
    # 20. owner (可选)
    args["owner"] = sys.argv[19] if actual_args >= 19 else "liguozhuang"
    # 21. description (可选)
    args["description"] = sys.argv[20] if actual_args >= 20 else ""
    # 22. location_x (可选)
    args["location_x"] = sys.argv[21] if actual_args >= 21 else "705"
    # 23. location_y (可选)
    args["location_y"] = sys.argv[22] if actual_args >= 22 else "636"

    # 检查脚本2的必填参数
    dev_required = ["dev_name", "directory", "node_name", "cluster_name", "cdm_job_name", "cron_expression"]
    for param in dev_required:
        if not args.get(param):
            LOGGER.error("缺少开发任务必填参数: %s", param)
            return None

    return args


def print_usage():
    """打印使用说明。"""
    print("""
用法: python 3CDM作业+开发作业.py <参数列表>

参数顺序（共23个位置参数，其中14个必填，9个可选）：

=== CDM作业参数（脚本1）===
  1. project_id      - 项目 ID（必填）
  2. cluster_id      - CDM集群 ID（必填）
  3. group_id        - 作业分组ID（可选，默认: "1"）
  4. group_name      - 作业分组名称（可选，默认: "DEFAULT"）
  5. job_name        - CDM作业名称（必填）
  6. from_link_name  - 源连接名称（必填）
  7. schema_name     - 源数据库schema名称（必填）
  8. from_table_name - 源数据库表名（必填）
  9. to_database     - 目标Hive数据库名称（必填）
  10. to_table_name   - 目标Hive表名（必填）
  11. data_source    - 数据来源标识（可选，默认: "<示例委办局>-i<示例业务场景>-i本市智慧养老系统"）

=== 开发任务参数（脚本2）===
  12. workspace_id   - 工作空间 ID（可选，传空字符串""表示不使用）
  13. dev_name       - 开发任务名称（必填）
  14. directory     - 作业目录路径（必填）
  15. node_name     - 节点名称（必填）
  16. cluster_name  - CDM集群名称（必填）
  17. cdm_job_name  - CDM作业名称（节点属性中的jobName，必填，通常与job_name相同）
  18. cron_expression - 调度Cron表达式（必填，如 "0 20 1 * * ?"）
  19. interval_type   - 调度间隔类型（可选，默认: days）
  20. owner          - 作业所有者（可选，默认: liguozhuang）
  21. description    - 作业描述（可选，默认: 空）
  22. location_x     - 画布X坐标（可选，默认: 705）
  23. location_y     - 画布Y坐标（可选，默认: 636）

示例:
  python 3CDM作业+开发作业.py \\
    "2d06fe3f78324f7d9b45abdc4db9e2a8" \\
    "3bdb7eec-5a76-46f9-a0ee-61cbecf20962" \\
    "1" "DEFAULT" \\
    "ONM_SZLG_MSSQ_CASE_APPEAL_0108" \\
    "DM" "LGYWTG" \\
    "ONM_SZLG_MSSQ_CASE_APPEAL" \\
    "ods_lgbs" "ONM_SZLG_MSSQ_CASE_APPEAL_0108" \\
    "<示例委办局>-i<示例业务场景>-i本市智慧养老系统" \\
    "" \\
    "dev_job_ods_test" "/测试目录" "test_node" \\
    "cdm-7551" "ONM_SZLG_MSSQ_CASE_APPEAL_0108" \\
    "0 20 1 * * ?"
""", file=sys.stderr)


def main():
    """程序主入口。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 解析参数
    args = parse_args()
    if args is None:
        return 2

    LOGGER.info("[main] 参数解析成功，开始执行合并流程")
    LOGGER.info("[main] CDM作业名称: %s", args["job_name"])
    LOGGER.info("[main] 开发任务名称: %s", args["dev_name"])

    # ========== 阶段1: CDM作业流程 ==========
    exit_code, x_auth_token = run_cdm_job_phase(args)
    if exit_code != 0:
        LOGGER.error("[main] CDM作业阶段执行失败，终止流程")
        return exit_code

    if x_auth_token is None:
        LOGGER.error("[main] 未能获取有效的认证Token")
        return 1

    # ========== 阶段2: 开发任务流程 ==========
    # 复用CDM阶段获取的token
    exit_code = run_dev_job_phase(args, x_auth_token)
    if exit_code != 0:
        LOGGER.error("[main] 开发任务阶段执行失败")
        return exit_code

    LOGGER.info("[main] ========== 完整流程执行成功 ==========")
    LOGGER.info("[main] CDM作业和开发任务均已完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
