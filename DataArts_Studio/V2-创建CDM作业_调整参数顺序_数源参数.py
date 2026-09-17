# -*- coding: utf-8 -*-
"""华为云 CDM：通过 REST 接口创建迁移作业并执行完整流程。

通过命令行参数指定作业配置（位置参数）：
    python script.py <project_id> <cluster_id> <from_table_name> <to_table_name> <from_link_name> <job_name> <schema_name> <to_database>

示例：
    python script.py 2d06fe3f78324f7d9b45abdc4db9e2a8 3bdb7eec-5a76-46f9-a0ee-61cbecf20962 ONM_SZLG_MSSQ_CASE_APPEAL ONM_SZLG_MSSQ_CASE_APPEAL_0108 DM 同步测试 LGYWTG ods_lgbs

完整流程：
    1. 创建作业
    2. 执行作业（第一次）
    3. 从Hive获取建表语句
    4. 修改建表语句（添加默认字段和分区）
    5. 调用修改作业接口（更新字段列表）
    6. 再次执行作业
"""

from __future__ import print_function

import json
import logging
import os
import re
import ssl
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

LOGGER = logging.getLogger(__name__)
ssl._create_default_https_context = ssl._create_unverified_context

IAM_URL = "https://<INTERNAL_IAM_HOST>/v3/auth/tokens"
IAM_USERNAME = "admin_user"
IAM_PASSWORD = "<YOUR_IAM_PASSWORD>"
IAM_DOMAIN_NAME = "政务大数据治理平台"

CDM_BASE_URL = "https://cdm.example.gov.cn"

# Hive配置
# 参考 HIVE_连接示例.py 的连接方式
# 关键参数：
#   1. host: 使用IP地址进行网络连接
#   2. krbhost: Kerberos认证使用的主机名（必须与Hive的principal一致）
#   3. database: 动态设置，通过to_database参数传入
# 注意：
#   - 请确保服务器/etc/hosts中包含: <INTERNAL_HIVE_HOST> <INTERNAL_HIVE_HOSTNAME>
#   - krbhost必须与Hive JDBC URL中的principal一致（hive/<INTERNAL_HIVE_HOSTNAME>）
HIVE_CONFIG = {
    "host": "<INTERNAL_HIVE_HOST>",  # Hive服务器IP地址（网络连接使用）
    "port": 21066,
    "username": "admin_user",
    "database": None,  # 动态设置，通过to_database参数传入
    "auth": "KERBEROS",
    "kerberos_service_name": "hive",
    #"krbhost": "<INTERNAL_HIVE_HOST>",  # 与Hive principal中的主机名一致
    "krbhost": "<INTERNAL_HIVE_HOSTNAME>",  # 与Hive principal中的主机名一致
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


def build_detail_url(project_id, cluster_id):
    """构建 CDM 作业接口 URL。

    Args:
        project_id: 华为云项目 ID。
        cluster_id: CDM 集群 ID。

    Returns:
        完整的 CDM 作业 API URL。
    """
    return "{}/v1.1/{}/clusters/{}/cdm/job".format(CDM_BASE_URL, project_id, cluster_id)


def build_start_job_url(project_id, cluster_id, job_name):
    """构建 CDM 执行作业接口 URL。

    Args:
        project_id: 华为云项目 ID。
        cluster_id: CDM 集群 ID。
        job_name: 作业名称。

    Returns:
        完整的 CDM 执行作业 API URL。
    """
    # 对job_name进行URL编码，处理中文字符
    encoded_job_name = urllib.parse.quote(job_name, safe='')
    return "{}/v1.1/{}/clusters/{}/cdm/job/{}/start".format(
        CDM_BASE_URL, project_id, cluster_id, encoded_job_name
    )


def build_update_job_url(project_id, cluster_id, job_name):
    """构建 CDM 修改作业接口 URL。

    Args:
        project_id: 华为云项目 ID。
        cluster_id: CDM 集群 ID。
        job_name: 作业名称。

    Returns:
        完整的 CDM 修改作业 API URL。
    """
    # 对job_name进行URL编码，处理中文字符
    encoded_job_name = urllib.parse.quote(job_name, safe='')
    return "{}/v1.1/{}/clusters/{}/cdm/job/{}".format(
        CDM_BASE_URL, project_id, cluster_id, encoded_job_name
    )


def build_status_job_url(project_id, cluster_id, job_name):
    """构建 CDM 查询作业状态接口 URL。

    Args:
        project_id: 华为云项目 ID。
        cluster_id: CDM 集群 ID。
        job_name: 作业名称。

    Returns:
        完整的 CDM 查询作业状态 API URL。
    """
    # 对job_name进行URL编码，处理中文字符
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
    """构建 CDM 作业请求体。

    Args:
        project_id: 华为云项目 ID。
        cluster_id: CDM 集群 ID。
        group_id: 作业分组ID，默认为"1"。
        group_name: 作业分组名称，默认为"DEFAULT"。
        job_name: 作业名称。
        from_link_name: 源连接名称。
        schema_name: 源数据库 schema 名称。
        from_table_name: 源数据库表名。
        to_database: 目标 Hive 数据库名称。
        to_table_name: 目标 Hive 表名。
        column_list: 字段列表字符串，格式为 "field1&field2&field3"。
                      如果为None且is_update=False，不包含columnList字段。
                      如果为None且is_update=True，使用默认字段列表。
        is_update: 是否为修改作业（True）还是创建作业（False）。
                   创建作业时不包含columnList和sampleValueColumn字段。
                   修改作业时包含columnList和sampleValueColumn字段。
        data_source: 数据来源标识字符串，用于填充 lgdsj_data_source 字段。
                     默认为 None，使用 "<示例委办局>-i<示例业务场景>-i本市智慧养老系统" 作为默认值。

    Returns:
        CDM 作业请求体字典。
    """
    # 设置数据来源默认值
    if data_source is None:
        data_source = "<示例委办局>-i<示例业务场景>-i本市智慧养老系统"
    # 新增字段列表
    default_extra_fields = ["lgdsj_timeflag", "lgdsj_data_source", "lgdsj_load_time", "<INTERNAL_DATASET>"]

    # toJobConfig.extendedFields 的不同值
    # 创建作业时的值（基础配置）
    to_extended_fields_create = "<INTERNAL_B64>"
    # 修改作业时的值（包含分区配置）
    to_extended_fields_update = "<INTERNAL_B64>"

    # 根据is_update选择正确的extendedFields值
    to_extended_fields_value = to_extended_fields_update if is_update else to_extended_fields_create
    LOGGER.info("[cdm_job] to_extended_fields_value: %s", to_extended_fields_value)
    # 构建fromJobConfig的inputs列表
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

    # 构建toJobConfig的inputs列表
    # 创建作业时：基础配置（不包含clearDataMode、shouldClearTable、extendedFields）
    to_job_config_inputs = [
        {"name": "toJobConfig.hive", "value": "hive"},
        {"name": "toJobConfig.database", "value": to_database},
        {"name": "toJobConfig.table", "value": to_table_name},
        {"name": "toJobConfig.tablePreparation", "value": "CREATE_WHEN_NOT_EXIST"},
        {"name": "toJobConfig.convertNull", "value": "TO_NULL"},
        {"name": "toJobConfig.csvDelimPolicy", "value": "DROP"},
    ]

    # extended-configs 默认不设置（创建作业时不需要）
    to_extended_config = None

    if is_update:
        # 修改作业时：添加基础配置中不包含的三个参数
        to_job_config_inputs.append({"name": "toJobConfig.shouldClearTable", "value": "true"})
        to_job_config_inputs.append({"name": "toJobConfig.clearDataMode", "value": "TRUNCATE"})

        # 设置 extended-configs（修改作业时需要）
        to_extended_config = {
            "name": "toJobConfig.extendedFields",
            "value": to_extended_fields_value,
        }

        # 修改作业时：需要添加columnList和sampleValueColumn字段
        if column_list is None:
            # 修改作业时必须提供column_list，不能为None
            LOGGER.error("[cdm_job] 调用修改任务接口时column_list不能为空，请从Hive建表语句中提取字段列表")
            raise ValueError("修改作业时必须提供column_list参数，不能为None")
        else:
            # 检查并添加缺失的新增字段
            existing_fields = set(column_list.split("&"))
            missing_fields = [f for f in default_extra_fields if f not in existing_fields]
            if missing_fields:
                column_list = column_list + "&" + "&".join(missing_fields)
                LOGGER.info("[cdm_job] 字段列表中自动添加缺失字段: %s", "&".join(missing_fields))

        # 在fromJobConfig中添加columnList和sampleValueColumn字段
        from_job_config_inputs.append({"name": "fromJobConfig.columnList", "value": column_list})
        from_job_config_inputs.append({
            "name": "fromJobConfig.sampleValueColumn",
            "value": "lgdsj_timeflag:${dateformat(yyyy-MM-dd HH:mm:ss)}&lgdsj_data_source:"+data_source+"&lgdsj_load_time:${dateformat(yyyy-MM-dd HH:mm:ss)}&<INTERNAL_DATASET>:'${dateformat(yyyyMMdd,-1,DAY)}'"
        })

        # 在toJobConfig中添加columnList字段（值与fromJobConfig.columnList保持一致）
        to_job_config_inputs.append({"name": "toJobConfig.columnList", "value": column_list})
    else:
        to_job_config_inputs.append({"name": "toJobConfig.shouldClearTable", "value": "false"})

    # 构建to-config-values（根据is_update决定是否包含extended-configs）
    to_config_values = {
        "configs": [
            {
                "inputs": to_job_config_inputs,
                "name": "toJobConfig",
            }
        ],
    }
    # 修改作业时才添加extended-configs
    if to_extended_config:
        to_config_values["extended-configs"] = to_extended_config

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
                                {
                                    "name": "throttlingConfig.concurrentSubJobs",
                                    "value": "10",
                                },
                                {"name": "throttlingConfig.numExtractors", "value": "1"},
                                {"name": "throttlingConfig.numSplits", "value": "1"},
                                {
                                    "name": "throttlingConfig.splitRetryTime",
                                    "value": "0",
                                },
                                {
                                    "name": "throttlingConfig.submitToCluster",
                                    "value": "false",
                                },
                                {"name": "throttlingConfig.numLoaders", "value": "1"},
                                {
                                    "name": "throttlingConfig.recordDirtyData",
                                    "value": "false",
                                },
                                {
                                    "name": "throttlingConfig.maxErrorRecords",
                                    "value": "10",
                                },
                                {"name": "throttlingConfig.throttling", "value": "false"},
                                {"name": "throttlingConfig.byteRate", "value": "10"},
                                {
                                    "name": "throttlingConfig.channelCapacityMb",
                                    "value": "64",
                                },
                                {
                                    "name": "throttlingConfig.recordRate",
                                    "value": "100000",
                                },
                            ],
                            "name": "throttlingConfig",
                        },
                        {"inputs": [], "name": "jarConfig"},
                        {
                            "inputs": [
                                {
                                    "name": "schedulerConfig.isSchedulerJob",
                                    "value": "false",
                                },
                                {
                                    "name": "schedulerConfig.disposableType",
                                    "value": "NONE",
                                },
                            ],
                            "name": "schedulerConfig",
                        },
                        {
                            "inputs": [],
                            "name": "transformConfig",
                        },
                        {
                            "inputs": [
                                {
                                    "name": "smnConfig.isNeedNotification",
                                    "value": "false",
                                }
                            ],
                            "name": "smnConfig",
                        },
                        {
                            "inputs": [
                                {
                                    "name": "retryJobConfig.retryJobType",
                                    "value": "NONE",
                                }
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


def get_x_auth_token(
    project_id,
    timeout_sec=30.0,
):
    """通过 IAM 鉴权接口获取 X-Auth-Token。

    Args:
        project_id: IAM 鉴权作用域项目 ID。
        timeout_sec: 请求超时秒数。

    Returns:
        请求成功返回响应头中的 ``X-Subject-Token``。

    Raises:
        ValueError: 入参缺失或响应中未返回 token。
        urllib.error.URLError: 网络请求失败。
    """
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


def create_cdm_job(
    body,
    x_auth_token,
    detail_url,
    project_id,
    timeout_sec=120.0,
):
    """POST 创建 CDM 作业。

    Args:
        body: 请求体，与控制台/文档中的 jobs 结构一致。
        x_auth_token: 华为云 Token（``X-Auth-Token``）。
        detail_url: CDM 作业接口 URL。
        project_id: 华为云项目 ID（用于设置 workspace 请求头）。
        timeout_sec: 请求超时秒数。

    Returns:
        (HTTP 状态码, 原始响应正文, 若为 JSON 则解析后的对象，否则为 None)。

    Raises:
        urllib.error.URLError: 网络或超时错误。
    """
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
            exc.code,
            detail_url,
            raw[:2000],
        )
        return int(exc.code), raw, parsed


def start_cdm_job(
    x_auth_token,
    start_url,
    project_id,
    timeout_sec=120.0,
):
    """POST 执行 CDM 作业。

    Args:
        x_auth_token: 华为云 Token（``X-Auth-Token``）。
        start_url: CDM 执行作业接口 URL。
        project_id: 华为云项目 ID（用于设置 workspace 请求头）。
        timeout_sec: 请求超时秒数。

    Returns:
        (HTTP 状态码, 原始响应正文, 若为 JSON 则解析后的对象，否则为 None)。

    Raises:
        urllib.error.URLError: 网络或超时错误。
    """
    detail_headers = {
        "X-Auth-Token": x_auth_token.strip(),
        "workspace": project_id,
        "Content-Type": "application/json;charset=UTF-8",
        "X-Language": "zh-cn",
    }

    # 执行作业接口不需要请求体
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
            exc.code,
            start_url,
            raw[:2000],
        )
        return int(exc.code), raw, parsed


def update_cdm_job(
    body,
    x_auth_token,
    update_url,
    project_id,
    timeout_sec=120.0,
):
    """PUT 修改 CDM 作业。

    Args:
        body: 请求体，与控制台/文档中的 jobs 结构一致。
        x_auth_token: 华为云 Token（``X-Auth-Token``）。
        update_url: CDM 修改作业接口 URL。
        project_id: 华为云项目 ID（用于设置 workspace 请求头）。
        timeout_sec: 请求超时秒数。

    Returns:
        (HTTP 状态码, 原始响应正文, 若为 JSON 则解析后的对象，否则为 None)。

    Raises:
        urllib.error.URLError: 网络或超时错误。
    """
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
            exc.code,
            update_url,
            raw[:2000],
        )
        return int(exc.code), raw, parsed


def get_cdm_job_status(
    x_auth_token,
    status_url,
    project_id,
    timeout_sec=30.0,
):
    """GET 查询 CDM 作业执行状态。

    Args:
        x_auth_token: 华为云 Token（``X-Auth-Token``）。
        status_url: CDM 查询作业状态接口 URL。
        project_id: 华为云项目 ID（用于设置 workspace 请求头）。
        timeout_sec: 请求超时秒数。

    Returns:
        (HTTP 状态码, 原始响应正文, 若为 JSON 则解析后的对象，否则为 None)。

    Raises:
        urllib.error.URLError: 网络或超时错误。
    """
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
            exc.code,
            status_url,
            raw[:2000],
        )
        return int(exc.code), raw, parsed


def wait_for_job_completion(
    x_auth_token,
    status_url,
    project_id,
    job_name,
    check_interval=10.0,
    max_wait_time=3600.0,
):
    """循环查询CDM作业执行状态，直到任务完成或失败。

    Args:
        x_auth_token: 华为云 Token（``X-Auth-Token``）。
        status_url: CDM 查询作业状态接口 URL。
        project_id: 华为云项目 ID。
        job_name: 作业名称（用于日志输出）。
        check_interval: 状态查询间隔秒数（默认10秒）。
        max_wait_time: 最大等待时间秒数（默认1小时）。

    Returns:
        0: 任务执行成功（SUCCEEDED）
        1: 任务执行失败或HTTP错误
        2: 任务状态异常（非RUNNING也非SUCCEEDED）
        3: 超过最大等待时间

    Raises:
        RuntimeError: 任务执行失败或状态异常。
    """
    import time

    start_time = time.time()
    check_count = 0

    LOGGER.info("[cdm_job] 开始轮询作业执行状态: %s", job_name)

    while True:
        check_count += 1
        elapsed_time = time.time() - start_time

        # 检查是否超过最大等待时间
        if elapsed_time > max_wait_time:
            LOGGER.error(
                "[cdm_job] 作业执行超过最大等待时间 %.0f 秒，停止轮询",
                max_wait_time
            )
            return 3

        # 查询任务状态
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

        # 解析状态
        status = None
        if parsed and isinstance(parsed, dict) and "submissions" in parsed:
            submissions = parsed.get("submissions", [])
            if submissions and len(submissions) > 0:
                # 获取最近一次提交的状态
                latest_submission = submissions[0]
                status = latest_submission.get("status")
                progress = latest_submission.get("progress", 0)
                LOGGER.info(
                    "[cdm_job] 作业状态查询 #%d: status=%s, progress=%s%%",
                    check_count, status, progress * 100
                )

        if status is None:
            LOGGER.warning("[cdm_job] 无法从响应中解析作业状态，原始响应: %s", raw[:500])
            # 继续轮询，不立即退出
        elif status in ("RUNNING", "BOOTING","PENDING"):
            # RUNNING: 正在执行中
            # BOOTING: 启动中（任务刚提交，正在初始化）
            # PENDING: 等待中
            LOGGER.info("[cdm_job] 作业正在%s中，%.0f秒后再次查询...",
                       "启动" if status == "BOOTING" else "执行", check_interval)
            time.sleep(check_interval)
            continue
        elif status == "SUCCEEDED":
            LOGGER.info("[cdm_job] 作业执行成功，总耗时 %.0f 秒", elapsed_time)
            return 0
        else:
            # 状态不为RUNNING、BOOTING、PENDING也不为SUCCEEDED，视为失败或异常
            LOGGER.error(
                "[cdm_job] 作业执行异常，状态=%s，终止流程。完整响应: %s",
                status,
                json.dumps(parsed, ensure_ascii=False, indent=2) if parsed else raw
            )
            return 2


def init_env_and_auth():
    """初始化环境变量+Kerberos认证。

    参考 Hive_连接示例2.py 的 init_env_and_auth 函数实现。
    1. 加载Hadoop环境变量
    2. 执行Kerberos认证（kinit）

    Raises:
        RuntimeError: 环境变量加载失败或Kerberos认证失败。
    """
    LOGGER.info("[hive] 开始执行：环境变量加载 + Kerberos认证")

    # 步骤1：加载Hadoop环境变量
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

    # 步骤2：Kerberos认证
    username = HIVE_CONFIG.get("username", "admin_user")
    LOGGER.info("[hive] 执行Kerberos认证（用户：%s）", username)

    # 使用密码认证（参考Hive_连接示例2.py的实现）
    # 密码通过环境变量传入，避免硬编码
    # Kerberos认证
    password = os.environ.get("KERBEROS_PASSWORD", "<YOUR_IAM_PASSWORD>")
    # kinit_result = subprocess.run(
    #     "echo 'Zhlg#mrs3' | kinit mrs_check",
    #     shell=True,
    #     executable="/bin/bash",
    #     check=False,  # 先不抛出异常，自定义日志
    #     stdout=subprocess.PIPE,
    #     stderr=subprocess.PIPE,
    #     text=True
    # )
    #
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
        # 没有密码环境变量，尝试无密码kinit（如果已有缓存凭据）
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
    """从Hive库中获取指定表的建表语句。

    使用PyHive连接Hive（支持Kerberos认证）执行SHOW CREATE TABLE命令。

    Args:
        table_name: Hive表名。
        to_database: Hive数据库名称。

    Returns:
        建表语句字符串。

    Raises:
        ImportError: 未安装PyHive或相关依赖。
        Exception: Hive连接或查询失败。
    """
    try:
        from pyhive import hive
    except ImportError:
        raise ImportError(
            "获取Hive DDL需要PyHive库。请执行: pip install pyhive[hive] thrift sasl"
        )

    conn = None
    cursor = None
    try:
        # 步骤1：执行环境变量加载 + Kerberos认证（参考Hive_连接示例2.py）
        init_env_and_auth()

        LOGGER.info("[hive] 正在连接Hive: %s:%s", HIVE_CONFIG["host"], HIVE_CONFIG["port"])

        # 构建连接参数，参考 HIVE_连接示例.py 的方式
        # 关键：使用 krbhost 参数指定Kerberos认证域名
        conn_params = {
            "host": HIVE_CONFIG["host"],
            "port": HIVE_CONFIG["port"],
            "username": HIVE_CONFIG["username"],
            "database": to_database,
        }


        # 如果启用了Kerberos认证，添加相关参数
        if HIVE_CONFIG.get("auth") == "KERBEROS":
            conn_params["auth"] = "KERBEROS"
            conn_params["kerberos_service_name"] = HIVE_CONFIG["kerberos_service_name"]
            # 关键参数：krbhost 用于Kerberos认证，区别于 host（网络连接）
            if HIVE_CONFIG.get("krbhost"):
                conn_params["krbhost"] = HIVE_CONFIG["krbhost"]
        LOGGER.info("[hive] 连接参数: %s", conn_params)
        # 参考 HIVE_连接示例.py 使用 hive.Connection
        conn = hive.Connection(**conn_params)
        LOGGER.info("[hive] 连接成功")
        cursor = conn.cursor()

        # 执行SHOW CREATE TABLE命令
        sql = "SHOW CREATE TABLE {}".format(table_name)
        LOGGER.info("[hive] 执行SQL: %s", sql)
        cursor.execute(sql)

        # 获取结果（建表语句可能跨多行）
        result = cursor.fetchall()
        ddl = "\n".join([row[0] for row in result])

        LOGGER.info("[hive] 成功获取表 %s 的建表语句", table_name)
        return ddl

    except Exception as exc:
        error_msg = str(exc)
        LOGGER.error("[hive] 获取建表语句失败: %s", error_msg)

        # 获取配置信息用于诊断
        hive_host = HIVE_CONFIG["host"]  # IP地址（网络连接）
        krb_host = HIVE_CONFIG.get("krbhost", hive_host)  # 域名（Kerberos认证）

        # 提供详细的错误诊断信息
        if "Name or service not known" in error_msg or "failed to resolve" in error_msg:
            LOGGER.error("[hive] DNS解析错误诊断:")
            LOGGER.error("[hive] 无法解析域名: %s", krb_host)
            LOGGER.error("[hive] 当前配置: host=%s (IP), krbhost=%s (Kerberos域名)", hive_host, krb_host)
            LOGGER.error("[hive] 解决方案:")
            LOGGER.error("[hive] 1. 在服务器上配置hosts解析，执行以下命令:")
            LOGGER.error('[hive]    sudo sh -c \'echo "<INTERNAL_HIVE_HOST> %s" >> /etc/hosts\'', krb_host)
            LOGGER.error("[hive] 2. 或者联系网络管理员配置DNS解析")
            LOGGER.error("[hive] 3. 验证连接: ping %s", krb_host)

        # Kerberos认证错误诊断
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

        # Kerberos ticket不可用错误
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
    """在Hive中执行DDL操作：删除表并重新创建。

    Args:
        to_database: Hive数据库名称。
        table_name: 要删除和重建的表名。
        create_ddl: 创建表的DDL语句。

    Returns:
        True: 执行成功

    Raises:
        Exception: Hive连接或DDL执行失败。
    """
    try:
        from pyhive import hive
    except ImportError:
        raise ImportError(
            "执行Hive DDL需要PyHive库。请执行: pip install pyhive[hive] thrift sasl"
        )

    conn = None
    cursor = None
    try:
        # 执行Kerberos认证
        init_env_and_auth()

        LOGGER.info("[hive] 正在连接Hive执行DDL: %s:%s", HIVE_CONFIG["host"], HIVE_CONFIG["port"])

        # 构建连接参数
        conn_params = {
            "host": HIVE_CONFIG["host"],
            "port": HIVE_CONFIG["port"],
            "username": HIVE_CONFIG["username"],
            "database": to_database,
        }

        # 如果启用了Kerberos认证，添加相关参数
        if HIVE_CONFIG.get("auth") == "KERBEROS":
            conn_params["auth"] = "KERBEROS"
            conn_params["kerberos_service_name"] = HIVE_CONFIG["kerberos_service_name"]
            if HIVE_CONFIG.get("krbhost"):
                conn_params["krbhost"] = HIVE_CONFIG["krbhost"]

        conn = hive.Connection(**conn_params)
        cursor = conn.cursor()

        # 步骤1：删除表
        drop_sql = "DROP TABLE IF EXISTS {}".format(table_name)
        LOGGER.info("[hive] 执行DDL: %s", drop_sql)
        cursor.execute(drop_sql)
        LOGGER.info("[hive] 表 %s 删除成功", table_name)

        # 步骤2：执行创建表的DDL
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


def parse_hive_columns(ddl):
    """从Hive建表语句中解析字段列表。

    Args:
        ddl: Hive建表语句。

    Returns:
        字段名字符串列表。

    Raises:
        ValueError: 解析失败。
    """
    # 移除注释和多余空白
    ddl_clean = re.sub(r'/\*.*?\*/', '', ddl, flags=re.DOTALL)
    ddl_clean = re.sub(r'--.*?\n', '\n', ddl_clean)

    # 提取列定义部分（CREATE TABLE (...) 中的内容）
    # 匹配CREATE TABLE语句后的括号内容
    match = re.search(
        r'CREATE\s+TABLE\s+[^\(]*\((.*?)\)\s*(?:PARTITIONED\s+BY|STORED\s+AS|ROW\s+FORMAT|TBLPROPERTIES|;)',
        ddl_clean, re.IGNORECASE | re.DOTALL
    )

    if not match:
        # 尝试另一种匹配方式
        match = re.search(
            r'CREATE\s+TABLE\s+[^\(]*\((.*)\)',
            ddl_clean, re.IGNORECASE | re.DOTALL
        )

    if not match:
        raise ValueError("无法从DDL中解析列定义")

    columns_section = match.group(1)

    # 解析各个字段定义
    columns = []
    # 按逗号分割字段定义（注意处理嵌套类型如ARRAY<>, MAP<>, STRUCT<>）
    # 使用正则表达式匹配字段名（字段名在类型定义之前）
    field_pattern = r'`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s+([A-Z]+)'

    for line in columns_section.split('\n'):
        line = line.strip()
        if not line or line.startswith('--'):
            continue

        # 尝试匹配字段定义
        match_field = re.match(r'`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s+', line)
        if match_field:
            col_name = match_field.group(1)
            # 排除Hive关键字和非字段行
            if col_name.upper() not in ('PRIMARY', 'FOREIGN', 'CONSTRAINT', 'PARTITIONED', 'STORED'):
                columns.append(col_name)

    return columns


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


def extract_columns_from_ddl(ddl):
    """从修改后的建表语句中提取所有字段（包括新增字段），并拼接成指定格式。

    Args:
        ddl: 修改后的建表语句。

    Returns:
        字段名字符串，格式为 "field1&field2&field3"。
    """
    columns = parse_hive_columns(ddl)
    # 添加分区字段
    columns.append(PARTITION_FIELD["name"])
    return "&".join(columns)


def main():
    """程序入口：执行完整CDM作业流程。

    完整流程：
        1. 创建作业
        2. 执行作业（第一次）
        2.5 查询作业执行状态（轮询直到SUCCEEDED）
        3. 从Hive获取建表语句
        4. 修改建表语句（添加默认字段和分区）
        4.5 删除当前表并执行修改后的建表语句（重建表结构）
        5. 调用修改作业接口（更新字段列表）
        6. 再次执行作业
        6.5 查询作业执行状态（轮询直到SUCCEEDED）

    命令行参数（位置参数）：
        sys.argv[1]: project_id - 华为云项目 ID
        sys.argv[2]: cluster_id - CDM 集群 ID
        sys.argv[3]: group_id - 作业分组ID（可选，默认"1"）
        sys.argv[4]: group_name - 作业分组名称（可选，默认"DEFAULT"）
        sys.argv[5]: job_name - 作业名称
        sys.argv[6]: from_link_name - 源连接名称
        sys.argv[7]: schema_name - 源数据库 schema 名称
        sys.argv[8]: from_table_name - 源数据库表名
        sys.argv[9]: to_database - 目标 Hive 数据库名称
        sys.argv[10]: to_table_name - 目标 Hive 表名
        sys.argv[11]: data_source - 数据来源标识（可选，默认"<示例委办局>-i<示例业务场景>-i本市智慧养老系统"）

    Returns:
        进程退出码，0 表示成功；1 表示网络/认证错误；2 表示入参不合法。
    """
    logging.basicConfig(
        level=logging.INFO,
        format="[%(name)s] [%(levelname)s] %(message)s",
    )

    # 支持8个必需参数 + 3个可选参数（group_id, group_name, data_source）
    min_args = 8
    max_args = 11
    actual_args = len(sys.argv) - 1

    if actual_args < min_args or actual_args > max_args:
        LOGGER.error(
            "[cdm_job] 参数数量错误，期望 %d-%d 个参数，实际 %d 个。",
            min_args, max_args, actual_args,
        )
        print(
            "用法: python script.py <project_id> <cluster_id> "
            "[<group_id> <group_name>] <job_name> <from_link_name> <schema_name> "
            "<from_table_name> <to_database> <to_table_name> [<data_source>]",
            file=sys.stderr,
        )
        print(
            "示例: python script.py 2d06fe3f78324f7d9b45abdc4db9e2a8 "
            "3bdb7eec-5a76-46f9-a0ee-61cbecf20962 1 DEFAULT "
            '\"同步测试\" DM LGYWTG ONM_SZLG_MSSQ_CASE_APPEAL ods_lgbs ONM_SZLG_MSSQ_CASE_APPEAL_0108 '
            '\"<示例委办局>-i<示例业务场景>-i本市智慧养老系统\"',
            file=sys.stderr,
        )
        return 2

    project_id = sys.argv[1]
    cluster_id = sys.argv[2]
    # 可选参数：group_id 和 group_name（默认值分别为 "1" 和 "DEFAULT"）
    group_id = sys.argv[3] if actual_args >= 3 else "1"
    group_name = sys.argv[4] if actual_args >= 4 else "DEFAULT"
    job_name = sys.argv[5]
    from_link_name = sys.argv[6]
    schema_name = sys.argv[7]
    from_table_name = sys.argv[8]
    to_database = sys.argv[9]
    to_table_name = sys.argv[10]
    # 可选参数：data_source（数据来源标识，用于填充 lgdsj_data_source 字段）
    data_source = sys.argv[11] if actual_args >= 11 else None

    LOGGER.info("[cdm_job] 作业分组配置: group_id=%s, group_name=%s", group_id, group_name)
    if data_source:
        LOGGER.info("[cdm_job] 数据来源标识: data_source=%s", data_source)

    # 构建URL
    detail_url = build_detail_url(project_id, cluster_id)
    LOGGER.info("[detail_url] === 创建作业链接 === url=%s",detail_url)
    start_url = build_start_job_url(project_id, cluster_id, job_name)
    LOGGER.info("[start_url] === 执行作业链接 === url=%s", start_url)
    update_url = build_update_job_url(project_id, cluster_id, job_name)
    LOGGER.info("[update_url] === 修改作业链接 === url=%s", update_url)
    status_url = build_status_job_url(project_id, cluster_id, job_name)
    LOGGER.info("[status_url] === 查询作业状态链接 === url=%s", status_url)

    # 获取认证Token
    try:
        x_auth_token = get_x_auth_token(project_id)
    except urllib.error.URLError:
        LOGGER.exception("[cdm_job] 获取 token 失败 iam_url=%s", IAM_URL)
        return 1

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
        is_update=False,  # 创建作业时不包含columnList和sampleValueColumn
        group_id=group_id,
        group_name=group_name,
        data_source=data_source,
    )
    LOGGER.exception("[cdm_job] === 步骤1: 创建作业 ===  参数=%s", job_payload)

    try:
        code, raw, parsed = create_cdm_job(
            job_payload,
            x_auth_token=x_auth_token,
            detail_url=detail_url,
            project_id=project_id,
        )
    except urllib.error.URLError as exc:
        LOGGER.exception("[cdm_job] 创建作业网络请求失败 url=%s", detail_url)
        return 1

    if parsed is not None:
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    else:
        print(raw)

    if not (200 <= code < 300):
        LOGGER.error("[cdm_job] 创建作业失败 HTTP %s", code)
        return 1

    LOGGER.info("[cdm_job] 创建作业成功 HTTP %s", code)

    # 步骤2：执行作业（第一次）
    LOGGER.info("[cdm_job] === 步骤2: 第一次执行作业 ===")
    try:
        code, raw, parsed = start_cdm_job(
            x_auth_token=x_auth_token,
            start_url=start_url,
            project_id=project_id,
        )
    except urllib.error.URLError as exc:
        LOGGER.exception("[cdm_job] 第一次执行作业网络请求失败 url=%s", start_url)
        return 1

    if parsed is not None:
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    else:
        print(raw)

    if not (200 <= code < 300):
        LOGGER.error("[cdm_job] 第一次执行作业失败 HTTP %s", code)
        return 1

    LOGGER.info("[cdm_job] 第一次执行作业成功 HTTP %s", code)

    # 步骤2.5：轮询查询作业执行状态，直到任务完成或失败
    LOGGER.info("[cdm_job] === 步骤2.5: 查询作业执行状态 ===")
    wait_result = wait_for_job_completion(
        x_auth_token=x_auth_token,
        status_url=status_url,
        project_id=project_id,
        job_name=job_name,
        check_interval=10.0,  # 每10秒查询一次
        max_wait_time=3600.0,  # 最大等待1小时
    )

    if wait_result != 0:
        # 任务执行失败或超时
        if wait_result == 2:
            LOGGER.error("[cdm_job] 作业执行状态异常，终止流程")
        elif wait_result == 3:
            LOGGER.error("[cdm_job] 作业执行超时，终止流程")
        else:
            LOGGER.error("[cdm_job] 查询作业状态失败，终止流程")
        return 1

    LOGGER.info("[cdm_job] 作业执行完成，继续下一步")

    # 步骤3：从Hive获取建表语句
    LOGGER.info("[cdm_job] === 步骤3: 从Hive获取建表语句 ===")
    try:
        original_ddl = get_hive_ddl(to_table_name, to_database)
        LOGGER.info("[cdm_job] 原始建表语句:\n%s", original_ddl[:500] + "..." if len(original_ddl) > 500 else original_ddl)
    except ImportError as exc:
        LOGGER.error("[cdm_job] %s", exc)
        return 1
    except Exception as exc:
        LOGGER.error("[cdm_job] 获取建表语句失败: %s", exc)
        return 1

    # 步骤4：修改建表语句
    LOGGER.info("[cdm_job] === 步骤4: 修改建表语句 ===")
    try:
        modified_ddl = modify_hive_ddl(original_ddl)
        LOGGER.info("[cdm_job] 修改后的建表语句:\n%s", modified_ddl)
    except Exception as exc:
        LOGGER.error("[cdm_job] 修改建表语句失败: %s", exc)
        return 1

    # 步骤4.5：删除当前表并执行修改后的建表语句
    LOGGER.info("[cdm_job] === 步骤4.5: 删除表并重建 ===")
    try:
        execute_hive_ddl(to_database, to_table_name, modified_ddl)
        LOGGER.info("[cdm_job] 表 %s 删除并重建成功", to_table_name)
    except ImportError as exc:
        LOGGER.error("[cdm_job] %s", exc)
        return 1
    except Exception as exc:
        LOGGER.error("[cdm_job] 删除并重建表失败: %s", exc)
        return 1

    # 步骤5：调用修改作业接口
    LOGGER.info("[cdm_job] === 步骤5: 调用修改作业接口 ===")
    try:
        # 从修改后的DDL中提取字段列表
        column_list = extract_columns_from_ddl(modified_ddl)
        LOGGER.info("[cdm_job] 提取的字段列表: %s", column_list[:100] + "..." if len(column_list) > 100 else column_list)

        # 构建更新后的作业参数
        updated_payload = build_cdm_job_payload(
            project_id=project_id,
            from_table_name=from_table_name,
            to_table_name=to_table_name,
            from_link_name=from_link_name,
            job_name=job_name,
            schema_name=schema_name,
            to_database=to_database,
            column_list=column_list,
            is_update=True,  # 修改作业时包含columnList和sampleValueColumn
            group_id=group_id,
            group_name=group_name,
            data_source=data_source,
        )
        LOGGER.exception("[cdm_job] === 步骤5: 调用修改作业接口 ===  参数=%s", updated_payload)
        code, raw, parsed = update_cdm_job(
            updated_payload,
            x_auth_token=x_auth_token,
            update_url=update_url,
            project_id=project_id,
        )
    except urllib.error.URLError as exc:
        LOGGER.exception("[cdm_job] 修改作业网络请求失败 url=%s", update_url)
        return 1
    except Exception as exc:
        LOGGER.error("[cdm_job] 修改作业失败: %s", exc)
        return 1

    if parsed is not None:
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    else:
        print(raw)

    if not (200 <= code < 300):
        LOGGER.error("[cdm_job] 修改作业失败 HTTP %s", code)
        return 1

    LOGGER.info("[cdm_job] 修改作业成功 HTTP %s", code)

    # 步骤6：再次执行作业
    LOGGER.info("[cdm_job] === 步骤6: 再次执行作业 ===")
    try:
        code, raw, parsed = start_cdm_job(
            x_auth_token=x_auth_token,
            start_url=start_url,
            project_id=project_id,
        )
    except urllib.error.URLError as exc:
        LOGGER.exception("[cdm_job] 第二次执行作业网络请求失败 url=%s", start_url)
        return 1

    if parsed is not None:
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    else:
        print(raw)

    if not (200 <= code < 300):
        LOGGER.error("[cdm_job] 第二次执行作业失败 HTTP %s", code)
        return 1

    LOGGER.info("[cdm_job] 第二次执行作业成功 HTTP %s", code)

    # 步骤6.5：轮询查询作业执行状态，直到任务完成或失败
    LOGGER.info("[cdm_job] === 步骤6.5: 查询作业执行状态 ===")
    wait_result = wait_for_job_completion(
        x_auth_token=x_auth_token,
        status_url=status_url,
        project_id=project_id,
        job_name=job_name,
        check_interval=10.0,  # 每10秒查询一次
        max_wait_time=3600.0,  # 最大等待1小时
    )

    if wait_result != 0:
        # 任务执行失败或超时
        if wait_result == 2:
            LOGGER.error("[cdm_job] 作业执行状态异常，终止流程")
        elif wait_result == 3:
            LOGGER.error("[cdm_job] 作业执行超时，终止流程")
        else:
            LOGGER.error("[cdm_job] 查询作业状态失败，终止流程")
        return 1

    LOGGER.info("[cdm_job] 作业执行完成")
    LOGGER.info("[cdm_job] === 完整流程执行成功 ===")

    return 0


if __name__ == "__main__":
    sys.exit(main())
