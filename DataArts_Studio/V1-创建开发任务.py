# -*- coding: utf-8 -*-
"""从 V1-CDM作业_开发作业_数据库直连获取源字段.py 复制的开发任务逻辑（IAM Token + 创建/启动作业）。

独立运行时的位置参数与 2创建开发任务.py 一致（最多 14 个）；合并脚本仍可自行实现参数解析后调用
run_dev_job_phase(args, token)。
"""

from __future__ import print_function

import json
import logging
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

LOGGER = logging.getLogger(__name__)
ssl._create_default_https_context = ssl._create_unverified_context

# ========== IAM 鉴权配置（与 V1-CDM 脚本一致）==========
IAM_URL = "https://<INTERNAL_IAM_HOST>/v3/auth/tokens"
IAM_USERNAME = "admin_user"
IAM_PASSWORD = "<YOUR_IAM_PASSWORD>"
IAM_DOMAIN_NAME = "政务大数据治理平台"

# ========== DataArts Studio API 配置（与 V1-CDM 脚本一致）==========
DATAARTS_BASE_URL = "https://dayu-dlf.<INTERNAL_REGION>.example.gov.cn"


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


def _parse_cli_args():
    """解析独立运行时的命令行（与 2创建开发任务.py 相同的 14 个位置参数约定）。"""
    argv = sys.argv[1:]
    if len(argv) < 9:
        return None

    project_id = argv[0]
    workspace_raw = argv[1] if len(argv) > 1 else ""
    workspace_id = workspace_raw.replace("-", "") if workspace_raw else ""

    return {
        "project_id": project_id,
        "workspace_id": workspace_id,
        "dev_name": argv[2] if len(argv) > 2 else "",
        "directory": argv[3] if len(argv) > 3 else "",
        "node_name": argv[4] if len(argv) > 4 else "",
        "cluster_name": argv[5] if len(argv) > 5 else "",
        "cluster_id": argv[6] if len(argv) > 6 else "",
        "cdm_job_name": argv[7] if len(argv) > 7 else "",
        "cron_expression": argv[8] if len(argv) > 8 else "0 20 1 * * ?",
        "interval_type": argv[9] if len(argv) > 9 else "days",
        "owner": argv[10] if len(argv) > 10 else "liguozhuang",
        "description": argv[11] if len(argv) > 11 else "",
        "location_x": argv[12] if len(argv) > 12 else "705",
        "location_y": argv[13] if len(argv) > 13 else "636",
    }


def _cli_usage():
    print(
        "用法: python V1-创建开发任务.py <project_id> <workspace_id> <dev_name> ...\n"
        "参数顺序与 2创建开发任务.py 相同，至少 9 个（含 cron）。",
        file=sys.stderr,
    )


def main():
    """独立运行：先 get_x_auth_token，再 run_dev_job_phase。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    args = _parse_cli_args()
    if args is None:
        _cli_usage()
        sys.exit(2)

    for key in (
        "project_id", "dev_name", "directory", "node_name",
        "cluster_name", "cluster_id", "cdm_job_name",
    ):
        if not args.get(key):
            LOGGER.error("[main] 缺少必填参数: %s", key)
            _cli_usage()
            sys.exit(2)

    try:
        token = get_x_auth_token(args["project_id"])
    except (urllib.error.URLError, ValueError) as exc:
        LOGGER.exception("[main] 获取 Token 失败: %s", exc)
        sys.exit(1)

    sys.exit(run_dev_job_phase(args, token))


if __name__ == "__main__":
    main()
