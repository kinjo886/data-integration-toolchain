# -*- coding: utf-8 -*-
"""
华为云 DataArts Studio + CDM 客户端（本市政务云平台本地网络 IAM 鉴权）。

复用自 DataArts Studio/ 目录下已有脚本的鉴权和 API 调用模式：
- IAM Token: POST https://<INTERNAL_IAM_HOST>/v3/auth/tokens → X-Subject-Token
- CDM API:  https://cdm.example.gov.cn/v1.1/...
- DataArts: https://dayu-dlf.<INTERNAL_REGION>.example.gov.cn/v1/...
"""

import json
import logging
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

# ==================== 环境变量配置 ====================

IAM_URL = os.getenv("IAM_URL", "https://<INTERNAL_IAM_HOST>/v3/auth/tokens")
IAM_USERNAME = os.getenv("IAM_USERNAME", "admin_user")
IAM_PASSWORD = os.getenv("IAM_PASSWORD", "<YOUR_IAM_PASSWORD>")
IAM_DOMAIN_NAME = os.getenv("IAM_DOMAIN_NAME", "政务大数据治理平台")

CDM_BASE_URL = os.getenv("CDM_BASE_URL", "https://cdm.example.gov.cn")
DATAARTS_BASE_URL = os.getenv("DATAARTS_BASE_URL", "https://dayu-dlf.<INTERNAL_REGION>.example.gov.cn")

API_RETRY_TIMES = int(os.getenv("HW_RETRY_TIMES", "3"))
RETRY_SLEEP = int(os.getenv("HW_RETRY_SLEEP", "2"))

LOGGER = logging.getLogger(__name__)

# 政务云平台本地网络自签证书，跳过 SSL 校验
ssl._create_default_https_context = ssl._create_unverified_context


# ==================== IAM 鉴权 ====================

def get_x_auth_token(project_id, timeout_sec=30.0):
    """通过 IAM 鉴权接口获取 X-Auth-Token（复用已有脚本逻辑）。"""
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
    req = urllib.request.Request(IAM_URL, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        token = (resp.getheader("X-Subject-Token") or "").strip()
        if not token:
            raise ValueError("IAM 鉴权成功但未获取到 X-Subject-Token。")
        LOGGER.info("[IAM] Token 获取成功")
        return token


def _make_auth_headers(x_auth_token, workspace_id=None):
    """构建带鉴权的请求头。"""
    headers = {
        "X-Auth-Token": x_auth_token.strip(),
        "Content-Type": "application/json;charset=UTF-8",
        "X-Language": "zh-cn",
    }
    if workspace_id:
        headers["workspace"] = workspace_id
    return headers


def _http_request(method, url, x_auth_token, body=None, workspace_id=None, timeout_sec=120.0):
    """带重试的 HTTP 请求，返回 (http_code, raw_text, parsed_json_or_None)。"""
    headers = _make_auth_headers(x_auth_token, workspace_id)
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body else None

    last_err = None
    for attempt in range(API_RETRY_TIMES):
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                code = int(resp.getcode())
                parsed = None
                try:
                    parsed = json.loads(raw) if raw.strip() else None
                except json.JSONDecodeError:
                    pass
                LOGGER.info("[HTTP] %s %s → %d", method, url[:120], code)
                return code, raw, parsed
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            last_err = exc
            LOGGER.warning("[HTTP] 请求失败 %s %s → %d: %s", method, url[:120], exc.code, raw[:500])
            if attempt < API_RETRY_TIMES - 1:
                time.sleep(RETRY_SLEEP)
        except Exception as exc:
            last_err = exc
            LOGGER.warning("[HTTP] 网络异常 %s %s: %s", method, url[:120], str(exc))
            if attempt < API_RETRY_TIMES - 1:
                time.sleep(RETRY_SLEEP)

    raise RuntimeError(f"华为接口请求失败，重试{API_RETRY_TIMES}次：{last_err}")


# ==================== CDM 客户端 ====================

class CdmClient:
    """CDM 云数据迁移客户端（Oracle → Hive）。"""

    def __init__(self, project_id, cluster_id, x_auth_token):
        self.project_id = project_id
        self.cluster_id = cluster_id
        self.x_auth_token = x_auth_token
        self.base = f"{CDM_BASE_URL}/v1.1/{project_id}/clusters/{cluster_id}/cdm"

    def _build_create_body(self, job_name, from_link_name, schema_name,
                           from_table, to_database, to_table,
                           is_increment=False, column_list=None):
        """构建 CDM 创建作业请求体（参考 V1-CDM作业_开发作业.py）。"""
        # CDM 用 generic-jdbc-connector 连接 Oracle；实际 from-connector-name
        # 使用用户在 Dbt 中配置的源连接名称
        if is_increment:
            incr_value = "true"
        else:
            incr_value = "false"

        from_config_inputs = [
            {"name": "fromJobConfig.useSql", "value": "false"},
            {"name": "fromJobConfig.schemaName", "value": schema_name},
            {"name": "fromJobConfig.tableName", "value": from_table},
            {"name": "fromJobConfig.incrMigration", "value": incr_value},
            {"name": "fromJobConfig.keyAtLeastOneZero", "value": "false"},
            {"name": "fromJobConfig.allowNullValueInPartitionColumn", "value": "true"},
            {"name": "fromJobConfig.cdc", "value": "false"},
            {"name": "fromJobConfig.createOutTable", "value": "false"},
            {"name": "fromJobConfig.enableWriteLobToString", "value": "false"},
            {"name": "fromJobConfig.writeLobDataAsFile", "value": "false"},
            {"name": "fromJobConfig.encodingForBinary", "value": "ISO_8859_1"},
            {"name": "fromJobConfig.usePattern", "value": "ORACLE"},
        ]

        if column_list:
            from_config_inputs.append({"name": "fromJobConfig.columnList", "value": column_list})

        to_config_inputs = [
            {"name": "toJobConfig.hive", "value": "hive"},
            {"name": "toJobConfig.database", "value": to_database},
            {"name": "toJobConfig.table", "value": to_table},
            {"name": "toJobConfig.tablePreparation", "value": "CREATE_WHEN_NOT_EXIST"},
            {"name": "toJobConfig.convertNull", "value": "TO_NULL"},
            {"name": "toJobConfig.csvDelimPolicy", "value": "DROP"},
            {"name": "toJobConfig.shouldClearTable", "value": "false"},
        ]

        if column_list:
            to_config_inputs.append({"name": "toJobConfig.columnList", "value": column_list})

        return {
            "jobs": [{
                "job_type": "NORMAL_JOB",
                "to-config-values": {
                    "configs": [{"inputs": to_config_inputs, "name": "toJobConfig"}],
                },
                "from-config-values": {
                    "configs": [{"inputs": from_config_inputs, "name": "fromJobConfig"}],
                    "extended-configs": {
                        "name": "fromJobConfig.extendedFields",
                        "value": "<INTERNAL_B64>",
                    },
                },
                "from-connector-name": from_link_name,
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
                        {"inputs": [], "name": "transformConfig"},
                        {"inputs": [{"name": "smnConfig.isNeedNotification", "value": "false"}],
                         "name": "smnConfig"},
                    ],
                },
            }]
        }

    def create_job(self, job_name, from_link_name, schema_name,
                   from_table, to_database, to_table, is_increment=False):
        """创建 CDM 迁移作业，返回 job_name（创建成功时与入参一致）。"""
        url = f"{self.base}/job"
        body = self._build_create_body(
            job_name=job_name,
            from_link_name=from_link_name,
            schema_name=schema_name,
            from_table=from_table,
            to_database=to_database,
            to_table=to_table,
            is_increment=is_increment,
        )
        code, _, parsed = _http_request("POST", url, self.x_auth_token, body=body)
        if not (200 <= code < 300):
            raise RuntimeError(f"创建 CDM 作业失败 HTTP{code}: {parsed}")
        LOGGER.info("[CDM] 作业创建成功: %s", job_name)
        return job_name

    def get_job_status(self, job_name):
        """查询 CDM 作业状态。"""
        encoded = urllib.parse.quote(job_name, safe="")
        url = f"{self.base}/job/{encoded}/status"
        _, _, parsed = _http_request("GET", url, self.x_auth_token)
        return parsed.get("data", {}).get("status", "UNKNOWN") if parsed else "UNKNOWN"

    def start_job(self, job_name):
        """手动执行 CDM 作业。"""
        encoded = urllib.parse.quote(job_name, safe="")
        url = f"{self.base}/job/{encoded}/start"
        code, _, parsed = _http_request("POST", url, self.x_auth_token)
        if not (200 <= code < 300):
            raise RuntimeError(f"启动 CDM 作业失败 HTTP{code}")
        LOGGER.info("[CDM] 作业已启动: %s", job_name)
        return True


# ==================== DataArts Studio 客户端 ====================

class DataArtsStudioClient:
    """DataArts Studio 数据开发客户端。"""

    def __init__(self, project_id, workspace_id, x_auth_token):
        self.project_id = project_id
        self.workspace_id = workspace_id
        self.x_auth_token = x_auth_token
        self.base = f"{DATAARTS_BASE_URL}/v1/{project_id}"

    # ---------- 作业 CRUD ----------

    def create_job(self, job_body):
        """创建 DataArts 开发作业。"""
        url = f"{self.base}/jobs"
        code, _, parsed = _http_request(
            "POST", url, self.x_auth_token,
            body=job_body, workspace_id=self.workspace_id,
        )
        if not (200 <= code < 300):
            raise RuntimeError(f"创建 DataArts 作业失败 HTTP{code}: {parsed}")
        job_id = None
        if parsed:
            # 尝试多种返回格式
            job_id = parsed.get("data", {}).get("job_id") or parsed.get("job_id")
        LOGGER.info("[DataArts] 作业创建成功, job_id=%s", job_id)
        return job_id

    def start_job(self, job_name):
        """启动 DataArts 作业（按名称）。"""
        encoded = urllib.parse.quote(job_name, safe="")
        url = f"{self.base}/jobs/{encoded}/start"
        code, _, parsed = _http_request(
            "POST", url, self.x_auth_token,
            workspace_id=self.workspace_id,
        )
        if not (200 <= code < 300):
            raise RuntimeError(f"启动 DataArts 作业失败 HTTP{code}")
        LOGGER.info("[DataArts] 作业已启动: %s", job_name)
        return True

    def get_job_status(self, job_name):
        """按名称查询作业状态。"""
        encoded = urllib.parse.quote(job_name, safe="")
        url = f"{self.base}/jobs/{encoded}"
        _, _, parsed = _http_request(
            "GET", url, self.x_auth_token,
            workspace_id=self.workspace_id,
        )
        if parsed:
            return parsed.get("data", {}).get("status", "UNKNOWN") or parsed.get("status", "UNKNOWN")
        return "UNKNOWN"

    # ---------- 开发作业请求体构建 ----------

    @staticmethod
    def build_dev_job_body(name, directory, node_name, cluster_name, cluster_id,
                           cdm_job_name, cron_expression, interval_type="days",
                           owner="liguozhuang", description="",
                           location_x="705", location_y="636",
                           node_location_x="-207.0", node_location_y="-253.0",
                           start_time="2026-03-23T00:00:00+08"):
        """构建 DataArts 开发任务完整请求体（参考 V1-创建开发任务.py）。"""
        expression = cron_expression.replace("-", " ")
        return {
            "basicConfig": {
                "agency": "",
                "customFields": {},
                "encrypt": False,
                "executeUser": "",
                "instanceTimeout": 0,
                "isIgnoreWaiting": 0,
                "jobDescription": description,
                "owner": owner,
                "priority": 0,
                "tags": [],
                "taskPriority": 0,
            },
            "cleanOverdueDays": 60,
            "cleanWaitingJob": "cleanup",
            "description": description,
            "directory": directory,
            "emptyRunningJob": "0",
            "lastUpdateUser": owner,
            "location": {"x": location_x, "y": location_y},
            "maskedParams": [],
            "name": name,
            "nodes": [{
                "execTimeOutRetry": "false",
                "failPolicy": "FAIL_CHILD",
                "lineageInfo": '[{"outputs":[],"inputs":[]}]',
                "location": {"x": node_location_x, "y": node_location_y},
                "maxExecutionTime": 360,
                "name": node_name,
                "pollingInterval": 20,
                "preNodeName": [],
                "properties": [
                    {"name": "jobType", "value": "existsJob"},
                    {"name": "clusterName", "value": cluster_name},
                    {"name": "clusterId", "value": cluster_id},
                    {"name": "jobName", "value": cdm_job_name},
                    {"name": "emptyRunningJob", "value": "0"},
                    {"name": "taskWorkGroupId", "value": "-1"},
                ],
                "resouces": [],
                "retryInterval": 120,
                "retryTimes": 0,
                "type": "CDMJob",
            }],
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
                    "expression": expression,
                    "expressionTimeZone": "Asia/Shanghai",
                    "intervalType": interval_type,
                    "isSkipSelfDepJob": "false",
                    "monitorObsPath": False,
                    "scanDuration": 0,
                    "scanInterval": 0,
                    "startTime": start_time,
                },
                "requireManualConfirmBeforeExecute": False,
                "scheduleOffset": 1,
                "type": "CRON",
            },
            "singleNodeJobFlag": False,
            "taskWorkGroupId": "",
            "useCdmCache": False,
            "version": "1",
        }


# ==================== Token 管理（全局复用） ====================

class TokenManager:
    """全局 Token 管理器，按 project_id 缓存，自动续期。"""

    def __init__(self):
        self._cache = {}  # project_id → (token, expire_time)

    def get_token(self, project_id):
        """获取 token，过期自动刷新。"""
        now = time.time()
        entry = self._cache.get(project_id)
        if entry:
            token, expire_time = entry
            if now < expire_time - 300:  # 提前 5 分钟刷新
                return token
        token = get_x_auth_token(project_id)
        # Token 默认 24 小时有效，缓存 23 小时
        self._cache[project_id] = (token, now + 23 * 3600)
        return token

    def invalidate(self, project_id):
        """强制清除缓存（鉴权失败时调用）。"""
        self._cache.pop(project_id, None)


# 全局单例
_token_manager = TokenManager()


# ==================== 批量执行器（集成 IAM） ====================

class BatchTaskExecutor:
    """批量任务执行器，集成 IAM Token 管理。"""

    def __init__(self, batch_size=50, default_owner="liguozhuang"):
        self.batch_size = batch_size
        self.default_owner = default_owner
        self.success_list = []
        self.skip_list = []
        self.fail_list = []

    def execute_single(self, clean_row):
        """执行单条迁移记录（CDM + DataArts ODS + 可选 STD）。"""
        from dbt_param_convert import ParamValidator, ParamTemplateRenderer

        project_id = clean_row.get("华为云项目 ID", "")
        cluster_id = clean_row["_cdm_cluster_id"]
        record_id = clean_row["_record_id"]
        workspace_id_raw = clean_row.get("工作空间id(政务数据局空间)", "")

        # 前置校验
        valid, msg = ParamValidator.check_required(clean_row)
        if not valid:
            return {"record_id": record_id, "status": "fail",
                    "reason": f"参数校验失败：{msg}", "remark": ""}

        # 获取 IAM Token
        try:
            token = _token_manager.get_token(project_id)
        except Exception as e:
            self.fail_list.append({"record_id": record_id, "status": "fail",
                                   "reason": f"IAM 鉴权失败：{e}", "remark": ""})
            return None

        # 初始化客户端
        da_client = DataArtsStudioClient(project_id, workspace_id_raw, token)
        cdm_client = CdmClient(project_id, cluster_id, token)

        template = ParamTemplateRenderer.match_template_type(clean_row)
        remark_parts = []
        cdm_ok = False
        ods_ok = False

        try:
            # 1. 创建 CDM 迁移作业
            cdm_name = clean_row.get("三期ods-cdm名称", "")
            from_link = clean_row.get("源连接名称", "")
            schema_name = clean_row.get("源数据库 schema 名称", "")
            from_table = clean_row.get("二期-源表", "")
            to_database = clean_row.get("三期-hive-ods库", "")
            to_table = clean_row.get("三期ods表英文名", "")
            is_incr = clean_row.get("_is_increment", False)

            created_name = cdm_client.create_job(
                job_name=cdm_name,
                from_link_name=from_link,
                schema_name=schema_name,
                from_table=from_table,
                to_database=to_database,
                to_table=to_table,
                is_increment=is_incr,
            )
            remark_parts.append(f"CDM作业:{created_name}")
            cdm_ok = True

            # 2. 创建 ODS 数据开发任务
            ods_job_name = clean_row.get("三期-ODS数据开发作业名称", "")
            ods_directory = clean_row.get("三期-ODS数据开发作业目录", "")
            ods_node_name = f"node_{ods_job_name}"
            cluster_name = clean_row.get("cdm集群名称", "")
            cron_expr = clean_row.get("_cron_expr", "0 20 1 * * ?")

            ods_body = DataArtsStudioClient.build_dev_job_body(
                name=ods_job_name,
                directory=ods_directory,
                node_name=ods_node_name,
                cluster_name=cluster_name,
                cluster_id=cluster_id,
                cdm_job_name=cdm_name,
                cron_expression=cron_expr,
                owner=self.default_owner,
                description=f"ODS迁移任务-{to_table}",
            )
            ods_job_id = da_client.create_job(ods_body)
            da_client.start_job(ods_job_name)
            remark_parts.append(f"ODS任务:{ods_job_id}")
            ods_ok = True

            # 3. STD 任务（按需）
            if template == "std_dev_task":
                std_job_name = clean_row.get("三期-std数据开发作业名称", "")
                std_directory = clean_row.get("三期-ODS数据开发作业目录", "").replace("ODS", "STD", 1)
                std_node_name = f"node_{std_job_name}"
                std_body = DataArtsStudioClient.build_dev_job_body(
                    name=std_job_name,
                    directory=std_directory,
                    node_name=std_node_name,
                    cluster_name=cluster_name,
                    cluster_id=cluster_id,
                    cdm_job_name=cdm_name,
                    cron_expression=cron_expr,
                    owner=self.default_owner,
                    description=f"STD迁移任务-{to_table}",
                )
                std_job_id = da_client.create_job(std_body)
                da_client.start_job(std_job_name)
                remark_parts.append(f"STD任务:{std_job_id}")

            return {
                "record_id": record_id,
                "status": "success",
                "cdm_ok": cdm_ok,
                "ods_ok": ods_ok,
                "remark": "; ".join(remark_parts),
            }

        except Exception as e:
            return {
                "record_id": record_id,
                "status": "fail",
                "reason": str(e)[:800],
                "remark": "; ".join(remark_parts),
            }

    def run_batch(self, clean_record_list):
        """分批串行执行全量记录。"""
        pending = []
        for row in clean_record_list:
            if row.get("_skip_delete"):
                self.skip_list.append({"record_id": row["_record_id"], "reason": "标记为已删除"})
                continue
            if row.get("_skip_finish"):
                self.skip_list.append({"record_id": row["_record_id"], "reason": "三期已完成，跳过创建"})
                continue
            pending.append(row)

        LOGGER.info("[Batch] 待处理 %d 条，跳过 %d 条，分片大小 %d",
                     len(pending), len(self.skip_list), self.batch_size)

        for start in range(0, len(pending), self.batch_size):
            chunk = pending[start:start + self.batch_size]
            LOGGER.info("[Batch] 执行第 %d/%d 批 (%d条)",
                         start // self.batch_size + 1,
                         (len(pending) + self.batch_size - 1) // self.batch_size,
                         len(chunk))
            for item in chunk:
                res = self.execute_single(item)
                if res is None:
                    continue  # IAM 失败已加入 fail_list
                if res["status"] == "success":
                    self.success_list.append(res)
                else:
                    self.fail_list.append(res)

        LOGGER.info("[Batch] 执行完成 - 成功:%d 失败:%d 跳过:%d",
                     len(self.success_list), len(self.fail_list), len(self.skip_list))
        return {
            "success": self.success_list,
            "skip": self.skip_list,
            "fail": self.fail_list,
        }
