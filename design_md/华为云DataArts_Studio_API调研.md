# 华为云 DataArts Studio API 调研报告

> **用途**：为复刻华为云 DataArts Studio MCP 连接器（模仿金山 kdocs-gov 连接器结构）提供完整 API 参考。
> **数据来源**：华为云官方帮助中心 + 华为云 Stack 企业文档 + 本地生产脚本（`DataArts Studio/*.py`）交叉验证。
> **整理日期**：2026-08-22

---

## 一、产品与 API 体系概述

DataArts Studio（数据治理中心）提供两大类 REST API：

| 体系 | 别名 | 基路径 | 用途 | 鉴权头 |
|------|------|--------|------|--------|
| **CDM 数据集成** | CDM / 云数据迁移 | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/...` | 数据迁移作业（表/文件/整库迁移） | `X-Auth-Token` |
| **数据开发** | DataArts Factory / DLF | `/v1/{project_id}/...` | 开发作业编排（DAG 工作流、脚本、资源） | `X-Auth-Token` + `workspace` |

### 政务本地网络实际端点（从本地脚本提取）

| 配置项 | 值 | 来源脚本 |
|--------|----|----------|
| IAM 鉴权 URL | `https://<INTERNAL_IAM_HOST>/v3/auth/tokens` | 全部脚本 |
| IAM 用户名 | `admin_user` | 全部脚本 |
| IAM 密码 | `<YOUR_IAM_PASSWORD>` | 全部脚本 |
| IAM Domain | `政务大数据治理平台` | 全部脚本 |
| CDM 端点 | `https://cdm.example.gov.cn` | V2-创建CDM作业*.py |
| DataArts 端点 | `https://dayu-dlf.<INTERNAL_REGION>.example.gov.cn` | V1-CDM作业_开发作业.py |
| 默认 project_id | `2d06fe3f78324f7d9b45abdc4db9e2a8` | 脚本示例参数 |
| 默认 cluster_id | `3bdb7eec-5a76-46f9-a0ee-61cbecf20962` | 脚本示例参数 |
| 证书 | HTTPS 自签，需关闭校验（`ssl._create_unverified_context`） | 全部脚本 |

---

## 二、鉴权：IAM Token 获取

### 接口

```
POST https://{iam_host}/v3/auth/tokens
Content-Type: application/json;charset=utf8
```

### 请求体

```json
{
  "auth": {
    "identity": {
      "methods": ["password"],
      "password": {
        "user": {
          "name": "admin_user",
          "password": "<YOUR_IAM_PASSWORD>",
          "domain": { "name": "政务大数据治理平台" }
        }
      }
    },
    "scope": { "project": { "id": "{project_id}" } }
  }
}
```

### 响应

- **HTTP 201**，响应头 `X-Subject-Token` 即为 Token 值
- Token 有效期默认 24 小时（可从响应体 `token.expires_at` 解析）
- 后续所有业务接口请求头携带 `X-Auth-Token: {token值}`

### 连接器实现要点

- 模仿金山连接器的 `cachedToken` 机制：首次获取后缓存，带 `expires_at`，过期自动续
- 建议落盘 `iam-token.json`（类似金山的 `user-token.json`），重启不丢
- **无需扫码授权**——华为云是机器账号密码鉴权，不需要 `user-auth-server.js`

---

## 三、CDM 数据集成 API

### 3.1 作业管理（9 个，核心）

#### ① 指定集群创建作业 — CreateJob

| 项 | 值 |
|----|----|
| 方法 | `POST` |
| URI | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/job` |
| 限流 | 1200 次/min |
| 请求头 | `X-Auth-Token`(必选)、`Content-Type: application/json` |
| 脚本实际 | 额外加了 `workspace: {project_id}`、`X-Language: zh-cn` |

**路径参数**：

| 参数 | 类型 | 必选 | 说明 |
|------|------|------|------|
| project_id | String | 是 | 项目 ID |
| cluster_id | String | 是 | CDM 集群 ID |

**请求体**：

```json
{
  "jobs": [
    {
      "job_type": "NORMAL_JOB",
      "name": "作业名(1~240字符)",
      "from-connector-name": "generic-jdbc-connector",
      "to-connector-name": "hive-connector",
      "from-link-name": "源连接名",
      "to-link-name": "目的连接名",
      "from-config-values": { "configs": [...] },
      "to-config-values": { "configs": [...] },
      "driver-config-values": { "configs": [...] }
    }
  ]
}
```

**job_type 枚举**：`NORMAL_JOB`(表/文件迁移)、`BATCH_JOB`(整库迁移)、`SCENARIO_JOB`(场景迁移)

**连接器类型枚举**（from/to-connector-name）：
`generic-jdbc-connector`(关系数据库)、`obs-connector`、`hdfs-connector`、`hbase-connector`、`hive-connector`、`ftp-connector`/`sftp-connector`、`mongodb-connector`、`redis-connector`、`kafka-connector`、`dis-connector`、`elasticsearch-connector`、`dli-connector`、`http-connector`、`dms-kafka-connector`

**响应**：

```json
{ "name": "作业名", "validation-result": [] }
```
> `validation-result` 空列表 = 成功；非空 = 校验失败原因

---

#### ② 随机集群创建作业并执行 — CreateAndStartRandomClusterJob

| 项 | 值 |
|----|----|
| 方法 | `POST` |
| URI | `/v1.1/{project_id}/clusters/job` |
| 限流 | 120 次/min |

**请求体**：`{ "jobs": [{Job}], "clusters": ["集群ID1", "集群ID2"] }`（系统随机选一个开机集群创建并执行）

**响应**：`{ "submissions": [{StartJobSubmission}] }`

---

#### ③ 启动作业 — StartJob

| 项 | 值 |
|----|----|
| 方法 | `PUT` |
| URI | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/job/{job_name}/start` |
| 限流 | 1200 次/min |
| 请求头 | `X-Auth-Token`(必选)、`Content-Type: application/json` |
| 脚本实际 | `method="PUT"`，无请求体，额外加 `workspace`/`X-Language` 头 |

**请求体**：`{ "variables": {} }`（作业变量，无变量时空对象；脚本中未传 body）

**响应**：`{ "submissions": [{StartJobSubmission}] }`

> ⚠️ **方法校正**：官方文档标注 PUT，本地脚本 `start_cdm_job()` 也用 `method="PUT"` ✓ 一致

---

#### ④ 停止作业 — StopJob

| 项 | 值 |
|----|----|
| 方法 | `PUT` |
| URI | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/job/{job_name}/stop` |
| 限流 | 1200 次/min |

**请求体**：无
**响应**：HTTP 200 空体

---

#### ⑤ 查询作业状态 — ShowJobStatus

| 项 | 值 |
|----|----|
| 方法 | `GET` |
| URI | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/job/{job_name}/status` |
| 限流 | 120 次/min |
| 脚本实际 | `get_cdm_job_status()` 用 `method="GET"` ✓ |

**响应**：

```json
{
  "submissions": [
    {
      "submission-id": 123,
      "job-name": "作业名",
      "status": "RUNNING",
      "progress": 0.75,
      "is-execute-auto": false,
      "creation-user": "admin_user",
      "creation-date": 1692672000000,
      "execute-date": 1692672060000,
      "last-update-date": 1692672120000,
      "delete_rows": 0,
      "update_rows": 0,
      "write_rows": 1000,
      "external-id": "job_xxx",
      "isStopingIncrement": "false",
      "isDeleteJob": false,
      "error-details": "",
      "error-summary": ""
    }
  ]
}
```

**status 枚举**：`BOOTING`(启动中)、`FAILURE_ON_SUBMIT`(提交失败)、`RUNNING`(运行中)、`SUCCEEDED`(成功)、`FAILED`(失败)、`UNKNOWN`、`NEVER_EXECUTED`

**counters**（仅 SUCCEEDED 时有）：`BYTES_WRITTEN`、`TOTAL_FILES`、`ROWS_READ`、`BYTES_READ`、`ROWS_WRITTEN`、`FILES_WRITTEN`、`FILES_READ`、`ROWS_WRITTEN_SKIPPED`

---

#### ⑥ 查询作业执行历史 — ShowSubmissions

| 项 | 值 |
|----|----|
| 方法 | `GET` |
| URI | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/submissions?jname={jname}` |
| 限流 | 120 次/min |

**Query 参数**：`jname`(必选,作业名)

**响应**：`{ "submissions": [{Submission}], "total": 100, "page_no": 1, "page_size": 10 }`

---

#### ⑦ 查询作业列表 — ShowJobs

| 项 | 值 |
|----|----|
| 方法 | `GET` |
| URI | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/job/{job_name}` |
| 限流 | 120 次/min |

**路径参数**：`job_name` 传 `all` 查全部，传具体名查单个

**Query 参数**：

| 参数 | 类型 | 必选 | 说明 |
|------|------|------|------|
| filter | String | 否 | job_name=all 时模糊过滤 |
| page_no | Integer | 否 | 页号 |
| page_size | Integer | 否 | 每页 10~100 |
| jobType | String | 否 | NORMAL_JOB/BATCH_JOB/SCENARIO_JOB |

**响应**：`{ "total": 100, "jobs": [{Job完整对象}], "page_no": 1, "page_size": 10 }`

**Job 响应完整字段**：job_type、from/to-connector-name、from/to-link-name、from/to-config-values、driver-config-values、name、creation-user、creation-date、update-date、update-user、external_id、id、enabled、status、is_incrementing、files_read、bytes_written、bytes_read、write_rows、rows_written、rows_read、files_written、delete_rows、update_rows、group_name、flag(定时1/0)、execute_start_date

---

#### ⑧ 修改作业 — UpdateJob

| 项 | 值 |
|----|----|
| 方法 | `PUT` |
| URI | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/job/{job_name}` |
| 限流 | 120 次/min |
| 脚本实际 | `update_cdm_job()` 用 `method="PUT"` ✓ |

**请求体**：`{ "jobs": [{Job}] }`（结构与 CreateJob 一致，所有字段必选）

**响应**：HTTP 200

---

#### ⑨ 删除作业 — DeleteJob

| 项 | 值 |
|----|----|
| 方法 | `DELETE` |
| URI | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/job/{job_name}` |
| 限流 | 120 次/min |

**请求体**：无
**响应**：HTTP 200；失败 500 `{ "errCode": "Cdm.0100", "externalMessage": "..." }`

---

### 3.2 集群管理（8 个，概览）

| 中文名 | 方法 | URI | 限流 |
|--------|------|-----|------|
| 创建集群 | POST | `/v1.1/{project_id}/clusters` | 5次/min |
| 查询集群列表 | GET | `/v1.1/{project_id}/clusters` | 120次/min |
| 查询集群详情 | GET | `/v1.1/{project_id}/clusters/{cluster_id}` | 120次/min |
| 重启集群 | POST | `/v1.1/{project_id}/clusters/{cluster_id}/action` | 20次/min |
| 启动集群 | POST | `/v1.1/{project_id}/clusters/{cluster_id}/action` | 20次/min |
| 停止集群 | POST | `/v1.1/{project_id}/clusters/{cluster_id}/action` | 20次/min |
| 删除集群 | DELETE | `/v1.1/{project_id}/clusters/{cluster_id}` | 20次/min |
| 修改集群 | POST | `/v1.1/{project_id}/cluster/modify/{cluster_id}` | 20次/min |

> 重启/启动/停止均走 `/action`，请求体 `{"restart"|"start"|"stop": {"type":"cdm"}}` 区分动作。

---

### 3.3 连接管理（4 个，概览）

| 中文名 | 方法 | URI | 限流 |
|--------|------|-----|------|
| 创建连接 | POST | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/link` | 120次/min |
| 查询连接列表 | GET | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/link` | 120次/min |
| 查询连接详情 | GET | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/link/{link_name}` | 120次/min |
| 修改连接 | PUT | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/link/{link_name}` | 120次/min |
| 删除连接 | DELETE | `/v1.1/{project_id}/clusters/{cluster_id}/cdm/link/{link_name}` | 120次/min |

**Link 结构**：`name`、`connector-name`、`enabled`、`link-config-values`(ConfigValues，configs 内 name 固定 `linkConfig`)

---

## 四、数据开发 DataArts Factory V1 API

> **请求头统一**：`X-Auth-Token`(必选)、`workspace`(可选，工作空间 ID；不设默认查 default 工作空间，无 default 时必填)、`Content-Type: application/json`
> **限流单位**：次/s（API 级 / 用户级）

### 4.1 作业开发（12 个，核心）

#### ① 创建作业 — CreateJob

| 项 | 值 |
|----|----|
| 方法 | `POST` |
| URI | `/v1/{project_id}/jobs` |
| 限流 | API 300次/s、用户 30次/s |
| 脚本实际 | `create_dev_job()` ✓ |

**请求体**：

```json
{
  "name": "作业名(<=128字符,字母/数字/中文/-/_/.)",
  "nodes": [
    {
      "name": "节点名",
      "type": "HiveSQL",
      "location": { "x": 100, "y": 100 },
      "preNodeName": [],
      "conditions": [],
      "properties": [{ "name": "...", "value": "..." }],
      "failPolicy": "FAIL",
      "maxExecutionTime": 60,
      "retryTimes": 1,
      "retryInterval": 120,
      "pollingInterval": 10
    }
  ],
  "schedule": { "type": "EXECUTE_ONCE" },
  "params": [{ "name": "参数名", "value": "参数值", "type": "variable" }],
  "directory": "/dir/a/",
  "processType": "BATCH",
  "singleNodeJobFlag": false,
  "targetStatus": "SAVED"
}
```

**Node.type 枚举**：`HiveSQL`、`SparkSQL`、`DWSSQL`、`DLISQL`、`Shell`、`CDMJob`、`RESTAPI`、`SMN`、`MRSSpark`、`MapReduce`、`MRSFlinkJob`、`MRSHetuEngine`、`DLISpark`、`RDSSQL`、`Dummy`、`DataMigration`、`OneclickCDC`、`CloudTableManager`、`OBSManager`、`DISTransferTask`、`ModelArtsTrain`

**schedule.type 枚举**：`EXECUTE_ONCE`(执行一次)、`CRON`(定时调度)、`EVENT`(事件触发)

**响应**：HTTP 204（成功无体）；失败 400 `{ "error_code": "DLF.0102", "error_msg": "..." }`

---

#### ② 修改作业 — UpdateJob

| 项 | 值 |
|----|----|
| 方法 | `PUT` |
| URI | `/v1/{project_id}/jobs/{job_name}` |
| 限流 | API 300次/s、用户 30次/s |

**请求体**：同创建作业 + `id`(Long,作业ID)

**响应**：HTTP 204

---

#### ③ 查询作业列表 — ListJobs

| 项 | 值 |
|----|----|
| 方法 | `GET` |
| URI | `/v1/{project_id}/jobs?jobType={jobType}&offset={offset}&limit={limit}&jobName={jobName}` |
| 限流 | API 300次/s、用户 30次/s |

**Query 参数**：

| 参数 | 类型 | 必选 | 说明 |
|------|------|------|------|
| limit | Integer | 否 | [1,1000]，默认 10 |
| offset | Integer | 否 | 默认 0 |
| jobType | String | 否 | REAL_TIME/BATCH，默认 BATCH |
| jobName | String | 否 | 模糊匹配 |
| tags | String | 否 | 逗号分隔标签 |

**响应**：`{ "total": 100, "jobs": [{name, jobType, owner, priority, status}] }`

---

#### ④ 查询作业详情 — ShowJob

| 项 | 值 |
|----|----|
| 方法 | `GET` |
| URI | `/v1/{project_id}/jobs/{job_name}?version={version}` |
| 限流 | API 100次/s、用户 10次/s |

**Query 参数**：`version`(可选，作业版本号，不传查最新)

**响应**：完整作业定义（name/nodes/schedule/params/directory/processType/id/createTime/version 等）

---

#### ⑤ 立即执行作业 — RunJobImmediate

| 项 | 值 |
|----|----|
| 方法 | `POST` |
| URI | `/v1/{project_id}/jobs/{job_name}/run-immediate` |
| 限流 | API 300次/s、用户 30次/s |

**请求体**：`{ "jobParams": [{ "name": "...", "value": "...", "type": "variable" }] }`

**响应**：`{ "instanceId": 12345 }`

---

#### ⑥ 启动作业（调度启动）— StartJob

| 项 | 值 |
|----|----|
| 方法 | `POST` |
| URI | `/v1/{project_id}/jobs/{job_name}/start` |
| 限流 | API 300次/s、用户 30次/s |
| 脚本实际 | `start_dev_job()` ✓ |

**请求体**：

```json
{
  "jobParams": [{ "name": "...", "value": "...", "paramType": "variable" }],
  "start_date": 20241030,
  "ignore_first_self_dep": false
}
```

**响应**：HTTP 204（无体）

> ⚠️ **与"立即执行"的区别**：`run-immediate` 是立即跑一次（返回 instanceId）；`start` 是启动调度计划（按 schedule 定时跑）。

---

#### ⑦ 停止作业 — StopJob

| 项 | 值 |
|----|----|
| 方法 | `POST` |
| URI | `/v1/{project_id}/jobs/{job_name}/stop` |
| 限流 | API 300次/s、用户 30次/s |

**请求体**：无
**响应**：HTTP 204

---

#### ⑧ 删除作业 — DeleteJob

| 项 | 值 |
|----|----|
| 方法 | `DELETE` |
| URI | `/v1/{project_id}/jobs/{job_name}` |
| 限流 | API 300次/s、用户 30次/s |

**Query 参数**：`approvers`(可选，审批人名)

**响应**：无

---

#### ⑨ 停止作业实例 — StopJobInstance

| 项 | 值 |
|----|----|
| 方法 | `POST` |
| URI | `/v1/{project_id}/jobs/{job_name}/instances/{instance_id}/stop` |

**响应**：HTTP 204

---

#### ⑩ 重跑作业实例 — RerunJobInstance

| 项 | 值 |
|----|----|
| 方法 | `POST` |
| URI | `/v1/{project_id}/jobs/{job_name}/instances/{instance_id}/restart` |

**请求体**：

```json
{
  "retry_location": "error_node",
  "job_param_version": "latest_version",
  "ignore_obs_monitor": false
}
```

**retry_location 枚举**：`error_node`(从失败节点重跑)、`first_node`(从头重跑)、`specified_node`(指定节点，需填 `node_name`)

---

#### ⑪ 查询作业实例列表 — ListJobInstances

| 项 | 值 |
|----|----|
| 方法 | `GET` |
| URI | `/v1/{project_id}/jobs/instances/detail?jobName={jobName}&minPlanTime={min}&maxPlanTime={max}&limit={limit}&offset={offset}&status={status}` |

**Query 参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| jobName | String | 批处理作业名或 `[实时作业名]_[节点名]` |
| minPlanTime | Long | 毫秒，默认当天 0 点 |
| maxPlanTime | Long | 毫秒，范围不超过 3 天 |
| limit | int | [1,1000]，默认 10 |
| offset | int | 默认 0 |
| status | String | waiting/running/success/fail/running-exception/pause/manual-stop |
| instanceType | int | 0正常/2手工/5补数据/6子作业/7单次 |
| preciseQuery | boolean | 作业名精确查询 |

**响应**：

```json
{
  "total": 50,
  "instances": [
    {
      "jobName": "作业名",
      "jobId": 123,
      "jobInstanceName": "运行时实例名",
      "status": "success",
      "planTime": 1692672000000,
      "startTime": 1692672060000,
      "endTime": 1692672120000,
      "executeTime": 60000,
      "instanceId": 456,
      "submitTime": 1692672000000,
      "instanceType": 0,
      "errorMessage": ""
    }
  ]
}
```

---

#### ⑫ 查询作业实例详情 — ShowJobInstance

| 项 | 值 |
|----|----|
| 方法 | `GET` |
| URI | `/v1/{project_id}/jobs/{job_name}/instances/{instance_id}` |

**响应**：`{ "jobName", "instanceId", "status", "planTime", "startTime", "endTime", "executeTime", "total"(节点数), "nodes":[{节点实例状态}] }`

---

### 4.2 脚本开发（8 个，概览）

| 中文名 | 方法 | URI |
|--------|------|-----|
| 创建脚本 | POST | `/v1/{project_id}/scripts` |
| 修改脚本 | PUT | `/v1/{project_id}/scripts/{script_name}` |
| 查询脚本信息 | GET | `/v1/{project_id}/script/{script_name}` |
| 查询脚本列表 | GET | `/v1/{project_id}/scripts?offset&limit&scriptName` |
| 查询脚本执行结果 | GET | `/v1/{project_id}/scripts/{script_name}/instances/{instance_id}` |
| 删除脚本 | DELETE | `/v1/{project_id}/scripts/{script_name}` |
| 执行脚本 | POST | `/v1/{project_id}/scripts/{script_name}/execute` |
| 停止脚本实例 | POST | `/v1/{project_id}/scripts/{script_name}/instances/{instance_id}/stop` |

**Script 结构**：`name`(必选,<=128字符)、`type`(必选:SparkSQL/HiveSQL/DWSSQL/Shell/PRESTO/ClickHouseSQL/HetuEngineSQL/PYTHON/ImpalaSQL/FlinkSQL/DLISQL/RDSSQL/SparkPython)、`content`(必选,最大4M)、`directory`、`connectionName`(SQL类必选)、`database`

---

## 五、关键数据结构汇总

### 5.1 CDM Job（jobs 数组元素）

```
Job {
  job_type:          "NORMAL_JOB" | "BATCH_JOB" | "SCENARIO_JOB"
  name:              String  (1~240字符)
  from-connector-name: String  (连接器类型枚举)
  to-connector-name:   String  (连接器类型枚举)
  from-link-name:    String  (源连接名)
  to-link-name:      String  (目的连接名)
  from-config-values:  ConfigValues  (源连接参数)
  to-config-values:    ConfigValues  (目的连接参数)
  driver-config-values: ConfigValues (作业驱动参数)
}
```

### 5.2 ConfigValues

```
ConfigValues {
  configs: [
    {
      inputs: [ { name: String, value: String, type: String } ],
      name:   "fromJobConfig" | "toJobConfig" | "linkConfig",
      id:     Integer,
      type:   String
    }
  ],
  extended-configs: { name: String, value: String }
}
```

### 5.3 driver-config-values 关键参数

| inputs.name | 说明 |
|-------------|------|
| `throttlingConfig.numExtractors` | 并发抽取数 |
| `throttlingConfig.numLoaders` | 并发加载数 |
| `throttlingConfig.recordDirtyData` | 是否记录脏数据 |
| `schedulerConfig.isSchedulerJob` | 是否定时作业 |
| `schedulerConfig.disposableType` | 一次性执行类型 |
| `retryJobConfig.retryJobType` | 重试类型 |
| `groupJobConfig.groupName` | 作业组名 |

### 5.4 StartJobSubmission / Submission（作业运行状态）

```
Submission {
  submission-id:    Integer
  job-name:         String
  status:           "BOOTING" | "FAILURE_ON_SUBMIT" | "RUNNING" |
                    "SUCCEEDED" | "FAILED" | "UNKNOWN" | "NEVER_EXECUTED"
  progress:         Float  (-1失败, 否则0~1)
  is-execute-auto:  Boolean
  creation-user:    String
  creation-date:    Long  (毫秒)
  execute-date:     Long  (毫秒)
  last-update-date: Long  (毫秒)
  delete_rows:      Integer
  update_rows:      Integer
  write_rows:       Integer
  external-id:      String
  error-details:    String  (仅FAILED)
  error-summary:    String  (仅FAILED)
  counters: { counter: { BYTES_WRITTEN, ROWS_READ, ... } }  (仅SUCCEEDED)
}
```

### 5.5 DataArts Factory V1 作业 Node

```
Node {
  name:              String  (<=128字符, 唯一)
  type:              String  (HiveSQL/SparkSQL/Shell/CDMJob/...)
  location:          { x: Int, y: Int }
  preNodeName:       [String]  (前置节点名)
  conditions:        [{ preNodeName, expression }]  (EL表达式条件)
  properties:        [{ name, value }]
  failPolicy:        "FAIL" | "IGNORE" | "SUSPEND" | "FAIL_CHILD"
  maxExecutionTime:  Int  (默认60, 范围5~7200)
  retryTimes:        Int  (默认1)
  retryInterval:     Int  (默认120, 范围5~600)
  pollingInterval:   Int  (默认10)
}
```

---

## 六、本地脚本已实现接口对照

### 6.1 脚本与文档一致性验证

| 脚本函数 | 接口 | 方法 | 脚本 vs 文档 |
|----------|------|------|-------------|
| `create_cdm_job()` | CDM 创建作业 | POST | ✓ 一致 |
| `start_cdm_job()` | CDM 启动作业 | PUT | ✓ 一致（脚本未传 body） |
| `update_cdm_job()` | CDM 修改作业 | PUT | ✓ 一致 |
| `get_cdm_job_status()` | CDM 查询状态 | GET | ✓ 一致 |
| `wait_for_job_completion()` | CDM 轮询状态 | GET | ✓ 封装层，循环调 get_cdm_job_status |
| `create_dev_job()` | 数据开发 创建作业 | POST | ✓ 一致 |
| `start_dev_job()` | 数据开发 启动作业 | POST | ✓ 一致 |

### 6.2 ⚠️ 关键差异：脚本给 CDM 接口加了 `workspace` 头

**文档说法**：CDM 接口仅需 `X-Auth-Token`，`workspace` 是数据开发 V1 才需要的。

**脚本实际**：所有 CDM 接口请求头都带了 `"workspace": project_id`：

```python
# 脚本中的请求头（create/start/update/status 全部如此）
detail_headers = {
    "X-Auth-Token": x_auth_token.strip(),
    "workspace": project_id,              # ← 文档说 CDM 不需要，但脚本加了且生产可用
    "Content-Type": "application/json;charset=UTF-8",
    "X-Language": "zh-cn",
}
```

**结论**：政务本地网络环境的 CDM 网关可能也走 workspace 隔离，或加了不影响。**复刻连接器时建议沿用脚本做法**（CDM 接口也带 workspace + X-Language 头），以保证与生产环境一致。

### 6.3 脚本中未实现但文档有的接口（复刻时可补）

| 接口 | 脚本状态 | 复刻价值 |
|------|----------|----------|
| CDM 停止作业 | 未实现 | ★★★ 高 |
| CDM 删除作业 | 未实现 | ★★★ 高 |
| CDM 查询作业列表 | 未实现 | ★★★ 高 |
| CDM 查询执行历史 | 未实现 | ★★☆ 中 |
| CDM 随机集群创建并执行 | 未实现 | ★☆☆ 低 |
| CDM 连接管理 CRUD | 未实现 | ★★☆ 中 |
| CDM 集群管理 | 未实现 | ★☆☆ 低 |
| 数据开发 查询作业列表 | 未实现 | ★★★ 高 |
| 数据开发 查询作业详情 | 未实现 | ★★★ 高 |
| 数据开发 立即执行 | 未实现 | ★★★ 高 |
| 数据开发 停止/删除作业 | 未实现 | ★★★ 高 |
| 数据开发 作业实例查询 | 未实现 | ★★☆ 中 |
| 数据开发 重跑实例 | 未实现 | ★★☆ 中 |
| 数据开发 脚本开发 | 未实现 | ★☆☆ 低 |

---

## 七、复刻连接器的接口选型建议

### 7.1 第一批工具（对应脚本已实现 + 高价值补充）

| 工具名 | 对应接口 | 来源 |
|--------|----------|------|
| `dataarts_gov_check_auth` | IAM 获取 token | 新建（模仿金山） |
| `dataarts_gov_auth_status` | 查看 token 缓存状态 | 新建（模仿金山） |
| `dataarts_gov_cdm_create_job` | CDM 创建作业 | 脚本已实现 |
| `dataarts_gov_cdm_start_job` | CDM 启动作业 | 脚本已实现 |
| `dataarts_gov_cdm_update_job` | CDM 修改作业 | 脚本已实现 |
| `dataarts_gov_cdm_get_status` | CDM 查询作业状态 | 脚本已实现 |
| `dataarts_gov_cdm_stop_job` | CDM 停止作业 | 文档补充 |
| `dataarts_gov_cdm_delete_job` | CDM 删除作业 | 文档补充 |
| `dataarts_gov_cdm_list_jobs` | CDM 查询作业列表 | 文档补充 |
| `dataarts_gov_dev_create_job` | 数据开发 创建作业 | 脚本已实现 |
| `dataarts_gov_dev_start_job` | 数据开发 启动作业 | 脚本已实现 |
| `dataarts_gov_dev_stop_job` | 数据开发 停止作业 | 文档补充 |
| `dataarts_gov_dev_delete_job` | 数据开发 删除作业 | 文档补充 |
| `dataarts_gov_dev_list_jobs` | 数据开发 查询作业列表 | 文档补充 |
| `dataarts_gov_dev_run_immediate` | 数据开发 立即执行 | 文档补充 |
| `dataarts_gov_request` | 通用透传（任意接口） | 新建（模仿金山） |

### 7.2 第二批工具（按需扩展）

- CDM：查询执行历史、随机集群创建执行、连接管理 CRUD
- 数据开发：查询作业详情、作业实例列表/详情、重跑实例、停止实例、脚本开发 CRUD

### 7.3 与金山连接器的结构对照

| 层 | 金山 kdocs-gov | 华为云 dataarts-gov |
|----|---------------|---------------------|
| 协议层 | JSON-RPC stdio | **照搬** |
| 鉴权 | KSO-1 HMAC-SHA256 + OAuth token | IAM 密码换 token（无签名） |
| 传输 | HTTP `<INTERNAL_API_HOST>:5489` | HTTPS 自签（关证书校验） |
| 工具层 | `kdocs_gov_*` 38 个 | `dataarts_gov_*` ~16 个 |
| 通用透传 | `kdocs_gov_request` | `dataarts_gov_request` |
| 用户授权 | `user-auth-server.js`（扫码） | **省略**（机器账号） |
| 文件结构 | 双文件 | 单文件 `server.js` |

---

## 八、待拍板事项

1. **工具覆盖范围**：第一批 16 个？还是只做脚本已有的 7 个 + 通用透传？
2. **凭据存放**：密码/项目ID/集群ID 硬编码默认值（与脚本一致）还是强制环境变量？
3. **CDM 的 workspace 头**：沿用脚本做法（CDM 也带 workspace + X-Language）还是按文档（CDM 不带）？建议沿用脚本。
4. **Hive 辅助功能**：脚本里有 Hive 建表语句获取/改写逻辑（非 REST API，是 JDBC 连接），是否纳入连接器？建议**不纳入**（连接器只做 REST API 透传，Hive JDBC 属于业务脚本层）。
