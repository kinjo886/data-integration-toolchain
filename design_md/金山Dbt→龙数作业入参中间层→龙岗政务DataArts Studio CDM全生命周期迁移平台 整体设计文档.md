# 金山Dbt→数据治理平台作业入参中间层→本市政务DataArts Studio CDM全生命周期迁移平台 整体设计文档

## 文档版本

V1\.0 \| 编制：大数据研发 \| 日期：2026\-07\-30

## 一、项目背景与目标

### 1\.1 现状痛点

1. **上游数据源**：金山多维表 Dbt 存储全量迁移配置清单（共 60 \+ 业务字段，二期 / 三期 ODS/STD/CDM 全生命周期信息、同步方式、调度、集群、建表语句、作业目录等），人工复制字段、组装 DataArts/CDM 入参极易出错、效率极低。

2. **中间工具**：现有**数据治理平台作业入参工具**仅支持手动录入 JSON 入参，无上游 Dbt 自动拉取、字段映射、批量格式化、自动下发 DataArts 能力。

3. **下游平台**：本市政务华为云 DataArts Studio（数据开发）\+ CDM 云数据迁移，需完成：CDM 迁移作业创建、ODS/STD 数据开发调度任务创建、定时调度启用、全生命周期状态回写 Dbt；当前全流程人工操作，无自动化链路。

4. **配套已有脚本**：`create-kingsoft-prod-all.py` 金山 Dbt 自动建表脚本，负责标准化生成迁移配置 Dbt 模板，本方案复用其全套金山 OpenAPI 签名、鉴权、分页读取能力。

### 1\.2 核心业务目标

搭建**三层自动化链路**：
`金山Dbt多维表（迁移配置清单） → 数据治理平台作业入参工具（中间转换层） → 本市政务DataArts Studio + CDM`
实现全链路自动化：

1. 自动分页拉取金山 Dbt 全量迁移配置记录；

2. 数据治理平台工具完成**字段清洗、类型转换、多场景入参模板组装**（自动建 CDM、单独新建 CDM、手工上报、ODS/STD 数据开发四类入参）；

3. 批量调用华为 DataArts OpenAPI：创建 CDM 迁移作业、创建 Hive ODS/STD 数据开发调度任务、配置定时调度、启动调度；

4. 执行结果、作业 ID、任务状态、三期完成标记**自动回写金山 Dbt 对应记录**；

5. 全链路日志、失败重试、干跑校验、异常诊断复用现有金山脚本容错能力。

### 1\.3 整体架构分层

|层级|组件|核心职责|依赖能力|
|---|---|---|---|
|上层数据源层|金山 Dbt 多维表 \+ \[create\-kingsoft\-prod\-all\.py\]\(create\-kingsoft\-prod\-all\.py\) 底层 API 工具类|读取迁移配置全量记录、执行结果回写 Dbt、KSO\-1 签名鉴权、分页查询记录、字段提取|金山本地网络 API：\[10\.102\.121\.40:5489\]\(10\.102\.121\.40:5489\) /openapi/v7|
|中间转换层|数据治理平台作业入参工具（新增 Dbt 适配模块）|字段标准化清洗、多场景入参模板渲染、参数校验、批量分片、下游 API 请求封装、状态归集|内置格式化规则、四类作业入参模板、DataArts/CDM 签名工具|
|下游执行层|本市政务华为云 DataArts Studio \+ CDM 集群|创建 CDM 迁移同步作业、创建 Hive SQL 调度任务、配置 cron 定时、启动调度、查询作业运行状态|华为云 DataArts OpenAPI、CDM 集群 API|

## 二、核心依赖组件能力梳理

### 2\.1 金山 Dbt OpenAPI 能力（复用已有 Python 脚本全套逻辑）

#### 2\.1\.1 鉴权与通信规则

- 本地网络地址：`<INTERNAL_API_HOST>:5489`

- 鉴权模式：client\_credentials OAuth2 \+ KSO\-1 HMAC\-SHA256 签名

- 全局密钥环境变量可覆盖：`KINGSOFT_CLIENT_ID / KINGSOFT_CLIENT_SECRET`

- 核心接口复用：

    1. `/openapi/oauth2/token`：获取 access\_token 全局复用

    2. `/openapi/v7/doclib/search`：定位迁移配置所在文档库（drive\_id）

    3. `/openapi/v7/files/search`：定位迁移配置\.dbt 文件（file\_id）

    4. `/openapi/v7/coop/dbsheet/{file_id}/schema`：获取 sheet 元数据、字段名称匹配

    5. `/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records/list_by_page`：**分页拉取全部迁移配置记录**（prefer\_id=false，返回展示名字段 key）

    6. `/openapi/v7/coop/dbsheet/{file_id}/sheets/{sheet_id}/records`：批量更新记录（执行结果回写 Dbt）

#### 2\.1\.2 迁移配置 Dbt 固定 60 \+ 业务字段（完整清单）

```Plain Text
序号,是否已删除,二期-中心库账号,二期-部门,二期-系统,三期-标准层工作空间,三期-标准层目录部门,三期-部门代码,三期-系统,三期-规范化系统,三期-系统代码,
ods被删是否处理,上级回流系统代码,上级回流库,手工上报迁移,二期-源表,三期-源表名修正,三期-目录名,规范化-三期-数据开发-表中文名,
三期-hive-ods库,三期-hive-std库,三期-ods表英文名,三期ods-cdm名称,源头是否涉及数据加密,二期-更新频率,最近更新时间,责任人,
二期匹配核对,ODS-CDM是否已建,ODS-数据开发是否已建,目录建议,是否已从中心库迁移数据,三期-是否完成,三期-同步方式,三期-备注,
重要-二期-同步方式（1-增量，0-全量）本列有值的必须按照此列同步方式开发,二期hive-库名,二期ods-表名,重要-二期-调度时间,三期-ods定时,
建表语句,三期-ODS数据开发作业名称,三期-std表英文名,三期-std数据开发作业名称,华为云项目 ID,cdm集群名称,
cdm集群ID(1f48:3bdb7eec-5a76-46f9-a0ee-61cbecf20962)(7551:48883276-b9e1-4cb5-bb3f-3a1ae613fceb),
源连接名称,源数据库 schema 名称,作业分组ID,作业分组名称,部门-系统名称,工作空间id(政务数据局空间),三期-ODS数据开发作业目录,
手工上报-多维表格目录,自动建CDM-数据开发作业入参,单独新建数据开发作业入参,单独新建cdm作业入参--数源为中心oracle库,
手工上报数据开发作业入参,是否二期中已接入,STD_是否增量,STD_是否创建脚本(二期有任务),STD_是否测试脚本,STD_是否创建任务,
STD_是否启动调度,STD_是否验证数据,确定范围（人，法，房）,辅助列,匹配,三期ods表名(省来源),三期ods表名(市来源),std表名,std取ods,
优先级,ods来源,是否使用
```

#### 2\.1\.3 复用工具函数（直接嵌入数据治理平台工具上游模块）

1. `iter_sheet_records_fields`：分页迭代所有记录，返回纯字段字典，自动兼容复杂单元格（单选 / 多选 / JSON 字符串）

2. `_extract_cell_text`：递归提取单元格可读文本，统一清洗空值、特殊字符

3. `_normalize_compare_text`：字段值标准化匹配（用于判断「是否已删除」「三期 \- 是否完成」等标记）

4. 网络重试、临时缓存、批量分片、诊断日志输出、dry\-run 干跑模式完整复用

### 2\.2 数据治理平台作业入参工具改造方案（中间层核心）

#### 2\.2\.1 原有能力

- 支持手动录入入参 JSON、校验参数合法性、生成 DataArts/CDM 标准请求体、调用下游平台接口、任务状态查询。

- 内置四类作业入参模板：

    1. 自动建 CDM \- 数据开发作业入参

    2. 单独新建数据开发作业入参

    3. 单独新建 cdm 作业入参（中心 Oracle 源）

    4. 手工上报数据开发作业入参

#### 2\.2\.2 新增 Dbt 上游适配模块（核心扩展）

1. **Dbt 配置接入模块**

    - 入参：源文档库名、源\.dbt 文件名、源 sheet 名（同`create-kingsoft-prod-all.py`启动参数）

    - 输出：全量清洗后的结构化迁移记录列表（每条记录映射 60 \+ 字段结构化对象，空值统一填充`""`，布尔标记转为`true/false`）

    - 缓存：文档库 drive\_id、文件 file\_id、sheet\_id 内存缓存，避免重复搜索 API 调用

2. **字段映射 \& 清洗模块**

    - 规则：

        - 同步方式：`重要-二期-同步方式`=1 → 增量 \(increment\)；0 → 全量 \(full\)

        - 定时调度：`重要-二期-调度时间` → 转换为 DataArts 标准 cron 表达式

        - 集群映射：`cdm集群名称`/`cdm集群ID`自动绑定 CDM 集群参数

        - 目录路由：`三期-ODS数据开发作业目录`/`手工上报-多维表格目录`自动填充作业目录路径

        - 过滤标记：`是否已删除=是`直接跳过本条记录；`三期-是否完成=是`跳过创建，仅回查状态

3. **多模板自动渲染引擎**
根据记录内标记自动匹配对应入参模板：

    - `手工上报迁移=是` → 手工上报模板

    - `自动建CDM标记` → 自动建 CDM 模板

    - `STD_是否创建任务=是` → STD 数据开发任务模板

    - `源连接名称=中心Oracle` → 单独 CDM Oracle 源模板

4. **批量分片控制器**
分片大小可配置（默认 50 条 / 批），单批串行执行，失败单条重试，不中断整批；

5. **结果归集 \& 回写模块**
收集每条记录执行结果（成功 / 失败、作业 ID、调度状态、错误堆栈），组装批量更新参数，调用金山 Dbt 接口回填对应记录字段：

    - ODS\-CDM 是否已建

    - ODS \- 数据开发是否已建

    - 三期 \- 是否完成

    - 三期 \- 备注（填充失败原因 / 作业 ID）

### 2\.3 下游 DataArts Studio \+ CDM OpenAPI 能力（数据治理平台工具下游模块）

#### 2\.3\.1 DataArts 数据开发核心接口

1. `POST /v1/{project_id}/jobs`：创建 Hive SQL 批处理作业（ODS/STD 建表、同步脚本）

2. `PUT /v1/{project_id}/jobs/{job_id}`：更新作业定时调度 cron 配置

3. `POST /v1/{project_id}/jobs/{job_id}/schedule/start`：启动定时调度

4. `GET /v1/{project_id}/jobs/{job_id}`：查询作业状态（NEW/RUNNING/SUCCEEDED/FAILED）

5. `POST /v1/{project_id}/jobs/{job_id}/run`：立即单次执行作业（可选）

#### 2\.3\.2 CDM 云数据迁移核心接口

1. `POST /cdm/v1.0/{project_id}/clusters/{cluster_id}/cdm/job`：创建 CDM 表迁移同步作业（Oracle→Hive ODS）

2. `GET /cdm/v1.0/{project_id}/clusters/{cluster_id}/cdm/job/{job_name}/status`：查询 CDM 同步任务运行状态

3. `PUT /cdm/v1.0/{project_id}/clusters/{cluster_id}/cdm/job/{job_name}`：更新同步增量 / 全量规则、调度周期

#### 2\.3\.3 入参组装规则（数据治理平台工具模板固化）

1. CDM 迁移作业必填参数来源（全部取自 Dbt 字段）

    - project\_id：华为云项目 ID

    - cluster\_id：cdm 集群 ID

    - name：三期 ods\-cdm 名称

    - from\-connector\-name：源连接名称

    - from\-database/schema：源数据库 schema 名称

    - to\-database：三期 \- hive\-ods 库

    - to\-table：三期 ods 表英文名

    - is\-increment：重要 \- 二期 \- 同步方式（1=true，0=false）

    - schedule\-cron：重要 \- 二期 \- 调度时间

2. DataArts ODS/STD 数据开发作业参数来源

    - job\_name：三期 \- ODS 数据开发作业名称 / 三期 \- std 数据开发作业名称

    - directory：三期 \- ODS 数据开发作业目录

    - sql\_content：建表语句

    - schedule：三期 \- ods 定时（cron）

    - workspace\_id：工作空间 id \(政务数据局空间\)

    - group\_id：作业分组 ID、作业分组名称

## 三、完整业务执行流程（端到端）

### 3\.1 启动入参（兼容原有金山脚本调用格式）

```bash
python longdata_job_param_middle.py \
  "迁移配置文档库名称" "迁移配置dbt文件名" "迁移配置sheet名" \
  --dry-run \          # 干跑：仅拉取Dbt数据、渲染入参，不调用下游DataArts/CDM
  --batch-size 50 \    # 批量分片大小
  --skip-write-dbt     # 执行完成后不回写Dbt（调试用）
```

### 3\.2 阶段 1：初始化金山 Dbt 连接，拉取全量迁移配置

1. 加载环境变量`KINGSOFT_*`鉴权参数，执行`app_authorize`获取全局 access\_token；

2. 三层模糊匹配定位源资源（文档库→dbt 文件→sheet），复用`_pick_best`择优匹配逻辑；

3. 分页迭代`iter_sheet_records`拉取 sheet 全部记录，每条记录执行字段提取清洗；

4. 过滤规则前置：

    - `是否已删除=是`：加入 skipped 列表，跳过下游创建；

    - `三期-是否完成=是`：仅查询下游作业状态，回填状态到 Dbt，不新建；

5. 输出统计：总记录数、待处理有效记录数、已删除跳过数、已完成跳过数；

6. 开启`--dry-run`则直接输出结构化入参 JSON，终止流程。

### 3\.3 阶段 2：数据治理平台工具字段标准化清洗与模板匹配

单条记录处理逻辑：

1. 60 \+ 原始字段统一清洗：空值填充、文本标准化、数字标记转换（同步方式、布尔标记）；

2. 依据记录内业务标记自动匹配四类入参模板之一；

3. 模板渲染：填充 DataArts/CDM 接口所有请求参数，完成参数合法性校验（必填项校验、集群 ID / 项目 ID 格式校验、cron 表达式校验）；

4. 生成标准下游请求体 JSON，存入批量任务队列。

### 3\.4 阶段 3：批量调用华为 DataArts \+ CDM 接口创建作业

按 batch\-size 分片串行执行：

1. 分片内逐条执行：

    1. 调用 CDM 创建 Oracle→Hive ODS 同步迁移作业；

    2. 调用 DataArts 创建 ODS Hive 建表调度任务；

    3. 若 STD 相关标记开启，创建 STD 层 Hive 数据开发任务；

    4. 配置定时 cron 调度，执行启动调度接口；

    5. 轮询查询作业创建状态，记录 job\_id、执行状态；

2. 异常容错：单条接口失败自动重试 3 次，仍失败则标记本条 failed，记录错误堆栈，不阻断分片内其他记录；

3. 归集每条记录执行结果：成功 / 失败、下游作业 ID、调度状态、错误信息。

### 3\.5 阶段 4：执行结果批量回写金山 Dbt 多维表

1. 组装批量更新记录请求体，每条记录更新以下字段：

    - `ODS-CDM是否已建`：是 / 否

    - `ODS-数据开发是否已建`：是 / 否

    - `三期-是否完成`：是（全部任务创建成功）/ 否（存在失败任务）

    - `三期-备注`：填充下游作业 ID、失败原因、CDM 集群信息

2. 调用金山 Dbt 批量更新 records 接口，完成状态回填；

3. 开启`--skip-write-dbt`则跳过本阶段。

### 3\.6 阶段 5：全链路汇总 JSON 输出（标准 stdout，兼容调度平台）

输出顶层结构（复用原有金山脚本输出格式，统一日志规范）：

1. 上游源信息：source\_drive\_id /source\_file\_id/source\_sheet\_id

2. 全局统计：总记录、待处理、跳过删除、跳过已完成、创建成功数、失败数

3. run\_summary 分项统计：CDM 作业新建数量、ODS/STD 数据开发任务新建数量、调度启动成功数

4. 明细数组：

    - created：成功创建记录（下游作业 ID、入参模板类型、CDM 集群）

    - skipped：跳过记录（原因：已删除 / 已完成）

    - failed：失败记录（完整报错、原始 Dbt 记录序号）

5. 全链路错误堆栈捕获，进程强制退出码 0，避免调度平台吞日志。

## 四、数据治理平台作业入参工具模块拆分设计（代码分层）

### 模块 1：kingsoft\_dbt\_client（直接复用 \[create\-kingsoft\-prod\-all\.py\]\(create\-kingsoft\-prod\-all\.py\) 工具类）

- 职责：金山 API 鉴权、资源定位、分页读取记录、批量更新记录、字段提取清洗、诊断调试输出

- 对外暴露类：`KingsoftDbtClient`

- 对外核心方法：

    1. `load_all_migrate_records()`：返回清洗后全量结构化记录列表

    2. `batch_update_records(results)`：批量回写执行结果到 Dbt

### 模块 2：dbt\_param\_convert（新增：Dbt 字段→数据治理平台标准入参转换器）

1. 字段清洗类`DbtFieldCleaner`：处理空值、同步方式转换、cron 转换、集群 ID 映射

2. 模板渲染类`ParamTemplateRenderer`：内置四类作业模板，自动匹配记录业务标记渲染下游请求 JSON

3. 参数校验器`ParamValidator`：校验华为项目 ID、集群 UUID、cron、必填字段完整性

### 模块 3：dataarts\_cdm\_client（数据治理平台原有下游客户端扩展）

1. DataArtsStudioClient：创建作业、更新调度、启动调度、查询状态

2. CdmClient：创建迁移作业、查询同步任务状态

3. 批量执行控制器`BatchTaskExecutor`：分片、重试、结果归集

### 模块 4：main\_entry（程序入口，参数解析、流程编排）

- 解析启动命令行参数（dry\-run、batch\-size、skip\-write\-dbt 等）

- 串联全流程：拉取 Dbt 数据 → 清洗渲染入参 → 批量创建下游任务 → 回写 Dbt → 输出汇总 JSON

- 全局异常捕获，统一格式化错误输出，兼容调度平台日志采集

## 五、容错、重试与调试机制（复用现有金山脚本设计）

1. **网络重试**

    - 金山 API：HTTP 5xx、超时、连接拒绝自动重试；

    - DataArts/CDM 接口：创建失败重试 3 次，间隔 2s 退避；

2. **干跑校验 \-\-dry\-run**
仅拉取 Dbt 数据、渲染全部下游入参 JSON，不调用任何写接口，用于上线前参数校验、字段映射核对；

3. **分层诊断日志**

    - 无匹配记录、字段值不匹配时自动输出字段样本；

    - 失败记录完整保存原始 Dbt 行号、60 \+ 原始字段、渲染后的入参、HTTP 响应报错；

4. **幂等保障**

    - 下游创建前查询同名作业是否存在，存在则跳过创建，仅更新调度配置；

    - Dbt 回填按 record\_id 精准更新，不会覆盖其他业务字段；

5. **环境变量全局开关**

    ```Plain Text
    KINGSOFT_API_HOST/KINGSOFT_CLIENT_ID：金山接口配置
    DATAARTS_ENDPOINT/DATAARTS_PROJECT_ID：华为DataArts配置
    CDM_ENDPOINT/CDM_DEFAULT_CLUSTER_ID：CDM集群默认配置
    KINGSOFT_CLEAR_SHEET_RECORDS_AFTER_CREATE：复用原有清理开关
    ```

## 六、上下游接口数据流转示例（单条记录简化）

### 输入：金山 Dbt 单条结构化清洗记录（核心关键字段节选）

```json
{
  "record_id": "rec_xxxx",
  "三期-hive-ods库": "ods_lgzw",
  "三期ods表英文名": "t_person_info",
  "三期ods-cdm名称": "oracle2hive_person_cdm",
  "源连接名称": "center_oracle_conn",
  "源数据库 schema 名称": "lg_center",
  "重要-二期-同步方式": "1",
  "重要-二期-调度时间": "0 30 2 * * ?",
  "华为云项目 ID": "1f48xxxx",
  "cdm集群ID": "3bdb7eec-5a76-46f9-a0ee-61cbecf20962",
  "三期-ODS数据开发作业名称": "ods_t_person_info_hive_load",
  "三期-ODS数据开发作业目录": "/政务数据/人社/ODS层",
  "建表语句": "CREATE TABLE ods_lgzw.t_person_info (...)",
  "手工上报迁移": "否",
  "是否已删除": "否",
  "三期-是否完成": "否"
}
```

### 中间层数据治理平台工具渲染输出：CDM 创建作业请求体（节选）

```json
{
  "name": "oracle2hive_person_cdm",
  "from-connector-name": "center_oracle_conn",
  "to-connector-name": "hive_ods_conn",
  "driver-config-values": {
    "configs": [
      {"name": "isIncremental", "value": "true"},
      {"name": "sourceSchema", "value": "lg_center"},
      {"name": "targetDatabase", "value": "ods_lgzw"},
      {"name": "targetTable", "value": "t_person_info"}
    ]
  },
  "schedule": {"cron": "0 30 2 * * ?"}
}
```

### 下游执行完成后，回写 Dbt 更新字段值

|字段|更新值|
|---|---|
|ODS\-CDM 是否已建|是|
|ODS \- 数据开发是否已建|是|
|三期 \- 是否完成|是|
|三期 \- 备注|CDM 作业：oracle2hive\_person\_cdm；ODS 任务：ods\_t\_person\_info\_hive\_load；集群 ID：3bdb7eec\-5a76\-46f9\-a0ee\-61cbecf20962|

## 七、风险与约束说明

### 7\.1 约束限制

1. 金山 Dbt 迁移配置 sheet**字段名固定不可修改**，字段变更需同步更新数据治理平台工具`DbtFieldCleaner`映射规则；

2. 华为云 DataArts、CDM 接口依赖本地网络连通，执行服务器需同时打通金山本地网络（\[10\.102\.121\.40:5489\]\(10\.102\.121\.40:5489\)）与华为云本地网络；

3. 同步方式仅支持 0/1 标记，Dbt 单元格不可存在其他文本值；

4. 单批次最大处理条数建议≤100，避免接口限流；

5. 建表语句字段过长会导致 DataArts 创建作业失败，需在 Dbt 模板中限制长度。

### 7\.2 潜在风险与规避方案

1. **金山 access\_token 过期**
规避：数据治理平台工具增加 token 过期捕获，自动重新执行鉴权刷新；

2. **下游平台同名作业冲突**
规避：创建前先查询作业是否存在，存在则跳过创建，更新调度配置；

3. **批量回写 Dbt 接口超限**
规避：回写分片（每 20 条一批），间隔 1s 调用；

4. **Dbt 单元格复杂格式解析失败**
规避：复用`_extract_cell_text`递归解析，诊断日志输出原始单元格值便于修正配置表；

5. **CDM 集群 ID 与名称不匹配**
规避：数据治理平台工具内置集群 ID \- 名称映射白名单，非法集群直接标记失败。

## 八、交付物清单

1. 本设计文档（V1\.0）

2. 改造后完整代码：

    - 复用`create-kingsoft-prod-all.py`金山 API 工具类封装为独立模块`kingsoft_dbt_client.py`

    - 新增`dbt_param_convert.py`字段清洗与模板渲染模块

    - 扩展数据治理平台原有`dataarts_cdm_client.py`下游接口客户端

    - 主程序入口`longdata_job_param_middle.py`

3. 环境变量配置说明文档

4. 干跑、全量执行、调试三种场景启动脚本示例

5. 输出 JSON 日志字段说明手册

## 九、待评审确认项（检查后反馈调整）

1. 60 \+ 业务字段映射关系是否完整、无遗漏；

2. 四类作业入参模板业务匹配逻辑是否符合迁移规范；

3. CDM 增量 / 全量、cron 调度转换规则是否匹配华为平台要求；

4. Dbt 回写字段、回填内容是否满足三期全生命周期台账管理要求；

5. 批量分片大小、重试次数、超时时间等参数是否需要调整；

6. 是否需要新增告警能力（失败记录输出告警 JSON / 对接调度告警）；

7. 是否增加导出迁移执行明细 CSV 能力。



> （注：部分内容可能由 AI 生成）
