# 数据集成作业自动化工具链（data-integration-toolchain）

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org)
[![Node](https://img.shields.io/badge/Node.js-%3E%3D18-green)](https://nodejs.org)
[![License](https://img.shields.io/badge/license-MIT-blue)](#license)

> **开源脱敏版。** 原项目为某政务数据治理平台的「数据集成作业自动化工具链」，覆盖多维表解析、数源参数中间层、CDM 作业创建、开发任务创建与政务云入库脚本。本仓库已移除所有真实凭据、内部域名、IP、业务系统名与运行产物，仅保留通用代码逻辑。

一套面向数据集成场景的自动化脚本集合，按数据流串成工具链：

1. **二维数组工具** — 将云表格 / 文本参数转换为标准二维数组，并合并执行 CDM 作业 + 开发任务创建。
2. **longdata_middle（数源参数中间层）** — 将 DBT 模型参数转换为平台作业入参；封装 DataArts CDM 客户端与政务云客户端。
3. **design_md** — 整体架构与接口调研设计文档。
4. **DataArts_Studio** — V1/V2 版 CDM 作业、开发任务批量创建脚本。
5. **KingSoft** — 政务云多维表数据入库与表结构迁移生产脚本。

## 技术栈

- Python 3.8+（主要逻辑）
- Node.js >= 18（部分中间件）
- 依赖：`openpyxl`、`requests` 等（具体见各脚本 import）

## 目录结构

```
data-integration-toolchain/
├── 二维数组工具/
│   ├── huaweiyuntool.py              # CDM作业 + 开发任务创建合并脚本
│   └── 龙数作业入参处理工具.py        # 作业入参处理
├── longdata_middle/                  # 数源参数中间层
│   ├── main.py
│   ├── longdata_job_param_middle.py  # 作业入参中间层
│   ├── dbt_param_convert.py          # DBT 参数转换
│   ├── dataarts_cdm_client.py        # CDM 客户端
│   ├── kingsoft_dbt_client.py        # 政务云客户端
│   ├── _compat.py
│   └── pyproject.toml
├── design_md/                        # 设计文档
│   ├── 华为云DataArts_Studio_API调研.md
│   └── 整体设计文档.md
├── DataArts_Studio/                  # CDM 作业 / 开发任务脚本
│   ├── V1-CDM作业_开发作业.py
│   ├── V1-创建开发任务.py
│   ├── V2-CDM作业_开发作业_数据库直连获取源字段.py
│   ├── V2-创建CDM任务-数据库直连获取源字段.py
│   └── V2-创建CDM作业_调整参数顺序_数源参数.py
└── KingSoft/                         # 政务云入库脚本
    ├── create-kingsoft-data-insert-hive-jods-prod.py
    ├── create-kingsoft-prod-all.py
    ├── hive-comment-update-from-mysql.py
    ├── lgbs-data-insert-kingsoft-prod-all.py
    └── lgbs_table_structure_move_department_prod.py
```

## 用法

各脚本独立运行，参数通过命令行位置参数或配置文件传入。以 CDM 作业创建为例：

```bash
python DataArts_Studio/V2-CDM作业_开发作业_数据库直连获取源字段.py <参数...>
python 二维数组工具/huaweiyuntool.py <22个位置参数>
```

> 具体参数顺序见各脚本头部注释。原实现中的真实 `project_id`、`cluster_id`、业务系统名、内网地址均已替换为占位符（如 `<YOUR_PROJECT_ID>`、`<示例业务系统>`）。

## 说明

- 本仓库**不含**任何真实表结构、凭据或运行产物（`.env`、`.xlsx`、`.exe`、`build/`、`dist/` 均已排除）。
- 连接外部平台的部分需你自备可达网络与授权凭据；切勿将真实凭据提交入库。

## License


本项目基于 [MIT License](./LICENSE) 开源，可自由使用、修改和分发。欢迎按需二次开发。
