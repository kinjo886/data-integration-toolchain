# -*- coding: utf-8 -*-
import json
import re

# 四类作业模板匹配标识
TEMPLATE_TAG_MANUAL = "手工上报迁移"
TEMPLATE_TAG_CDM_AUTO = "自动建CDM-数据开发作业入参"
TEMPLATE_TAG_STD_TASK = "STD_是否创建任务"
TEMPLATE_TAG_CENTER_ORACLE = "源连接名称"

# Dbt回填字段
WRITE_BACK_FIELDS = [
    "ODS-CDM是否已建",
    "ODS-数据开发是否已建",
    "三期-是否完成",
    "三期-备注"
]

class DbtFieldCleaner:
    @staticmethod
    def clean_all_fields(raw_row):
        clean = {}
        for k, v in raw_row.items():
            clean[k] = str(v).strip() if v is not None else ""
        # 同步方式转换
        sync_raw = clean.get("重要-二期-同步方式（1-增量，0-全量）本列有值的必须按照此列同步方式开发", "")
        clean["_is_increment"] = True if sync_raw == "1" else False
        # cron校验
        cron = clean.get("重要-二期-调度时间", "")
        clean["_cron_expr"] = cron
        # 集群ID映射
        cdm_id_str = clean.get("cdm集群ID(1f48:3bdb7eec-5a76-46f9-a0ee-61cbecf20962)(7551:48883276-b9e1-4cb5-bb3f-3a1ae613fceb)", "")
        clean["_cdm_cluster_id"] = cdm_id_str
        clean["_record_id"] = raw_row.get("record_id")
        clean["_skip_delete"] = raw_row.get("_skip_delete")
        clean["_skip_finish"] = raw_row.get("_skip_finish")
        return clean

    @staticmethod
    def build_write_back_payload(record_id, cdm_ok, ods_ok, all_finish, remark):
        return {
            "record_id": record_id,
            "fields": {
                "ODS-CDM是否已建": "是" if cdm_ok else "否",
                "ODS-数据开发是否已建": "是" if ods_ok else "否",
                "三期-是否完成": "是" if all_finish else "否",
                "三期-备注": remark
            }
        }

class ParamValidator:
    @staticmethod
    def check_cron(cron):
        if not cron:
            return False, "调度cron为空"
        return True, ""

    @staticmethod
    def check_uuid(uuid_str):
        pattern = r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$"
        if re.match(pattern, uuid_str):
            return True, ""
        return False, f"集群/项目ID格式非法：{uuid_str}"

    @staticmethod
    def check_required(clean_row):
        req = [
            ("华为云项目 ID", "华为云项目ID为空"),
            ("_cdm_cluster_id", "CDM集群ID为空"),
            ("三期-hive-ods库", "ODS库名缺失"),
            ("三期ods表英文名", "ODS表英文名缺失"),
            ("源连接名称", "源连接名称缺失"),
            ("源数据库 schema 名称", "源schema缺失"),
            ("三期-ODS数据开发作业名称", "ODS作业名称缺失"),
            ("建表语句", "建表语句为空")
        ]
        msg_list = []
        for k, msg in req:
            if not clean_row.get(k, "") and not clean_row.get(k.replace("_","-"), ""):
                msg_list.append(msg)
        if msg_list:
            return False, ";".join(msg_list)
        # 校验集群UUID
        ok, m = ParamValidator.check_uuid(clean_row["_cdm_cluster_id"])
        if not ok:
            msg_list.append(m)
        ok_cron, m_cron = ParamValidator.check_cron(clean_row["_cron_expr"])
        if not ok_cron:
            msg_list.append(m_cron)
        return len(msg_list) == 0, ";".join(msg_list)

class ParamTemplateRenderer:
    @staticmethod
    def match_template_type(clean_row):
        if clean_row.get(TEMPLATE_TAG_MANUAL, "") == "是":
            return "manual_report"
        if clean_row.get(TEMPLATE_TAG_STD_TASK, "") == "是":
            return "std_dev_task"
        if "oracle" in clean_row.get(TEMPLATE_TAG_CENTER_ORACLE, "").lower():
            return "cdm_center_oracle"
        return "auto_cdm_dev"

    @staticmethod
    def render_cdm_body(clean_row):
        return {
            "name": clean_row.get("三期ods-cdm名称"),
            "from-connector-name": clean_row.get("源连接名称"),
            "to-connector-name": "hive_ods_conn",
            "driver-config-values": {
                "configs": [
                    {"name": "isIncremental", "value": clean_row["_is_increment"]},
                    {"name": "sourceSchema", "value": clean_row.get("源数据库 schema 名称")},
                    {"name": "targetDatabase", "value": clean_row.get("三期-hive-ods库")},
                    {"name": "targetTable", "value": clean_row.get("三期ods表英文名")}
                ]
            },
            "schedule": {"cron": clean_row["_cron_expr"]}
        }

    @staticmethod
    def render_dataarts_ods_body(clean_row):
        return {
            "job_name": clean_row.get("三期-ODS数据开发作业名称"),
            "directory": clean_row.get("三期-ODS数据开发作业目录"),
            "sql_content": clean_row.get("建表语句"),
            "schedule_cron": clean_row["_cron_expr"],
            "workspace_id": clean_row.get("工作空间id(政务数据局空间)"),
            "group_id": clean_row.get("作业分组ID")
        }

    @staticmethod
    def render_std_body(clean_row):
        return {
            "job_name": clean_row.get("三期-std数据开发作业名称"),
            "target_db": clean_row.get("三期-hive-std库"),
            "std_table": clean_row.get("std表名"),
            "source_ods_table": clean_row.get("三期ods表英文名"),
            "cron": clean_row["_cron_expr"]
        }