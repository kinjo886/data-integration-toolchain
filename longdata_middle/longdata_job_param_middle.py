# -*- coding: utf-8 -*-
import sys
import json
from dotenv import load_dotenv
load_dotenv()

from kingsoft_dbt_client import KingsoftDbtClient
from dbt_param_convert import DbtFieldCleaner

def print_json(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))

def main():
    argv = sys.argv
    dry_run = "--dry-run" in argv
    skip_write_dbt = "--skip-write-dbt" in argv
    batch_size = 50
    if "--batch-size" in argv:
        idx = argv.index("--batch-size")
        batch_size = int(argv[idx+1])
    args = [x for x in argv[1:] if x not in ("--dry-run", "--skip-write-dbt", "--batch-size") and not x.isdigit()]
    if len(args) != 3:
        print("用法：python longdata_job_param_middle.py 文档库名 dbt文件名 sheet名 [--dry-run] [--skip-write-dbt] [--batch-size 50]")
        sys.exit(0)
    doclib, dbt_file, sheet = args[0], args[1], args[2]
    # 1. 拉取Dbt全量数据
    print("===== 初始化金山Dbt客户端，拉取迁移配置 =====")
    dbt_client = KingsoftDbtClient()
    source_info = dbt_client.load_all_migrate_records(doclib, dbt_file, sheet)
    raw_records = source_info["records"]
    print(f"读取总行数：{len(raw_records)}")
    # 2. 字段清洗
    clean_list = []
    for row in raw_records:
        clean_row = DbtFieldCleaner.clean_all_fields(row)
        clean_list.append(clean_row)
    # 干跑模式
    if dry_run:
        print_json({
            "dry_run": True,
            "source_info": source_info,
            "clean_records_sample": clean_list[:10],
            "summary": f"共{len(clean_list)}条记录，仅渲染参数，不调用下游创建接口"
        })
        sys.exit(0)
    # 3. 批量执行DataArts/CDM创建
    from dataarts_cdm_client import BatchTaskExecutor
    executor = BatchTaskExecutor(batch_size=batch_size)
    exec_result = executor.run_batch(clean_list)
    # 4. 批量回写Dbt
    update_payload = []
    for suc in exec_result["success"]:
        payload = DbtFieldCleaner.build_write_back_payload(
            suc["record_id"], True, True, True, suc["remark"]
        )
        update_payload.append(payload)
    for fail in exec_result["fail"]:
        payload = DbtFieldCleaner.build_write_back_payload(
            fail["record_id"], False, False, False, f"创建失败：{fail['reason']}"
        )
        update_payload.append(payload)
    source_file_id = source_info["source_file_id"]
    source_sheet_id = source_info["source_sheet_id"]
    if not skip_write_dbt and update_payload:
        print("===== 开始批量回写金山多维表状态 =====")
        dbt_client.batch_update_records(source_file_id, source_sheet_id, update_payload)
    # 5. 输出汇总日志
    total = len(clean_list)
    suc_cnt = len(exec_result["success"])
    skip_cnt = len(exec_result["skip"])
    fail_cnt = len(exec_result["fail"])
    out_summary = {
        "source": source_info,
        "total_records": total,
        "run_stat": {
            "success_count": suc_cnt,
            "skip_count": skip_cnt,
            "fail_count": fail_cnt
        },
        "success_detail": exec_result["success"],
        "skip_detail": exec_result["skip"],
        "fail_detail": exec_result["fail"]
    }
    print_json(out_summary)
    if fail_cnt > 0:
        sys.exit(2)
    sys.exit(0)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        err_info = {
            "global_error": True,
            "error_msg": str(e),
            "traceback": traceback.format_exc()
        }
        print_json(err_info)
        sys.exit(0)