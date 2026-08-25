from __future__ import annotations

import csv
import json
import shutil
from collections import Counter
from pathlib import Path

from config import ANOMALY_TYPES_BY_TASK, TASK_ORGANIZATION_CONFIG, TASK_TYPES
from pipeline.core.logging_utils import setup_stage_logger, suppress_console_progress_lines
from pipeline.core.progress import ProgressBar


def organize_by_task_type(
    input_dir: Path | None = None,
    results_file: Path | None = None,
    output_dir: Path | None = None,
) -> dict:
    input_dir = input_dir or TASK_ORGANIZATION_CONFIG["input_dir"]
    results_file = results_file or TASK_ORGANIZATION_CONFIG["classification_results_file"]
    output_dir = output_dir or TASK_ORGANIZATION_CONFIG["output_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_stage_logger(TASK_ORGANIZATION_CONFIG["stage_name"], output_dir)
    manifest_file = TASK_ORGANIZATION_CONFIG["manifest_file"]
    summary_file = TASK_ORGANIZATION_CONFIG["summary_file"]
    if output_dir != TASK_ORGANIZATION_CONFIG["output_dir"]:
        manifest_file = output_dir / "manifest.jsonl"
        summary_file = output_dir / "summary.json"

    counters = Counter()
    logger.info(
        "organization started input_dir=%s results_file=%s output_dir=%s "
        "copy_files=%s materialize_files=%s",
        input_dir,
        results_file,
        output_dir,
        TASK_ORGANIZATION_CONFIG["copy_files"],
        TASK_ORGANIZATION_CONFIG["materialize_files"],
    )

    total_records = _count_records(results_file)
    progress_bar = ProgressBar(
        total=total_records,
        desc="整理进度",
        mode=TASK_ORGANIZATION_CONFIG["progress_bar"],
    )
    if progress_bar.active:
        suppress_console_progress_lines(logger)

    with manifest_file.open("w", encoding="utf-8") as manifest:
        for record in _iter_records(results_file):
            counters["seen"] += 1
            item = _organize_one_record(input_dir, output_dir, record)
            if item["action"] == "missing_source":
                counters["missing_source"] += 1
                logger.warning("missing source file: %s", item["source"])
            else:
                counters["organized"] += 1
                counters[item["best_task_type"]] += 1
                counters[
                    f"secondary::{item['best_task_type']}::{item['best_anomaly_type']}"
                ] += 1
            manifest.write(json.dumps(item, ensure_ascii=False) + "\n")
            progress_bar.update(1, 缺失源文件=counters["missing_source"])

            if counters["seen"] % TASK_ORGANIZATION_CONFIG["progress_log_interval"] == 0:
                manifest.flush()
                logger.info(
                    "progress seen=%s organized=%s missing_source=%s",
                    counters["seen"],
                    counters["organized"],
                    counters["missing_source"],
                )
    progress_bar.close()

    summary = {
        "stage": TASK_ORGANIZATION_CONFIG["stage_name"],
        "total_records": counters["seen"],
        "total_files": counters["organized"],
        "missing_source": counters["missing_source"],
        "copy_files": TASK_ORGANIZATION_CONFIG["copy_files"],
        "materialize_files": TASK_ORGANIZATION_CONFIG["materialize_files"],
        "label_counts": {
            task_type: counters[task_type]
            for task_type in TASK_TYPES
            if counters[task_type]
        },
        "anomaly_counts": {
            task_type: {
                anomaly_type: counters[
                    f"secondary::{task_type}::{anomaly_type}"
                ]
                for anomaly_type in ANOMALY_TYPES_BY_TASK[task_type]
                if counters[f"secondary::{task_type}::{anomaly_type}"]
            }
            for task_type in TASK_TYPES
            if any(
                counters[f"secondary::{task_type}::{anomaly_type}"]
                for anomaly_type in ANOMALY_TYPES_BY_TASK[task_type]
            )
        },
        "unclassified_count": counters[TASK_ORGANIZATION_CONFIG["unclassified_dir"]],
    }
    if TASK_ORGANIZATION_CONFIG["write_manifest"]:
        summary_file.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    logger.info("organization finished summary=%s", summary)
    return summary


def _organize_one_record(input_dir: Path, output_dir: Path, record: dict) -> dict:
    task_type = record.get("final", {}).get("best_task_type")
    if task_type not in TASK_TYPES:
        task_type = TASK_ORGANIZATION_CONFIG["unclassified_dir"]

    if task_type == TASK_ORGANIZATION_CONFIG["unclassified_dir"]:
        anomaly_type = TASK_ORGANIZATION_CONFIG["unclassified_dir"]
    elif task_type == "其它异常":
        anomaly_type = "其它异常"
    else:
        anomaly_type = record.get("final", {}).get("best_anomaly_type")
        if anomaly_type not in ANOMALY_TYPES_BY_TASK[task_type]:
            anomaly_type = "其它任务类型"

    relative_path = Path(record["relative_path"])
    source = input_dir / relative_path
    if task_type in {"其它异常", TASK_ORGANIZATION_CONFIG["unclassified_dir"]}:
        destination = output_dir / task_type / relative_path
    else:
        destination = output_dir / task_type / anomaly_type / relative_path
    item = {
        "source": str(source),
        "destination": str(destination),
        "relative_path": record["relative_path"],
        "best_task_type": task_type,
        "best_anomaly_type": anomaly_type,
        "decision_status": record.get("final", {}).get("decision_status"),
    }
    if not source.exists():
        item["action"] = "missing_source"
        return item

    if not TASK_ORGANIZATION_CONFIG["materialize_files"]:
        # Manifest-only mode: record the source/destination mapping without
        # touching any file on disk. manifest.jsonl is the mapping table;
        # downstream consumers resolve the original file via relative_path.
        item["action"] = "manifest_only"
        return item

    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not TASK_ORGANIZATION_CONFIG["overwrite"]:
        raise FileExistsError(destination)
    if TASK_ORGANIZATION_CONFIG["copy_files"]:
        shutil.copy2(source, destination)
        item["action"] = "copied"
    else:
        shutil.move(str(source), str(destination))
        item["action"] = "moved"
    return item


def _count_records(results_file: Path) -> int:
    """Cheap line count for the progress bar's total (no field parsing)."""
    if not results_file.exists():
        return 0
    if results_file.suffix.lower() == ".csv":
        with results_file.open("r", encoding="utf-8-sig", newline="") as file:
            total_lines = sum(1 for _ in file)
        return max(total_lines - 1, 0)  # exclude header row
    with results_file.open("r", encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


def _iter_records(results_file: Path):
    if results_file.suffix.lower() == ".csv":
        with results_file.open("r", encoding="utf-8-sig", newline="") as file:
            for row in csv.DictReader(file):
                yield {
                    "relative_path": row["relative_path"],
                    "final": {
                        "best_task_type": row.get("best_task_type"),
                        "best_anomaly_type": row.get("best_anomaly_type"),
                        "decision_status": row.get("decision_status"),
                    },
                }
        return

    # 兼容读取历史 JSONL，但新版分类阶段不再生成该文件。
    with results_file.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line:
                yield json.loads(line)
