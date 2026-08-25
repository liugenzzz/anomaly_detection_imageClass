from __future__ import annotations

from pathlib import Path

from config import ANOMALY_TYPES_BY_TASK, TASK_ORGANIZATION_CONFIG, TASK_TYPES


def resolve_organization_labels(
    task_type: str | None,
    anomaly_type: str | None,
) -> tuple[str, str]:
    """Apply the same one/two-level label fallback rules organizer.py has
    always used, so classify's inline manifest writer and the standalone
    organize stage agree on where a record belongs.
    """
    if task_type not in TASK_TYPES:
        task_type = TASK_ORGANIZATION_CONFIG["unclassified_dir"]

    if task_type == TASK_ORGANIZATION_CONFIG["unclassified_dir"]:
        resolved_anomaly_type = TASK_ORGANIZATION_CONFIG["unclassified_dir"]
    elif task_type == "其它异常":
        resolved_anomaly_type = "其它异常"
    else:
        resolved_anomaly_type = anomaly_type
        if resolved_anomaly_type not in ANOMALY_TYPES_BY_TASK[task_type]:
            resolved_anomaly_type = "其它任务类型"

    return task_type, resolved_anomaly_type


def build_destination_path(
    output_dir: Path,
    task_type: str,
    anomaly_type: str,
    relative_path: Path,
) -> Path:
    if task_type in {"其它异常", TASK_ORGANIZATION_CONFIG["unclassified_dir"]}:
        return output_dir / task_type / relative_path
    return output_dir / task_type / anomaly_type / relative_path
