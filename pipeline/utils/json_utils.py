from __future__ import annotations

import json
import re

from config import TASK_TYPES


def parse_model_json(content: str) -> dict:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start < 0 or end <= start:
            raise
        return json.loads(cleaned[start : end + 1])


def normalize_task_result(parsed: dict) -> dict:
    normalized_scores = _normalize_scores(parsed.get("task_scores"), TASK_TYPES)
    best_task_type = _normalize_best_label(
        parsed.get("best_task_type"), normalized_scores, "其它异常"
    )

    evidence = parsed.get("task_evidence") or {}
    return {
        "task_scores": normalized_scores,
        "best_task_type": best_task_type,
        "task_evidence": {
            task_type: str(evidence.get(task_type, "")) for task_type in TASK_TYPES
        },
        "description": parsed.get("description") or {},
    }


def normalize_anomaly_result(parsed: dict, anomaly_types: list[str]) -> dict:
    normalized_scores = _normalize_scores(
        parsed.get("anomaly_scores"), anomaly_types
    )
    best_anomaly_type = _normalize_best_label(
        parsed.get("best_anomaly_type"),
        normalized_scores,
        "其它任务类型" if "其它任务类型" in anomaly_types else "其它异常",
    )
    evidence = parsed.get("anomaly_evidence") or {}
    return {
        "anomaly_scores": normalized_scores,
        "best_anomaly_type": best_anomaly_type,
        "anomaly_evidence": {
            anomaly_type: str(evidence.get(anomaly_type, ""))
            for anomaly_type in anomaly_types
        },
    }


def _normalize_scores(raw_scores: object, labels: list[str]) -> dict[str, float]:
    scores = raw_scores if isinstance(raw_scores, dict) else {}
    normalized_scores = {}
    for label in labels:
        try:
            score = float(scores.get(label, 0.0))
        except (TypeError, ValueError):
            score = 0.0
        normalized_scores[label] = max(0.0, min(1.0, score))
    return normalized_scores


def _normalize_best_label(
    declared_label: object,
    normalized_scores: dict[str, float],
    fallback_label: str,
) -> str:
    highest_score = max(normalized_scores.values())
    highest_labels = [
        label for label, score in normalized_scores.items() if score == highest_score
    ]
    if declared_label in highest_labels:
        return str(declared_label)
    if fallback_label in highest_labels:
        return fallback_label
    return highest_labels[0]
