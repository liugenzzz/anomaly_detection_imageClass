from __future__ import annotations

import csv
import json
import logging
import threading
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

from config import (
    ALL_ANOMALY_TYPES,
    ANOMALY_TYPES_BY_TASK,
    CLASSIFICATION_SCHEMA_VERSION,
    DATA_CONFIG,
    PROVIDERS,
    TASK_CLASSIFICATION_CONFIG,
    TASK_TYPES,
)
from pipeline.core.checkpoint import CheckpointStore
from pipeline.core.image_io import iter_image_paths, read_image_payload
from pipeline.core.logging_utils import setup_stage_logger, suppress_console_progress_lines
from pipeline.core.model_client import call_chat_completion
from pipeline.core.progress import ProgressBar
from pipeline.fusion.unanimous import unanimous_vote
from pipeline.stages.task_type.prompts import (
    build_level1_messages,
    build_level2_messages,
)
from pipeline.utils.json_utils import (
    normalize_anomaly_result,
    normalize_task_result,
    parse_model_json,
)


CSV_FIELDNAMES = [
    "classification_schema_version",
    "image_id",
    "sha256",
    "relative_path",
    "mime_type",
    "size_bytes",
    "best_task_type",
    "best_anomaly_type",
    "decision_status",
    "decision_reason",
    "valid_provider_count",
    "level1_expected_provider_count",
    "level1_valid_provider_count",
    "level1_sufficient_provider_count",
    "level1_unanimous",
    "level1_score_threshold",
    "level1_threshold_passed",
    "level1_provider_votes_json",
    "level1_provider_scores_json",
    "level1_provider_errors_json",
    "level2_unanimous",
    "level2_expected_provider_count",
    "level2_valid_provider_count",
    "level2_sufficient_provider_count",
    "level2_score_threshold",
    "level2_threshold_passed",
    "level2_provider_votes_json",
    "level2_provider_scores_json",
    "level2_provider_errors_json",
    *TASK_TYPES,
    *[f"二级_{anomaly_type}" for anomaly_type in ALL_ANOMALY_TYPES],
]


class ProviderRuntime:
    def __init__(self, providers: list[dict], logger: logging.Logger):
        self.logger = logger
        self.failure_threshold = TASK_CLASSIFICATION_CONFIG[
            "disable_provider_after_consecutive_failures"
        ]
        self.reenable_cooldown_seconds = TASK_CLASSIFICATION_CONFIG[
            "provider_reenable_cooldown_seconds"
        ]
        self.semaphores = {
            provider["name"]: threading.BoundedSemaphore(
                max(1, int(provider.get("max_concurrency", 1)))
            )
            for provider in providers
        }
        self.consecutive_failures = {provider["name"]: 0 for provider in providers}
        # provider_name -> monotonic timestamp of the most recent disable event.
        # A provider stays disabled only until the cooldown elapses, at which
        # point one probe request is let through (a half-open retry) instead
        # of the provider being permanently excluded from voting for the rest
        # of the run.
        self.disabled_at: dict[str, float] = {}
        self.lock = threading.Lock()

    def is_disabled(self, provider_name: str) -> bool:
        with self.lock:
            disabled_at = self.disabled_at.get(provider_name)
            if disabled_at is None:
                return False
            if time.monotonic() - disabled_at < self.reenable_cooldown_seconds:
                return True
            # Cooldown elapsed: let one probe request through. If it fails
            # again, register_failure immediately re-disables and resets the
            # cooldown timer below.
            self.logger.info(
                "provider cooldown elapsed, probing for recovery: %s",
                provider_name,
            )
            return False

    def register_success(self, provider_name: str) -> None:
        with self.lock:
            self.consecutive_failures[provider_name] = 0
            if self.disabled_at.pop(provider_name, None) is not None:
                self.logger.warning(
                    "provider recovered and re-enabled: %s", provider_name
                )

    def register_failure(self, provider_name: str, error: str) -> None:
        with self.lock:
            failures = self.consecutive_failures.get(provider_name, 0) + 1
            self.consecutive_failures[provider_name] = failures
            if failures >= self.failure_threshold:
                was_disabled = provider_name in self.disabled_at
                self.disabled_at[provider_name] = time.monotonic()
                if not was_disabled:
                    self.logger.warning(
                        "provider disabled after %s consecutive failures: %s; "
                        "last_error=%s; will retry after %ss",
                        failures,
                        provider_name,
                        error,
                        self.reenable_cooldown_seconds,
                    )

    def semaphore_for(self, provider_name: str) -> threading.BoundedSemaphore:
        return self.semaphores[provider_name]


def classify_dataset(
    input_dir: Path | None = None,
    output_dir: Path | None = None,
    providers: list[dict] | None = None,
    dry_run: bool = False,
    resume: bool | None = None,
) -> dict:
    input_dir = input_dir or DATA_CONFIG["input_dir"]
    output_dir = output_dir or TASK_CLASSIFICATION_CONFIG["output_dir"]
    providers = providers or PROVIDERS
    resume = TASK_CLASSIFICATION_CONFIG["resume"] if resume is None else resume
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_stage_logger(TASK_CLASSIFICATION_CONFIG["stage_name"], output_dir)
    csv_path = output_dir / "results.csv"
    summary_path = output_dir / "summary.json"
    checkpoint_path = TASK_CLASSIFICATION_CONFIG["checkpoint_file"]
    if output_dir != TASK_CLASSIFICATION_CONFIG["output_dir"]:
        checkpoint_path = output_dir / "checkpoint.sqlite3"

    if resume and csv_path.exists():
        _validate_resume_csv(csv_path)
    if not resume:
        _reset_outputs(csv_path, summary_path, checkpoint_path)

    checkpoint_existed = checkpoint_path.exists()
    checkpoint = CheckpointStore(checkpoint_path)
    if resume and csv_path.exists() and not checkpoint_existed:
        restored = _bootstrap_checkpoint_from_csv(csv_path, checkpoint)
        logger.info("checkpoint restored from results_csv restored=%s", restored)
    provider_runtime = ProviderRuntime(providers, logger)
    counters = Counter()
    futures = set()
    max_workers = max(1, int(TASK_CLASSIFICATION_CONFIG["image_max_workers"]))

    # iter_image_paths already sorts (fully materializes) the glob result, so
    # this costs nothing extra beyond what the loop below did anyway, and it
    # gives the progress bar a real total up front.
    image_paths = list(iter_image_paths(input_dir, DATA_CONFIG["recursive"]))

    logger.info(
        "classification started input_dir=%s output_dir=%s dry_run=%s resume=%s providers=%s image_workers=%s total_images=%s",
        input_dir,
        output_dir,
        dry_run,
        resume,
        [provider["name"] for provider in providers],
        max_workers,
        len(image_paths),
    )

    progress_bar = ProgressBar(
        total=len(image_paths),
        desc="分类进度",
        mode=TASK_CLASSIFICATION_CONFIG["progress_bar"],
    )
    if progress_bar.active:
        suppress_console_progress_lines(logger)

    try:
        csv_exists = resume and csv_path.exists() and csv_path.stat().st_size > 0
        mode = "a" if csv_exists else "w"
        with csv_path.open(mode, encoding="utf-8-sig", newline="") as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDNAMES)
            if not csv_exists:
                csv_writer.writeheader()
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for path in image_paths:
                    relative_path = str(path.relative_to(input_dir))
                    counters["seen"] += 1
                    if resume and checkpoint.is_done(relative_path):
                        counters["skipped_checkpoint"] += 1
                        progress_bar.update(1, 跳过=counters["skipped_checkpoint"], 失败=counters["failed"])
                        continue

                    futures.add(
                        executor.submit(
                            classify_one_image,
                            path,
                            input_dir,
                            providers,
                            provider_runtime,
                            dry_run,
                        )
                    )
                    if len(futures) >= max_workers:
                        _drain_completed(
                            futures,
                            csv_writer,
                            csv_file,
                            checkpoint,
                            counters,
                            logger,
                            progress_bar,
                        )

                while futures:
                    _drain_completed(
                        futures,
                        csv_writer,
                        csv_file,
                        checkpoint,
                        counters,
                        logger,
                        progress_bar,
                    )
    finally:
        progress_bar.close()
        checkpoint.close()

    summary = _build_summary_from_csv(csv_path, dry_run=dry_run)
    summary.update(
        {
            "seen_this_run": counters["seen"],
            "processed_this_run": counters["processed"],
            "skipped_checkpoint": counters["skipped_checkpoint"],
            "failed_this_run": counters["failed"],
            "provider_failures_this_run": counters["provider_failures"],
        }
    )
    if TASK_CLASSIFICATION_CONFIG["write_summary"]:
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    logger.info("classification finished summary=%s", summary)
    return summary


def classify_one_image(
    path: Path,
    input_dir: Path,
    providers: list[dict],
    provider_runtime: ProviderRuntime,
    dry_run: bool = False,
) -> dict:
    payload = read_image_payload(path)
    record = {
        "classification_schema_version": CLASSIFICATION_SCHEMA_VERSION,
        "image_id": payload["sha256"][:16],
        "sha256": payload["sha256"],
        "relative_path": str(path.relative_to(input_dir)),
        "mime_type": payload["mime_type"],
        "size_bytes": payload["size_bytes"],
        "provider_results": [],
        "level2_provider_results": [],
    }

    if payload["mime_type"] not in DATA_CONFIG["allowed_mime_types"]:
        record["final"] = {
            "best_task_type": None,
            "best_anomaly_type": None,
            "decision_status": "invalid_image",
            "decision_reason": "unsupported_or_unknown_mime_type",
            "final_scores": {},
            "anomaly_scores": {},
        }
        return record

    if dry_run:
        record["final"] = {
            "best_task_type": None,
            "best_anomaly_type": None,
            "decision_status": "dry_run",
            "decision_reason": "image_validated_without_model_call",
            "final_scores": {},
            "anomaly_scores": {},
        }
        return record

    level1_provider_results = _classify_with_providers(
        providers,
        build_level1_messages(payload["data_uri"]),
        provider_runtime,
        normalize_task_result,
    )
    record["provider_results"] = level1_provider_results
    level1_vote = unanimous_vote(
        level1_provider_results,
        TASK_TYPES,
        score_field="task_scores",
        best_label_field="best_task_type",
        expected_provider_count=len(providers),
        score_threshold=TASK_CLASSIFICATION_CONFIG["level1_score_threshold"],
        require_all_providers=TASK_CLASSIFICATION_CONFIG[
            "require_all_selected_providers"
        ],
        min_valid_provider_count=TASK_CLASSIFICATION_CONFIG[
            "min_valid_provider_count"
        ],
    )
    best_task_type = level1_vote["best_label"]

    if best_task_type == "其它异常":
        level2_vote = {
            "final_scores": {"其它异常": 1.0},
            "best_label": "其它异常",
            "decision_status": level1_vote["decision_status"],
            "decision_reason": "level1_other_skips_second_level",
            "provider_votes": {},
            "provider_selected_scores": {},
            "vote_counts": {},
            "unanimous": level1_vote["unanimous"],
            "consensus_label": "其它异常",
            "score_threshold": TASK_CLASSIFICATION_CONFIG[
                "level2_score_threshold"
            ],
            "threshold_operator": ">",
            "threshold_passed": level1_vote["threshold_passed"],
            "complete_provider_pool": level1_vote["complete_provider_pool"],
            "require_all_providers": TASK_CLASSIFICATION_CONFIG[
                "require_all_selected_providers"
            ],
            "min_valid_provider_count": TASK_CLASSIFICATION_CONFIG[
                "min_valid_provider_count"
            ],
            "sufficient_provider_count": level1_vote[
                "sufficient_provider_count"
            ],
            "expected_provider_count": len(providers),
            "valid_provider_count": 0,
            "skipped": True,
        }
    else:
        anomaly_types = ANOMALY_TYPES_BY_TASK[best_task_type]
        level2_provider_results = _classify_with_providers(
            providers,
            build_level2_messages(payload["data_uri"], best_task_type),
            provider_runtime,
            lambda parsed: normalize_anomaly_result(parsed, anomaly_types),
        )
        record["level2_provider_results"] = level2_provider_results
        level2_vote = unanimous_vote(
            level2_provider_results,
            anomaly_types,
            score_field="anomaly_scores",
            best_label_field="best_anomaly_type",
            expected_provider_count=len(providers),
            other_label="其它任务类型",
            score_threshold=TASK_CLASSIFICATION_CONFIG["level2_score_threshold"],
            require_all_providers=TASK_CLASSIFICATION_CONFIG[
                "require_all_selected_providers"
            ],
            min_valid_provider_count=TASK_CLASSIFICATION_CONFIG[
                "min_valid_provider_count"
            ],
        )

    level1_decision = dict(level1_vote)
    level1_decision["best_task_type"] = level1_decision.pop("best_label")
    level2_decision = dict(level2_vote)
    level2_decision["anomaly_scores"] = level2_decision.pop("final_scores")
    level2_decision["best_anomaly_type"] = level2_decision.pop("best_label")
    overall_status = (
        "auto_accept"
        if level1_decision["decision_status"] == "auto_accept"
        and level2_decision["decision_status"] == "auto_accept"
        else "needs_review"
    )
    if level1_decision["decision_status"] != "auto_accept":
        overall_reason = f"level1:{level1_decision['decision_reason']}"
    elif level2_decision["decision_status"] != "auto_accept":
        overall_reason = f"level2:{level2_decision['decision_reason']}"
    elif best_task_type == "其它异常":
        overall_reason = "level1:all_providers_unanimous_other"
    else:
        overall_reason = "all_providers_unanimous_at_both_levels"

    record["final"] = {
        "best_task_type": level1_decision["best_task_type"],
        "best_anomaly_type": level2_decision["best_anomaly_type"],
        "decision_status": overall_status,
        "decision_reason": overall_reason,
        "final_scores": level1_decision["final_scores"],
        "anomaly_scores": level2_decision["anomaly_scores"],
        "valid_provider_count": level1_decision["valid_provider_count"],
        "level1": level1_decision,
        "level2": level2_decision,
    }
    return record


def _classify_with_providers(
    providers: list[dict],
    messages: list[dict],
    provider_runtime: ProviderRuntime,
    normalizer,
) -> list[dict]:
    max_workers = min(
        TASK_CLASSIFICATION_CONFIG["max_provider_workers_per_image"],
        max(1, len(providers)),
    )
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _call_and_parse_provider,
                provider,
                messages,
                provider_runtime,
                normalizer,
            )
            for provider in providers
        ]
        for future in futures:
            results.append(future.result())
    return sorted(results, key=lambda item: item["provider"])


def _call_and_parse_provider(
    provider: dict,
    messages: list[dict],
    provider_runtime: ProviderRuntime,
    normalizer,
) -> dict:
    provider_name = provider["name"]
    if provider_runtime.is_disabled(provider_name):
        return {
            "provider": provider_name,
            "model": provider["model"],
            "weight": provider.get("weight", 1),
            "ok": False,
            "skipped": True,
            "error": "provider_disabled",
        }

    try:
        with provider_runtime.semaphore_for(provider_name):
            response = call_chat_completion(
                provider,
                messages,
                retries=TASK_CLASSIFICATION_CONFIG["request_retries"],
                retry_sleep=TASK_CLASSIFICATION_CONFIG["retry_sleep_seconds"],
            )
        parsed = normalizer(parse_model_json(response["content"]))
        provider_runtime.register_success(provider_name)
        result = {
            "provider": provider_name,
            "model": provider["model"],
            "weight": provider.get("weight", 1),
            "ok": True,
            "parsed": parsed,
        }
        if TASK_CLASSIFICATION_CONFIG["save_provider_raw_response"]:
            result["raw_content"] = response["content"]
        return result
    except Exception as exc:
        error = str(exc)
        provider_runtime.register_failure(provider_name, error)
        return {
            "provider": provider_name,
            "model": provider["model"],
            "weight": provider.get("weight", 1),
            "ok": False,
            "error": error,
        }


def _drain_completed(
    futures: set,
    csv_writer: csv.DictWriter,
    csv_file,
    checkpoint: CheckpointStore,
    counters: Counter,
    logger: logging.Logger,
    progress_bar: ProgressBar,
) -> None:
    done, pending = wait(futures, return_when=FIRST_COMPLETED)
    futures.clear()
    futures.update(pending)
    for future in done:
        record = future.result()
        csv_writer.writerow(_record_to_csv_row(record))
        csv_file.flush()
        counters["processed"] += 1
        if record.get("final", {}).get("best_task_type") == "其它异常":
            counters["其它异常"] += 1
        provider_failures = [
            result
            for result in (
                record.get("provider_results", [])
                + record.get("level2_provider_results", [])
            )
            if not result.get("ok")
        ]
        counters["provider_failures"] += len(provider_failures)
        for result in provider_failures:
            logger.warning(
                "provider request failed image=%s provider=%s skipped=%s error=%s",
                record.get("relative_path"),
                result.get("provider"),
                bool(result.get("skipped")),
                result.get("error"),
            )
        status = record.get("final", {}).get("decision_status")
        if status == "failed":
            counters["failed"] += 1
        if status != "failed" or TASK_CLASSIFICATION_CONFIG["checkpoint_failed"]:
            checkpoint.mark_done(record)
        progress_bar.update(
            1,
            处理中=len(pending),
            失败=counters["failed"],
            其它异常=counters["其它异常"],
        )
        if counters["processed"] % TASK_CLASSIFICATION_CONFIG["progress_log_interval"] == 0:
            logger.info(
                "progress processed=%s skipped_checkpoint=%s failed=%s",
                counters["processed"],
                counters["skipped_checkpoint"],
                counters["failed"],
            )


def _reset_outputs(*paths: Path) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def _record_to_csv_row(record: dict) -> dict:
    final = record.get("final", {})
    level1 = final.get("level1", {})
    level2 = final.get("level2", {})
    scores = final.get("final_scores", {})
    anomaly_scores = final.get("anomaly_scores", {})
    return {
        "classification_schema_version": record.get(
            "classification_schema_version"
        ),
        "image_id": record.get("image_id"),
        "sha256": record.get("sha256"),
        "relative_path": record.get("relative_path"),
        "mime_type": record.get("mime_type"),
        "size_bytes": record.get("size_bytes"),
        "best_task_type": final.get("best_task_type"),
        "best_anomaly_type": final.get("best_anomaly_type"),
        "decision_status": final.get("decision_status"),
        "decision_reason": final.get("decision_reason"),
        "valid_provider_count": final.get("valid_provider_count"),
        "level1_expected_provider_count": level1.get("expected_provider_count"),
        "level1_valid_provider_count": level1.get("valid_provider_count"),
        "level1_sufficient_provider_count": level1.get(
            "sufficient_provider_count"
        ),
        "level1_unanimous": level1.get("unanimous"),
        "level1_score_threshold": level1.get("score_threshold"),
        "level1_threshold_passed": level1.get("threshold_passed"),
        "level1_provider_votes_json": _compact_json(level1.get("provider_votes")),
        "level1_provider_scores_json": _compact_json(
            level1.get("provider_selected_scores")
        ),
        "level1_provider_errors_json": _provider_errors_json(
            record.get("provider_results")
        ),
        "level2_unanimous": level2.get("unanimous"),
        "level2_expected_provider_count": level2.get("expected_provider_count"),
        "level2_valid_provider_count": level2.get("valid_provider_count"),
        "level2_sufficient_provider_count": level2.get(
            "sufficient_provider_count"
        ),
        "level2_score_threshold": level2.get("score_threshold"),
        "level2_threshold_passed": level2.get("threshold_passed"),
        "level2_provider_votes_json": _compact_json(level2.get("provider_votes")),
        "level2_provider_scores_json": _compact_json(
            level2.get("provider_selected_scores")
        ),
        "level2_provider_errors_json": _provider_errors_json(
            record.get("level2_provider_results")
        ),
        **{task_type: scores.get(task_type) for task_type in TASK_TYPES},
        **{
            f"二级_{anomaly_type}": anomaly_scores.get(anomaly_type)
            for anomaly_type in ALL_ANOMALY_TYPES
        },
    }


def _provider_errors_json(provider_results: list[dict] | None) -> str:
    return _compact_json(
        {
            result.get("provider", "unknown"): result.get("error", "unknown_error")
            for result in (provider_results or [])
            if not result.get("ok")
        }
    )


def _compact_json(value: object) -> str:
    return json.dumps(value or {}, ensure_ascii=False, separators=(",", ":"))


def _bootstrap_checkpoint_from_csv(
    csv_path: Path,
    checkpoint: CheckpointStore,
) -> int:
    restored = 0
    for row in _iter_csv_rows(csv_path):
        status = row.get("decision_status")
        if status != "failed" or TASK_CLASSIFICATION_CONFIG["checkpoint_failed"]:
            checkpoint.mark_done(
                {
                    "relative_path": row.get("relative_path"),
                    "image_id": row.get("image_id"),
                    "sha256": row.get("sha256"),
                    "final": {"decision_status": status},
                }
            )
            restored += 1
    return restored


def _build_summary_from_csv(csv_path: Path, dry_run: bool) -> dict:
    status_counts = Counter()
    label_counts = Counter()
    anomaly_counts = Counter()
    total = 0
    for row in _iter_csv_rows(csv_path):
        total += 1
        status_counts[row.get("decision_status") or None] += 1
        label_counts[row.get("best_task_type") or None] += 1
        anomaly_counts[row.get("best_anomaly_type") or None] += 1
    return {
        "stage": TASK_CLASSIFICATION_CONFIG["stage_name"],
        "dry_run": dry_run,
        "total_images": total,
        "status_counts": dict(status_counts),
        "label_counts": dict(label_counts),
        "anomaly_counts": dict(anomaly_counts),
    }


def _iter_csv_rows(path: Path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        yield from csv.DictReader(file)


def _validate_resume_csv(csv_path: Path) -> None:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        existing_fields = set(reader.fieldnames or [])
        missing_fields = set(CSV_FIELDNAMES) - existing_fields
        if missing_fields:
            raise RuntimeError(
                "existing classification CSV uses an incompatible schema; "
                f"missing_fields={sorted(missing_fields)}. "
                "Rerun classification with --no-resume to rebuild outputs."
            )
        first_row = next(reader, None)
        if first_row is None:
            return
        existing_version = first_row.get("classification_schema_version")
        if existing_version != CLASSIFICATION_SCHEMA_VERSION:
            raise RuntimeError(
                "existing classification CSV uses an incompatible version; "
                f"expected={CLASSIFICATION_SCHEMA_VERSION}, "
                f"found={existing_version!r}. "
                "Rerun classification with --no-resume to rebuild outputs."
            )
