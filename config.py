from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent


CLASSIFICATION_SCHEMA_VERSION = "two_level_available_provider_vote_csv_v3"


TASK_TYPES = [
    "军事异常检测",
    "工业设备异常检测",
    "飞机螺丝及结构异常检测",
    "其它异常",
]


ANOMALY_TYPES_BY_TASK = {
    "军事异常检测": ["集结", "爆炸", "烟雾", "越界移动", "其它任务类型"],
    "工业设备异常检测": ["发热", "泄漏", "结构变形", "其它任务类型"],
    "飞机螺丝及结构异常检测": ["飞机螺丝缺失", "飞机锈蚀", "其它任务类型"],
    "其它异常": ["其它异常"],
}


ALL_ANOMALY_TYPES = list(
    dict.fromkeys(
        anomaly_type
        for task_type in TASK_TYPES
        for anomaly_type in ANOMALY_TYPES_BY_TASK[task_type]
    )
)


PROVIDERS = [
    {
        "name": "fx_q3_235",
        "enabled": True,
        "url": "https://mcc-pre.3xmt.com/gateway/ai-service/v1/chat/completions",
        "model": "fx-q3-235",
        "api_key": "sk-2h1RwMjqYcdh6F5Fs5",
        "stream": False,
        "temperature": 0.6,
        "max_tokens": 8192,
        "timeout": 2400,
        "chat_template_kwargs": {"enable_thinking": False},
        # urllib/OpenSSL repeatedly raises ASN1 NOT_ENOUGH_DATA for this
        # gateway's image requests; requests uses a more robust HTTP stack.
        "transport": "requests",
        "capabilities": ["text", "image"],
        "task_types": TASK_TYPES,
        "weight": 1,
        # This HTTPS gateway has occasionally returned malformed TLS records
        # under parallel urllib requests. Keep one in-flight request while
        # retaining image-level concurrency for the other providers.
        "max_concurrency": 1,
    },
    {
        "name": "Qwen3.6-27B",
        "enabled": True,
        "url": "http://192.168.78.36:3012/v1/chat/completions",
        "model": "Qwen3.6-27B",
        "api_key": "sk-bveYeVn6NAdRRElTWCqhtyJbkTL5XwweedczV9FJ05kDqhqX",
        "stream": False,
        "temperature": 0.6,
        "max_tokens": 8192,
        "timeout": 2400,
        "chat_template_kwargs": {"enable_thinking": False},
        "capabilities": ["text", "image"],
        "task_types": TASK_TYPES,
        "weight": 2,
        "max_concurrency": 4,
    },
]


DATA_CONFIG = {
    "input_dir": PROJECT_ROOT / "data",
    "recursive": False,
    "allowed_mime_types": {
        "image/jpeg",
        "image/png",
        "image/webp",
        "image/gif",
        "image/bmp",
    },
}


LOGGING_CONFIG = {
    "log_dir": PROJECT_ROOT / "logs",
    "log_level": "INFO",
    "console": True,
}


TASK_CLASSIFICATION_CONFIG = {
    "stage_name": "task_type_classification",
    "output_dir": PROJECT_ROOT / "outputs" / "task_type_classification",
    "provider_config_file": None,
    "fusion_method": "unanimous_vote",
    "require_all_selected_providers": False,
    "min_valid_provider_count": 2,
    "level1_score_threshold": 0.7,
    "level2_score_threshold": 0.65,
    "image_max_workers": 3,
    "max_provider_workers_per_image": 3,
    "request_retries": 2,
    "retry_sleep_seconds": 2,
    "disable_provider_after_consecutive_failures": 5,
    "resume": True,
    "checkpoint_file": PROJECT_ROOT / "outputs" / "task_type_classification" / "checkpoint.sqlite3",
    "checkpoint_failed": False,
    "progress_log_interval": 100,
    "save_provider_raw_response": False,
    "write_summary": True,
}


TASK_ORGANIZATION_CONFIG = {
    "stage_name": "task_type_organization",
    "input_dir": DATA_CONFIG["input_dir"],
    "classification_results_file": TASK_CLASSIFICATION_CONFIG["output_dir"] / "results.csv",
    "output_dir": PROJECT_ROOT / "data_by_task",
    "copy_files": True,
    "overwrite": True,
    "unclassified_dir": "_未分类",
    "write_manifest": True,
    "manifest_file": PROJECT_ROOT / "data_by_task" / "manifest.jsonl",
    "summary_file": PROJECT_ROOT / "data_by_task" / "summary.json",
    "progress_log_interval": 100,
}
