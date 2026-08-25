from __future__ import annotations

import argparse
from pathlib import Path

from config import DATA_CONFIG, PROVIDERS, TASK_CLASSIFICATION_CONFIG, TASK_ORGANIZATION_CONFIG
from pipeline.core.provider_pool import load_provider_pool
from pipeline.stages.task_type.classifier import classify_dataset
from pipeline.stages.task_type.organizer import organize_by_task_type


def main() -> None:
    parser = argparse.ArgumentParser(description="Modular data-processing pipeline")
    parser.add_argument(
        "--run-stages",
        choices=["all", "classify", "organize"],
        default="all",
        help="Default run mode when no subcommand is provided. Defaults to all.",
    )
    parser.add_argument("--input-dir", type=Path, default=None)
    parser.add_argument(
        "--recursive",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Walk all subdirectories under --input-dir (needed when images "
            "are split across many subfolders). Defaults to "
            "DATA_CONFIG['recursive'] in config.py when not passed."
        ),
    )
    parser.add_argument(
        "--classification-output-dir",
        type=Path,
        default=TASK_CLASSIFICATION_CONFIG["output_dir"],
    )
    parser.add_argument(
        "--organization-output-dir",
        type=Path,
        default=TASK_ORGANIZATION_CONFIG["output_dir"],
    )
    parser.add_argument(
        "--results-file",
        type=Path,
        default=None,
        help="Classification CSV used by organize mode. Defaults to classification output results.csv.",
    )
    parser.add_argument(
        "--providers",
        default="all",
        help="Comma-separated provider names. Defaults to all providers.",
    )
    parser.add_argument(
        "--provider-config",
        type=Path,
        default=TASK_CLASSIFICATION_CONFIG["provider_config_file"],
        help="Optional JSON file used to dynamically load the provider pool.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable checkpoint resume and rebuild classification outputs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate classification input/output without model calls. Organize is skipped in all mode.",
    )

    subparsers = parser.add_subparsers(dest="stage")

    classify_parser = subparsers.add_parser(
        "task-classify",
        help="Run two-level image anomaly classification",
    )
    classify_parser.add_argument("--input-dir", type=Path, default=None)
    classify_parser.add_argument(
        "--recursive",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Walk all subdirectories under --input-dir. Defaults to config.py's DATA_CONFIG['recursive'].",
    )
    classify_parser.add_argument("--output-dir", type=Path, default=TASK_CLASSIFICATION_CONFIG["output_dir"])
    classify_parser.add_argument(
        "--providers",
        default="all",
        help="Comma-separated provider names. Defaults to all providers.",
    )
    classify_parser.add_argument(
        "--provider-config",
        type=Path,
        default=TASK_CLASSIFICATION_CONFIG["provider_config_file"],
        help="Optional JSON file used to dynamically load the provider pool.",
    )
    classify_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate image discovery and output writing without model calls.",
    )
    classify_parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable checkpoint resume and rebuild classification outputs.",
    )
    classify_parser.add_argument(
        "--shard-count",
        type=int,
        default=None,
        help=(
            "Split the dataset into this many shards for parallel processes "
            "(each shard writes to its own <output-dir>/shard_i_of_N/ with an "
            "independent results.csv/checkpoint/manifest, no coordination "
            "needed between processes). Requires --shard-index."
        ),
    )
    classify_parser.add_argument(
        "--shard-index",
        type=int,
        default=None,
        help="0-based index of this process's shard. Requires --shard-count.",
    )

    organize_parser = subparsers.add_parser(
        "task-organize",
        help="Copy images into first/second-level folders from classification results",
    )
    organize_parser.add_argument("--input-dir", type=Path, default=None)
    organize_parser.add_argument("--results-file", type=Path, default=TASK_ORGANIZATION_CONFIG["classification_results_file"])
    organize_parser.add_argument("--output-dir", type=Path, default=TASK_ORGANIZATION_CONFIG["output_dir"])

    args = parser.parse_args()
    if args.stage == "task-classify":
        selected_providers = select_providers(args.providers, args.provider_config)
        if not selected_providers and not args.dry_run:
            raise SystemExit(f"No providers selected from: {args.providers}")
        summary = classify_dataset(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            providers=selected_providers,
            dry_run=args.dry_run,
            resume=not args.no_resume,
            recursive=args.recursive,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
        )
    elif args.stage == "task-organize":
        summary = organize_by_task_type(
            input_dir=args.input_dir,
            results_file=args.results_file,
            output_dir=args.output_dir,
        )
    elif args.stage is None:
        summary = run_default_pipeline(args)
    else:
        raise SystemExit(f"Unsupported stage: {args.stage}")
    print(summary)


def run_default_pipeline(args: argparse.Namespace) -> dict:
    summaries = {}
    results_file = args.results_file or (args.classification_output_dir / "results.csv")

    if args.run_stages in {"all", "classify"}:
        selected_providers = select_providers(args.providers, args.provider_config)
        if not selected_providers and not args.dry_run:
            raise SystemExit(f"No providers selected from: {args.providers}")
        summaries["task_classify"] = classify_dataset(
            input_dir=args.input_dir,
            output_dir=args.classification_output_dir,
            providers=selected_providers,
            dry_run=args.dry_run,
            resume=not args.no_resume,
            recursive=args.recursive,
        )

    if args.run_stages in {"all", "organize"}:
        if args.dry_run and args.run_stages == "all":
            summaries["task_organize"] = {
                "stage": TASK_ORGANIZATION_CONFIG["stage_name"],
                "skipped": True,
                "reason": "dry_run_classification_has_no_final_labels",
            }
        elif args.run_stages == "all" and not TASK_ORGANIZATION_CONFIG["materialize_files"]:
            # classify_dataset already wrote <classification_output_dir>/manifest.jsonl
            # inline in this case (see classifier.py's write_inline_manifest) —
            # running organize again would just rebuild the same mapping from
            # results.csv a second time for no benefit. `--run-stages organize`
            # on its own still works if you ever need to force a rebuild.
            summaries["task_organize"] = {
                "stage": TASK_ORGANIZATION_CONFIG["stage_name"],
                "skipped": True,
                "reason": "manifest_already_written_inline_by_classify",
                "manifest_file": str(args.classification_output_dir / "manifest.jsonl"),
            }
        else:
            summaries["task_organize"] = organize_by_task_type(
                input_dir=args.input_dir,
                results_file=results_file,
                output_dir=args.organization_output_dir,
            )

    return {
        "run_stages": args.run_stages,
        "summaries": summaries,
    }


def select_providers(provider_names_text: str, provider_config_file: Path | None) -> list[dict]:
    return load_provider_pool(provider_names_text, provider_config_file)


if __name__ == "__main__":
    main()
