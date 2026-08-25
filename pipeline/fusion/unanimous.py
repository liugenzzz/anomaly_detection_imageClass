from __future__ import annotations

from collections import Counter


def unanimous_vote(
    provider_results: list[dict],
    labels: list[str],
    score_field: str,
    best_label_field: str,
    expected_provider_count: int,
    other_label: str = "其它异常",
    score_threshold: float = 0.90,
    require_all_providers: bool = False,
    min_valid_provider_count: int = 2,
) -> dict:
    """Fuse model results with a strict all-model unanimous vote.

    Scores are retained as an equal-weight arithmetic mean for audit and CSV
    output.  The winning label is accepted only when every configured provider
    returned a valid result, all participating providers selected the same
    label, and every participating provider scored that label strictly above
    ``score_threshold``. Unreachable or invalid providers are excluded from
    voting unless ``require_all_providers`` is enabled. The fallback label
    itself does not require a threshold.
    """
    if other_label not in labels:
        raise ValueError(f"other label must be in candidate labels: {other_label}")
    if not 0.0 <= score_threshold <= 1.0:
        raise ValueError(f"score threshold must be between 0 and 1: {score_threshold}")
    if min_valid_provider_count < 1:
        raise ValueError(
            f"minimum valid provider count must be positive: {min_valid_provider_count}"
        )

    result_provider_names = [
        str(result.get("provider", "")) for result in provider_results
    ]
    duplicate_provider_names = sorted(
        {
            name
            for name in result_provider_names
            if name and result_provider_names.count(name) > 1
        }
    )
    if duplicate_provider_names:
        raise ValueError(
            "Provider results must have unique names; duplicate names: "
            + ", ".join(duplicate_provider_names)
        )

    valid_results = [result for result in provider_results if result.get("ok")]
    valid_provider_count = len(valid_results)
    totals = {label: 0.0 for label in labels}
    provider_votes = {}
    provider_selected_scores = {}

    for result in valid_results:
        parsed = result.get("parsed") or {}
        scores = parsed.get(score_field) or {}
        for label in labels:
            totals[label] += float(scores.get(label, 0.0))
        vote = parsed.get(best_label_field)
        if vote not in labels:
            vote = other_label
        provider_votes[result["provider"]] = vote
        provider_selected_scores[result["provider"]] = float(scores.get(vote, 0.0))

    if valid_provider_count:
        final_scores = {
            label: round(totals[label] / valid_provider_count, 4)
            for label in labels
        }
    else:
        final_scores = {label: 0.0 for label in labels}

    vote_counts = Counter(provider_votes.values())
    complete_provider_pool = (
        expected_provider_count > 0
        and valid_provider_count == expected_provider_count
    )
    sufficient_provider_count = valid_provider_count >= min_valid_provider_count
    provider_pool_eligible = sufficient_provider_count and (
        complete_provider_pool or not require_all_providers
    )
    unanimous = (
        provider_pool_eligible
        and len(vote_counts) == 1
        and sum(vote_counts.values()) == valid_provider_count
    )
    consensus_label = next(iter(vote_counts)) if unanimous else None
    threshold_passed = bool(
        unanimous
        and (
            consensus_label == other_label
            or all(
                provider_selected_scores[provider_name] > score_threshold
                for provider_name in provider_votes
            )
        )
    )

    if not valid_provider_count:
        best_label = other_label
        decision_status = "needs_review"
        decision_reason = "no_valid_provider_result_routed_to_other"
    elif not sufficient_provider_count:
        best_label = other_label
        decision_status = "needs_review"
        decision_reason = "insufficient_valid_provider_count_routed_to_other"
    elif require_all_providers and not complete_provider_pool:
        best_label = other_label
        decision_status = "needs_review"
        decision_reason = "incomplete_provider_pool_routed_to_other"
    elif not unanimous:
        best_label = other_label
        decision_status = "needs_review"
        decision_reason = "provider_disagreement_routed_to_other"
    elif not threshold_passed:
        best_label = other_label
        decision_status = "needs_review"
        decision_reason = "unanimous_score_not_above_threshold_routed_to_other"
    else:
        best_label = consensus_label
        decision_status = "auto_accept"
        decision_reason = "all_providers_unanimous"

    return {
        "final_scores": final_scores,
        "best_label": best_label,
        "decision_status": decision_status,
        "decision_reason": decision_reason,
        "provider_votes": provider_votes,
        "provider_selected_scores": provider_selected_scores,
        "vote_counts": dict(vote_counts),
        "unanimous": unanimous,
        "consensus_label": consensus_label,
        "score_threshold": score_threshold,
        "threshold_operator": ">",
        "threshold_passed": threshold_passed,
        "complete_provider_pool": complete_provider_pool,
        "require_all_providers": require_all_providers,
        "min_valid_provider_count": min_valid_provider_count,
        "sufficient_provider_count": sufficient_provider_count,
        "expected_provider_count": expected_provider_count,
        "valid_provider_count": valid_provider_count,
    }
