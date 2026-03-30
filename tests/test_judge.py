from __future__ import annotations

from oss_ai_stack_map.models.core import ClassificationDecision, JudgeDecision
from oss_ai_stack_map.pipeline.classification import (
    apply_judge_decisions,
    merge_judge_decisions,
    select_judge_candidates,
    should_send_to_judge,
    should_send_to_validation_judge,
)


def test_should_send_to_judge_on_borderline_scores(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    decision = ClassificationDecision(
        repo_id=1,
        full_name="owner/repo",
        passed_candidate_filter=True,
        passed_serious_filter=False,
        passed_ai_relevance_filter=False,
        passed_major_filter=False,
        score_serious=runtime.study.classification.serious_pass_score,
        score_ai=1,
        notes=[],
    )
    assert should_send_to_judge(runtime=runtime, decision=decision)


def test_apply_judge_decision_overrides_on_high_confidence(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.override_on_high_confidence = True
    runtime.study.judge.min_confidence_to_override = "high"
    runtime.segments.precedence = ["ai_application"]
    decision = ClassificationDecision(
        repo_id=1,
        full_name="owner/repo",
        passed_candidate_filter=True,
        passed_serious_filter=False,
        passed_ai_relevance_filter=False,
        passed_major_filter=False,
        score_serious=2,
        score_ai=2,
        notes=[],
    )
    judge = JudgeDecision(
        repo_id=1,
        full_name="owner/repo",
        judge_mode="hardening",
        serious_project=True,
        ai_relevant=True,
        include_in_final_set=True,
        primary_segment="ai_application",
        confidence="high",
        override_rule_decision=True,
        reasons=["Clear product repo"],
        model="gpt-5.4-nano",
    )

    apply_judge_decisions(runtime=runtime, decisions=[decision], judge_decisions=[judge])

    assert decision.passed_major_filter is True
    assert decision.judge_override_applied is True
    assert decision.judge_mode == "hardening"
    assert decision.primary_segment == "ai_application"
    assert decision.rule_passed_major_filter is False
    assert judge.applied is True


def test_validation_judge_applies_when_it_disagrees_with_rule(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.override_on_high_confidence = True
    runtime.study.judge.min_confidence_to_override = "high"
    decision = ClassificationDecision(
        repo_id=1,
        full_name="owner/repo",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        rule_passed_serious_filter=True,
        rule_passed_ai_relevance_filter=True,
        rule_passed_major_filter=True,
        score_serious=6,
        score_ai=6,
        notes=[],
    )
    judge = JudgeDecision(
        repo_id=1,
        full_name="owner/repo",
        judge_mode="validation",
        serious_project=False,
        ai_relevant=False,
        include_in_final_set=False,
        primary_segment=None,
        confidence="high",
        override_rule_decision=False,
        reasons=["Collection repo"],
        model="gpt-5.4-nano",
    )

    apply_judge_decisions(runtime=runtime, decisions=[decision], judge_decisions=[judge])

    assert decision.passed_major_filter is False
    assert decision.judge_override_applied is True
    assert decision.judge_mode == "validation"


def test_validation_pass_preserves_original_rule_fields(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.override_on_high_confidence = True
    runtime.study.judge.min_confidence_to_override = "high"
    decision = ClassificationDecision(
        repo_id=1,
        full_name="owner/repo",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        rule_passed_serious_filter=True,
        rule_passed_ai_relevance_filter=False,
        rule_passed_major_filter=False,
        score_serious=6,
        score_ai=2,
        notes=["hardening judge override applied"],
    )
    judge = JudgeDecision(
        repo_id=1,
        full_name="owner/repo",
        judge_mode="validation",
        serious_project=True,
        ai_relevant=True,
        include_in_final_set=True,
        primary_segment=None,
        confidence="high",
        override_rule_decision=False,
        reasons=["Validation confirms prior override"],
        model="gpt-5.4-nano",
    )

    apply_judge_decisions(runtime=runtime, decisions=[decision], judge_decisions=[judge])

    assert decision.passed_ai_relevance_filter is True
    assert decision.passed_major_filter is True
    assert decision.rule_passed_ai_relevance_filter is False
    assert decision.rule_passed_major_filter is False
    assert decision.judge_override_applied is False
    assert judge.applied is False


def test_high_confidence_hardening_judge_is_authoritative(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.override_on_high_confidence = True
    runtime.study.judge.min_confidence_to_override = "high"
    decision = ClassificationDecision(
        repo_id=1,
        full_name="owner/repo",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        rule_passed_serious_filter=True,
        rule_passed_ai_relevance_filter=True,
        rule_passed_major_filter=True,
        score_serious=6,
        score_ai=6,
        notes=[],
    )
    judge = JudgeDecision(
        repo_id=1,
        full_name="owner/repo",
        judge_mode="hardening",
        serious_project=False,
        ai_relevant=False,
        include_in_final_set=False,
        primary_segment=None,
        confidence="high",
        override_rule_decision=False,
        reasons=["Judge is authoritative"],
        model="gpt-5.4-nano",
    )

    apply_judge_decisions(runtime=runtime, decisions=[decision], judge_decisions=[judge])

    assert decision.passed_serious_filter is False
    assert decision.passed_ai_relevance_filter is False
    assert decision.passed_major_filter is False
    assert decision.judge_override_applied is True


def test_low_confidence_hardening_judge_does_not_override(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.override_on_high_confidence = True
    runtime.study.judge.min_confidence_to_override = "high"
    decision = ClassificationDecision(
        repo_id=1,
        full_name="owner/repo",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        rule_passed_serious_filter=True,
        rule_passed_ai_relevance_filter=True,
        rule_passed_major_filter=True,
        score_serious=6,
        score_ai=6,
        notes=[],
    )
    judge = JudgeDecision(
        repo_id=1,
        full_name="owner/repo",
        judge_mode="hardening",
        serious_project=False,
        ai_relevant=False,
        include_in_final_set=False,
        primary_segment=None,
        confidence="medium",
        override_rule_decision=False,
        reasons=["Judge is not authoritative at medium confidence"],
        model="gpt-5.4-nano",
    )

    apply_judge_decisions(runtime=runtime, decisions=[decision], judge_decisions=[judge])

    assert decision.passed_serious_filter is True
    assert decision.passed_ai_relevance_filter is True
    assert decision.passed_major_filter is True
    assert decision.judge_override_applied is False
    assert judge.applied is False


def test_invalid_judge_segment_is_ignored(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.override_on_high_confidence = True
    runtime.study.judge.min_confidence_to_override = "high"
    runtime.segments.precedence = ["serving_runtime"]
    decision = ClassificationDecision(
        repo_id=1,
        full_name="owner/repo",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        score_serious=6,
        score_ai=6,
        primary_segment="serving_runtime",
        notes=[],
    )
    judge = JudgeDecision(
        repo_id=1,
        full_name="owner/repo",
        judge_mode="validation",
        serious_project=True,
        ai_relevant=True,
        include_in_final_set=True,
        primary_segment="agent_platform",
        confidence="high",
        override_rule_decision=True,
        reasons=["segment only"],
        model="gpt-5.4-nano",
    )

    apply_judge_decisions(runtime=runtime, decisions=[decision], judge_decisions=[judge])

    assert decision.primary_segment == "serving_runtime"
    assert decision.judge_primary_segment is None
    assert judge.applied is False
    assert "validation judge segment ignored: agent_platform" in decision.notes


def test_merge_judge_decisions_replaces_by_repo_id() -> None:
    existing = [
        JudgeDecision(
            repo_id=1,
            full_name="owner/repo-1",
            judge_mode="hardening",
            serious_project=False,
            ai_relevant=False,
            include_in_final_set=False,
            confidence="medium",
            override_rule_decision=False,
            reasons=["old"],
            model="gpt-5.4-nano",
        )
    ]
    new = [
        JudgeDecision(
            repo_id=1,
            full_name="owner/repo-1",
            judge_mode="validation",
            serious_project=True,
            ai_relevant=True,
            include_in_final_set=True,
            confidence="high",
            override_rule_decision=True,
            reasons=["new"],
            model="gpt-5.4-nano",
        ),
        JudgeDecision(
            repo_id=2,
            full_name="owner/repo-2",
            judge_mode="validation",
            serious_project=True,
            ai_relevant=False,
            include_in_final_set=False,
            confidence="medium",
            override_rule_decision=False,
            reasons=["another"],
            model="gpt-5.4-nano",
        ),
    ]

    merged = merge_judge_decisions(existing, new)

    assert [decision.repo_id for decision in merged] == [1, 2]
    assert merged[0].reasons == ["new"]
    assert merged[0].judge_mode == "validation"


def test_should_send_to_validation_judge_for_selected_repo() -> None:
    decision = ClassificationDecision(
        repo_id=1,
        full_name="owner/repo",
        passed_candidate_filter=True,
        passed_serious_filter=True,
        passed_ai_relevance_filter=True,
        passed_major_filter=True,
        score_serious=5,
        score_ai=5,
        notes=[],
    )
    assert should_send_to_validation_judge(decision)


def test_select_judge_candidates_fills_remaining_capacity_with_validation(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.hardening_enabled = True
    runtime.study.judge.validation_enabled = True
    runtime.study.judge.max_cases_per_run = 3
    decisions = [
        ClassificationDecision(
            repo_id=1,
            full_name="owner/repo-1",
            passed_candidate_filter=True,
            passed_serious_filter=False,
            passed_ai_relevance_filter=False,
            passed_major_filter=False,
            score_serious=3,
            score_ai=1,
            notes=[],
        ),
        ClassificationDecision(
            repo_id=2,
            full_name="owner/repo-2",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            notes=[],
        ),
        ClassificationDecision(
            repo_id=3,
            full_name="owner/repo-3",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=7,
            score_ai=7,
            notes=[],
        ),
        ClassificationDecision(
            repo_id=4,
            full_name="owner/repo-4",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=7,
            score_ai=7,
            notes=[],
        ),
    ]

    candidates = select_judge_candidates(
        runtime=runtime,
        decisions=decisions,
        already_judged_repo_ids={4},
    )

    assert [(candidate.decision.repo_id, candidate.judge_mode) for candidate in candidates] == [
        (1, "hardening"),
        (2, "validation"),
        (3, "validation"),
    ]


def test_select_judge_candidates_can_run_hardening_only(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.hardening_enabled = True
    runtime.study.judge.validation_enabled = False
    runtime.study.judge.max_cases_per_run = 3
    decisions = [
        ClassificationDecision(
            repo_id=1,
            full_name="owner/repo-1",
            passed_candidate_filter=True,
            passed_serious_filter=False,
            passed_ai_relevance_filter=False,
            passed_major_filter=False,
            score_serious=3,
            score_ai=1,
            notes=[],
        ),
        ClassificationDecision(
            repo_id=2,
            full_name="owner/repo-2",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            notes=[],
        ),
    ]

    candidates = select_judge_candidates(runtime=runtime, decisions=decisions)

    assert [(candidate.decision.repo_id, candidate.judge_mode) for candidate in candidates] == [
        (1, "hardening")
    ]


def test_select_judge_candidates_can_run_validation_only(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.enabled = False
    runtime.study.judge.hardening_enabled = False
    runtime.study.judge.validation_enabled = True
    runtime.study.judge.max_cases_per_run = 3
    decisions = [
        ClassificationDecision(
            repo_id=1,
            full_name="owner/repo-1",
            passed_candidate_filter=True,
            passed_serious_filter=False,
            passed_ai_relevance_filter=False,
            passed_major_filter=False,
            score_serious=3,
            score_ai=1,
            notes=[],
        ),
        ClassificationDecision(
            repo_id=2,
            full_name="owner/repo-2",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            notes=[],
        ),
        ClassificationDecision(
            repo_id=3,
            full_name="owner/repo-3",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=7,
            score_ai=7,
            notes=[],
        ),
    ]

    candidates = select_judge_candidates(runtime=runtime, decisions=decisions)

    assert [(candidate.decision.repo_id, candidate.judge_mode) for candidate in candidates] == [
        (2, "validation"),
        (3, "validation"),
    ]


def test_select_judge_candidates_can_sample_validation_set_by_fraction(runtime_config) -> None:
    runtime = runtime_config["runtime"]
    runtime.study.judge.enabled = False
    runtime.study.judge.hardening_enabled = False
    runtime.study.judge.validation_enabled = True
    runtime.study.judge.max_cases_per_run = 10
    runtime.study.judge.validation_sample_fraction = 0.4
    runtime.study.judge.validation_sample_seed = 7
    decisions = [
        ClassificationDecision(
            repo_id=1,
            full_name="owner/repo-1",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_a",
            notes=[],
        ),
        ClassificationDecision(
            repo_id=2,
            full_name="owner/repo-2",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_a",
            notes=[],
        ),
        ClassificationDecision(
            repo_id=3,
            full_name="owner/repo-3",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_b",
            notes=[],
        ),
        ClassificationDecision(
            repo_id=4,
            full_name="owner/repo-4",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_b",
            notes=[],
        ),
        ClassificationDecision(
            repo_id=5,
            full_name="owner/repo-5",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_c",
            notes=[],
        ),
        ClassificationDecision(
            repo_id=6,
            full_name="owner/repo-6",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_c",
            notes=[],
        ),
        ClassificationDecision(
            repo_id=7,
            full_name="owner/repo-7",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_d",
            notes=[],
        ),
        ClassificationDecision(
            repo_id=8,
            full_name="owner/repo-8",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_d",
            notes=[],
        ),
        ClassificationDecision(
            repo_id=9,
            full_name="owner/repo-9",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_e",
            notes=[],
        ),
        ClassificationDecision(
            repo_id=10,
            full_name="owner/repo-10",
            passed_candidate_filter=True,
            passed_serious_filter=True,
            passed_ai_relevance_filter=True,
            passed_major_filter=True,
            score_serious=6,
            score_ai=6,
            primary_segment="segment_e",
            notes=[],
        ),
    ]

    candidates = select_judge_candidates(runtime=runtime, decisions=decisions)

    assert len(candidates) == 4
    assert all(candidate.judge_mode == "validation" for candidate in candidates)
    assert {candidate.decision.primary_segment for candidate in candidates} == {
        "segment_a",
        "segment_b",
        "segment_c",
        "segment_d",
    }
    assert [candidate.decision.repo_id for candidate in candidates] == [1, 4, 5, 8]
