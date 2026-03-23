"""
Tests for .github/workflows/build-and-push.yml release workflow configuration.

Run: python3 .github/tests/test_release_workflow.py
"""

import sys
import yaml
from pathlib import Path

WORKFLOW_PATH = Path(__file__).parent.parent / "workflows" / "build-and-push.yml"

with open(WORKFLOW_PATH) as f:
    _WF = yaml.safe_load(f)


def get_on(wf: dict) -> dict:
    """Return the 'on' trigger section.

    PyYAML (YAML 1.1) parses bare 'on' as boolean True.
    """
    return wf.get(True) or {}


# ---------------------------------------------------------------------------
# Trigger tests
# ---------------------------------------------------------------------------


def test_trigger_is_tag_push_not_branch():
    on = get_on(_WF)

    assert "push" in on, "Trigger must have 'push' event"
    push = on["push"]

    assert "tags" in push, (
        "Release workflow must trigger on 'push.tags', not 'push.branches'. "
        f"Current trigger: {push}"
    )

    assert "branches" not in push, (
        "Release workflow must NOT trigger on branch pushes — "
        "remove 'push.branches' to prevent publishing on every main commit."
    )


def test_trigger_tag_pattern_restricts_to_version_tags():
    tag_patterns = get_on(_WF)["push"]["tags"]

    assert any(p.startswith("v") for p in tag_patterns), (
        "Tag filter must be scoped to version tags (starting with 'v'). "
        f"Current patterns: {tag_patterns}"
    )

    assert not any(p.strip() == "*" for p in tag_patterns), (
        "Tag filter must NOT be '*' — use a scoped pattern like 'v*.*.*' "
        "to avoid publishing on non-release tags."
    )


# ---------------------------------------------------------------------------
# Docker metadata / image tag tests
# ---------------------------------------------------------------------------


def _get_meta_step(wf: dict) -> dict:
    steps = wf["jobs"]["build-and-release"]["steps"]
    meta = next((s for s in steps if s.get("id") == "meta"), None)
    assert meta is not None, "Docker metadata step with id='meta' must exist in build-and-release job"
    return meta


_TAGS_CONFIG = _get_meta_step(_WF)["with"]["tags"]


def test_docker_image_tagged_with_release_version():
    has_version_tag = (
        "type=semver" in _TAGS_CONFIG
        or "type=ref,event=tag" in _TAGS_CONFIG
    )
    assert has_version_tag, (
        "Docker metadata must include a version tag sourced from the Git tag. "
        "Use 'type=semver,pattern={{version}}' or 'type=ref,event=tag'. "
        f"Current tags config:\n{_TAGS_CONFIG}"
    )


def test_docker_image_tagged_with_latest():
    assert "latest" in _TAGS_CONFIG, (
        "Docker metadata must include 'type=raw,value=latest' for backwards compatibility. "
        f"Current tags config:\n{_TAGS_CONFIG}"
    )


def test_docker_image_not_tagged_only_with_sha():
    non_latest = [line.strip() for line in _TAGS_CONFIG.strip().splitlines() if line.strip() and "latest" not in line]

    assert not (non_latest and all(
        "github.sha" in line and "semver" not in line and "ref,event=tag" not in line
        for line in non_latest
    )), (
        "Image must not be tagged with only a commit SHA. "
        "Add a semver or ref,event=tag entry to carry the release version."
    )


# ---------------------------------------------------------------------------
# Job dependency (quality gate) tests
# ---------------------------------------------------------------------------


def test_build_and_release_requires_test_job():
    build_job = _WF["jobs"]["build-and-release"]

    assert "needs" in build_job, (
        "build-and-release must declare 'needs: test' to enforce the quality gate."
    )
    needs = build_job["needs"]
    needs_list = [needs] if isinstance(needs, str) else needs
    assert "test" in needs_list, f"'test' must be in needs, got: {needs}"


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _run_all():
    tests = [
        test_trigger_is_tag_push_not_branch,
        test_trigger_tag_pattern_restricts_to_version_tags,
        test_docker_image_tagged_with_release_version,
        test_docker_image_tagged_with_latest,
        test_docker_image_not_tagged_only_with_sha,
        test_build_and_release_requires_test_job,
    ]

    failures = []
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except (AssertionError, KeyError, TypeError) as e:
            print(f"  FAIL  {t.__name__}")
            print(f"        {e}")
            failures.append(t.__name__)

    print()
    total = len(tests)
    if failures:
        print(f"FAILED  {len(failures)}/{total} tests failed")
        sys.exit(1)
    else:
        print(f"PASSED  {total}/{total} tests passed")


if __name__ == "__main__":
    _run_all()
