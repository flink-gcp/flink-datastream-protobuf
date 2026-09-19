# Copyright 2026 The flink-gcp authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Evaluate publication guards against PR, main, fork and release events."""

import re
from pathlib import Path
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def evaluate(text, context):
    text = text.removeprefix("${{").removesuffix("}}").strip()
    text = re.sub(
        r"(?:github|needs)\.[a-zA-Z0-9_.]+", lambda m: repr(context.get(m[0], "")), text
    )
    return eval(
        text.replace("&&", " and ").replace("||", " or "),
        {
            "__builtins__": {},
            "format": lambda template, *args: template.format(*args),
            "always": lambda: True,
        },
    )


@pytest.mark.parametrize(
    "event,ref,result,upstream,repository,build,publish",
    [
        ("pull_request", "refs/pull/1/merge", "", "", "1358906035", True, False),
        ("push", "refs/heads/main", "", "", "1358906035", True, True),
        ("workflow_dispatch", "refs/heads/main", "", "", "1358906035", True, True),
        ("workflow_dispatch", "refs/heads/topic", "", "", "1358906035", True, False),
        ("push", "refs/heads/main", "", "", "fork", True, False),
        (
            "workflow_run",
            "refs/heads/main",
            "success",
            "push",
            "1358906035",
            True,
            True,
        ),
        (
            "workflow_run",
            "refs/heads/main",
            "failure",
            "push",
            "1358906035",
            False,
            False,
        ),
        (
            "workflow_run",
            "refs/heads/main",
            "cancelled",
            "push",
            "1358906035",
            False,
            False,
        ),
        (
            "workflow_run",
            "refs/heads/main",
            "success",
            "workflow_dispatch",
            "1358906035",
            False,
            False,
        ),
    ],
)
def test_publication_requires_trusted_main_and_successful_release(
    event, ref, result, upstream, repository, build, publish
):
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yaml").read_text())
    context = {
        "github.event_name": event,
        "github.ref": ref,
        "github.repository_id": repository,
        "github.event.workflow_run.head_repository.id": repository,
        "github.event.workflow_run.conclusion": result,
        "github.event.workflow_run.event": upstream,
    }
    source = workflow["jobs"]["source"]
    assert bool(evaluate(source["if"], context)) == build
    assert bool(build and evaluate(source["outputs"]["publish"], context)) == publish
    context["needs.source.result"] = "success" if build else "skipped"
    assert bool(evaluate(workflow["jobs"]["ci_passed"]["if"], context)) == build
    if publish:
        assert evaluate(workflow["concurrency"]["group"], context) == "docs-publication"
        assert not evaluate(workflow["concurrency"]["cancel-in-progress"], context)


@pytest.mark.parametrize("result", ["success", "failure", "cancelled"])
def test_aggregate_still_reports_source_failures(result):
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yaml").read_text())
    assert evaluate(
        workflow["jobs"]["ci_passed"]["if"], {"needs.source.result": result}
    )


def test_deploy_is_gated_and_only_job_with_write_permissions():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yaml").read_text())
    assert workflow["permissions"] == {}
    assert set(workflow["jobs"]["ci_passed"]["needs"]) == {
        "source",
        "verify",
        "lint",
        "docs",
    }
    deploy = workflow["jobs"]["deploy"]
    assert "ci_passed" in deploy["needs"]
    assert deploy["permissions"] == {"pages": "write", "id-token": "write"}
    assert deploy["environment"]["name"] == "github-pages"
    for name, job in workflow["jobs"].items():
        if name != "deploy":
            assert "write" not in job.get("permissions", {}).values()
    docs = yaml.safe_load((ROOT / ".github/workflows/docs.yaml").read_text())
    assert docs["permissions"] == {"contents": "read"}
    assert set(docs["jobs"]["docs_passed"]["needs"]) == {"plan", "build", "assemble"}
