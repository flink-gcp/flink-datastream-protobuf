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

"""Run the Release workflow's shell programs with synthetic inputs and a recording `just`.

Adapted from flink-connector-gcp's scripts/tests/test_release_workflow.py at
f5e8d50466dc8940e19b620aa84dbd530a7c6a70.
"""

import os
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
STUB = Path(__file__).with_name("record-just-stub.sh")
WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/release.yaml").read_text())
JOB = WORKFLOW["jobs"]["release"]
STEPS = {step["name"]: step for step in JOB["steps"] if "name" in step}
NOT_REHEARSAL = "inputs.mode != 'rehearse'"
PUSH = "github.event_name == 'push'"
DROP = "always() && github.event_name == 'workflow_dispatch' && inputs.mode != 'rehearse'"


@pytest.fixture
def just(tmp_path):
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "just").symlink_to(STUB)
    return {"PATH": f"{tools}{os.pathsep}{os.environ['PATH']}", "STUB_CALLS": str(tmp_path / "calls")}


def run(step, tmp_path, **env):
    return subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", step["run"]],
                          env={**os.environ, **env}, cwd=tmp_path, capture_output=True, text=True, check=False)


def calls(tmp_path):
    text = (tmp_path / "calls").read_text()
    return [call.splitlines() for call in text.split("----\n") if call]


@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
@pytest.mark.parametrize("version", ["1.0.0", "0.0.0", "10.20.30"])
def test_version_resolution(tmp_path, event, version):
    output = tmp_path / "output"
    result = run(STEPS["Resolve the release version"], tmp_path, GITHUB_OUTPUT=str(output), GITHUB_EVENT_NAME=event,
                 GITHUB_REF_NAME="v" + version if event == "push" else "main",
                 INPUT_VERSION=version if event == "workflow_dispatch" else "")
    assert result.returncode == 0, result.stderr
    assert output.read_text() == f"version={version}\n"


@pytest.mark.parametrize("version", ["v1.0.0", "1.0.0-1.20", "01.0.0", "1.0.0-SNAPSHOT", "1.0.0\nother=value", ""])
@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
def test_invalid_version_cannot_inject_outputs(tmp_path, version, event):
    output = tmp_path / "output"
    result = run(STEPS["Resolve the release version"], tmp_path, GITHUB_OUTPUT=str(output), GITHUB_EVENT_NAME=event,
                 GITHUB_REF_NAME="v" + version if event == "push" else "main",
                 INPUT_VERSION=version if event == "workflow_dispatch" else "")
    assert result.returncode != 0
    assert "expected bare X.Y.Z" in result.stderr
    assert not output.exists()


def git(repository, *arguments):
    environment = {"GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "test@invalid", "GIT_COMMITTER_NAME": "test",
                   "GIT_COMMITTER_EMAIL": "test@invalid"}
    return subprocess.run(["git", "-C", str(repository), "-c", "commit.gpgsign=false", *arguments], check=True,
                          capture_output=True, text=True, env={**os.environ, **environment}).stdout.strip()


@pytest.mark.parametrize("contained", [True, False])
def test_the_commit_must_be_on_main(tmp_path, contained):
    repository = tmp_path / "repository"
    git(tmp_path, "init", "-q", str(repository))
    git(repository, "commit", "-q", "--allow-empty", "-m", "main")
    git(repository, "update-ref", "refs/remotes/origin/main", "HEAD")
    if not contained:
        git(repository, "checkout", "-q", "-b", "side")
        git(repository, "commit", "-q", "--allow-empty", "-m", "unmerged")
    result = run(STEPS["Assert the commit is on main"], repository)
    assert (result.returncode == 0) == contained, result.stderr
    if not contained:
        assert "not contained in main" in result.stderr


def test_steps_run_in_the_order_that_gates_publication():
    names = list(STEPS)
    order = ["Resolve the release version", "Assert the commit is on main", "Rehearse both lines without uploading",
             "Stage the Flink 2.x line", "Stage the Flink 1.20 LTS line", "Publish both Central deployments",
             "Publish the GitHub Release", "Drop dry-run deployments"]
    assert [name for name in names if name in order] == order
    assert names[-1] == "Drop dry-run deployments"


def test_triggers_permissions_and_guards():
    # PyYAML reads the bare `on` key as True.
    triggers = WORKFLOW[True]
    assert triggers["push"] == {"tags": ["v[0-9]+.[0-9]+.[0-9]+"]}
    mode = triggers["workflow_dispatch"]["inputs"]["mode"]
    assert (mode["default"], mode["options"]) == ("rehearse", ["rehearse", "validate"])
    assert WORKFLOW["permissions"] == {} and JOB["permissions"] == {"contents": "write"}
    assert WORKFLOW["concurrency"] == {"group": "release", "cancel-in-progress": False}
    assert JOB["if"] == "github.repository_id == '1358906035'"
    assert JOB["environment"] == "release"
    # The mirror settings are global, so setup-java's user settings with the central server stay in effect.
    assert JOB["env"] == {"MAVEN_ARGS": "--global-settings ${{ github.workspace }}/.github/maven/settings.xml"}
    assert all(not step.get("uses", "").startswith("./.github/actions/maven") for step in JOB["steps"])
    checkout = JOB["steps"][0]
    assert checkout["with"]["persist-credentials"] is False and checkout["with"]["fetch-depth"] == 0
    assert STEPS["Assert the commit is on main"]["if"] == NOT_REHEARSAL
    # The tests run each step under the shell the workflow names, including pipefail.
    assert WORKFLOW["defaults"] == {"run": {"shell": "bash"}}
    java = next(step for step in JOB["steps"] if step.get("uses", "").startswith("actions/setup-java@"))
    assert {key: java["with"][key] for key in ("server-id", "server-username-env-var", "server-password-env-var")} == {
        "server-id": "central", "server-username-env-var": "CENTRAL_TOKEN_USERNAME",
        "server-password-env-var": "CENTRAL_TOKEN_PASSWORD"}


def test_every_run_rehearses_signed_without_central_credentials(tmp_path, just):
    rehearse = STEPS["Rehearse both lines without uploading"]
    assert "if" not in rehearse
    assert set(rehearse["env"]) == {"VERSION", "MAVEN_GPG_KEY", "MAVEN_GPG_PASSPHRASE"}
    names = list(STEPS)
    assert names.index("Rehearse both lines without uploading") < names.index("Stage the Flink 2.x line")
    result = run(rehearse, tmp_path, **just, VERSION="1.0.0", RUNNER_TEMP=str(tmp_path))
    assert result.returncode == 0, result.stderr
    assert calls(tmp_path) == [["release-rehearse", "1.0.0", str(tmp_path / "rehearsal")]]


@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
def test_both_lines_are_captured_and_named(tmp_path, just, event):
    for name in ("Stage the Flink 2.x line", "Stage the Flink 1.20 LTS line"):
        assert STEPS[name]["if"] == NOT_REHEARSAL
        result = run(STEPS[name], tmp_path, **just, VERSION="1.0.0", GITHUB_EVENT_NAME=event, GITHUB_RUN_ID="12",
                     GITHUB_RUN_ATTEMPT="2", RUNNER_TEMP=str(tmp_path))
        assert result.returncode == 0, result.stderr
    dry = " dry run" if event == "workflow_dispatch" else ""
    assert calls(tmp_path) == [
        ["central-portal", "capture", str(tmp_path / file), "just", "release-stage", version,
         f"-DdeploymentName=flink-datastream-protobuf {version} (run 12 attempt 2){dry}"]
        for file, version in (("central-2x.id", "1.0.0"), ("central-1x.id", "1.0.0-1.20"))]


@pytest.mark.parametrize("name", ["Stage the Flink 2.x line", "Stage the Flink 1.20 LTS line"])
def test_staging_failure_reaches_the_workflow(tmp_path, just, name):
    result = run(STEPS[name], tmp_path, **just, STUB_EXIT="7", VERSION="1.0.0",
                 GITHUB_EVENT_NAME="push", GITHUB_RUN_ID="12", GITHUB_RUN_ATTEMPT="2", RUNNER_TEMP=str(tmp_path))
    assert result.returncode == 7


def test_publication_follows_both_stages_and_precedes_the_github_release(tmp_path, just):
    publish, release = STEPS["Publish both Central deployments"], STEPS["Publish the GitHub Release"]
    # An if without a status function keeps Actions' implicit success() gate.
    assert publish["if"] == release["if"] == PUSH
    names = list(STEPS)
    assert (names.index("Stage the Flink 1.20 LTS line") < names.index("Publish both Central deployments")
            < names.index("Publish the GitHub Release"))
    (tmp_path / "central-2x.id").write_text("first\n")
    (tmp_path / "central-1x.id").write_text("second\n")
    result = run(publish, tmp_path, **just, RUNNER_TEMP=str(tmp_path))
    assert result.returncode == 0, result.stderr
    assert calls(tmp_path) == [["central-portal", "publish", "first", "second"]]
    assert set(release["env"]) == {"GH_TOKEN"}
    assert release["run"].startswith('gh release create "$GITHUB_REF_NAME"') and "--verify-tag" in release["run"]
    assert "--draft" not in release["run"] and "--prerelease" not in release["run"]


@pytest.mark.parametrize("files, dropped", [
    ({}, []),
    ({"central-2x.id": ""}, []),  # capture creates the file before Maven uploads anything
    ({"central-2x.id": "first\n"}, ["first"]),
    ({"central-2x.id": "first\n", "central-1x.id": "second\n"}, ["first", "second"]),
    ({"central-2x.id": "first\nextra\n", "central-1x.id": "second\n"}, ["first", "extra", "second"])])
def test_dry_run_cleanup_drops_every_captured_deployment(tmp_path, just, files, dropped):
    cleanup = STEPS["Drop dry-run deployments"]
    assert cleanup["if"] == DROP
    for file, text in files.items():
        (tmp_path / file).write_text(text)
    result = run(cleanup, tmp_path, **just, RUNNER_TEMP=str(tmp_path))
    assert result.returncode == 0, result.stderr
    if dropped:
        assert calls(tmp_path) == [["central-portal", "drop", *dropped]]
    else:
        assert not (tmp_path / "calls").exists()


def test_dry_run_cleanup_failure_is_not_green(tmp_path, just):
    (tmp_path / "central-2x.id").write_text("first\n")
    result = run(STEPS["Drop dry-run deployments"], tmp_path, **just, RUNNER_TEMP=str(tmp_path), STUB_EXIT="3")
    assert result.returncode == 3


def test_only_staging_publication_and_cleanup_hold_central_credentials():
    assert "secrets." not in str(WORKFLOW.get("env", {})) + str(JOB.get("env", {}))
    # Every step, named or not, in any field: env, with, or run.
    holders = {step.get("name", step.get("uses")) for step in JOB["steps"] if "secrets.CENTRAL_TOKEN" in str(step)}
    assert holders == {"Stage the Flink 2.x line", "Stage the Flink 1.20 LTS line",
                       "Publish both Central deployments", "Drop dry-run deployments"}
    assert {STEPS[name]["if"] for name in holders} == {NOT_REHEARSAL, PUSH, DROP}
    signers = {step.get("name", step.get("uses")) for step in JOB["steps"] if "secrets.MAVEN_GPG" in str(step)}
    assert signers == {"Rehearse both lines without uploading", "Stage the Flink 2.x line", "Stage the Flink 1.20 LTS line"}
    token = {step.get("name", step.get("uses")) for step in JOB["steps"] if "github.token" in str(step)}
    assert token == {"Publish the GitHub Release"}
