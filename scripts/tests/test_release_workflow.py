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
f5e8d50466dc8940e19b620aa84dbd530a7c6a70, with the job split and re-run guard tests of
2b417720fbdbb7c3347ff5bd6253c216862d3273.
"""

import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest
import yaml

from conftest import load_script

ROOT = Path(__file__).resolve().parents[2]
STUB = Path(__file__).with_name("record-just-stub.sh")
GH_STUB = Path(__file__).with_name("release-gh-stub.sh")
WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/release.yaml").read_text())
JOBS = WORKFLOW["jobs"]
JOB = JOBS["stage"]


def named_steps(job):
    return {step["name"]: step for step in job["steps"] if "name" in step}


STEPS = named_steps(JOB)
NOT_REHEARSAL = "inputs.mode != 'rehearse'"
PUSH = "github.event_name == 'push'"
DROP = "always() && github.event_name == 'workflow_dispatch' && inputs.mode != 'rehearse'"


@pytest.fixture
def just(tmp_path):
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "just").symlink_to(STUB)
    # A stub that lost its exec bit would let the real just run the release recipes.
    assert shutil.which("just", path=str(tools)) == str(tools / "just")
    return {"PATH": f"{tools}{os.pathsep}{os.environ['PATH']}", "STUB_CALLS": str(tmp_path / "calls")}


def run(step, tmp_path, **env):
    return subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", step["run"]],
                          env={**os.environ, **env}, cwd=tmp_path, capture_output=True, text=True, check=False)


@pytest.fixture
def gh(tmp_path):
    tools = tmp_path / "gh-tools"
    tools.mkdir()
    (tools / "gh").symlink_to(GH_STUB)
    # A stub that lost its exec bit would let the real gh reach GitHub; the invalid host and token
    # make any such call fail.
    assert shutil.which("gh", path=str(tools)) == str(tools / "gh")
    (tmp_path / "jobs").mkdir()
    return {"PATH": f"{tools}{os.pathsep}{os.environ['PATH']}", "STUB_GH_CALLS": str(tmp_path / "gh-calls"),
            "STUB_JOBS": str(tmp_path / "jobs"), "GH_HOST": "invalid.invalid", "GH_TOKEN": "invalid"}


def calls(tmp_path, name="calls"):
    text = (tmp_path / name).read_text()
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
    order = ["Refuse to restage a staged release", "Resolve the release version", "Assert the commit is on main",
             "Rehearse both lines without uploading", "Stage the Flink 2.x line", "Stage the Flink 1.20 LTS line",
             "Hand both deployment IDs to the publication", "Drop dry-run deployments"]
    assert [name for name in names if name in order] == order
    assert names[-1] == "Drop dry-run deployments"


def test_triggers_permissions_and_guards():
    # PyYAML reads the bare `on` key as True.
    triggers = WORKFLOW[True]
    assert triggers["push"] == {"tags": ["v[0-9]+.[0-9]+.[0-9]+"]}
    mode = triggers["workflow_dispatch"]["inputs"]["mode"]
    assert (mode["default"], mode["options"]) == ("rehearse", ["rehearse", "validate"])
    assert WORKFLOW["permissions"] == {} and JOB["permissions"] == {"contents": "read", "actions": "read"}
    assert WORKFLOW["concurrency"] == {"group": "release", "cancel-in-progress": False}
    assert JOB["if"] == "github.repository_id == '1358906035'"
    assert JOB["environment"] == "release"
    # The mirror settings are global, so setup-java's user settings with the central server stay in effect.
    assert JOB["env"] == {"MAVEN_ARGS": "--global-settings ${{ github.workspace }}/.github/maven/settings.xml"}
    assert all(not step.get("uses", "").startswith("./.github/actions/maven") for step in JOB["steps"])
    checkout = JOB["steps"][1]
    assert checkout["uses"].startswith("actions/checkout@") and checkout["with"]["ref"] == "${{ github.sha }}"
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


FIRST = "cbdbc784-e398-4996-8082-b9cbff37460c"
SECOND = "bb77b450-7129-495d-8ad2-fb09fc1bfae2"


def test_publication_follows_successful_staging_in_its_own_jobs():
    assert list(JOBS) == ["stage", "publish-central", "github-release"]
    publish, release = JOBS["publish-central"], JOBS["github-release"]
    # A bare event guard keeps Actions' implicit success() over every need, so neither job starts
    # unless staging, and then Publish, succeeded; a dispatch never reaches either.
    assert publish["if"] == release["if"] == PUSH
    assert publish["needs"] == "stage" and release["needs"] == "publish-central"
    for name, job in JOBS.items():
        for step in job["steps"]:
            assert "if" not in step or name == "stage", (name, step)
    staging = str(JOB["steps"])
    for later in ("central-portal publish", "gh release"):
        assert later not in staging, later


@pytest.mark.parametrize("ids", [(FIRST, SECOND), (FIRST.upper(), SECOND)])
def test_ids_reach_the_publication_only_as_two_lower_case_uuids(tmp_path, ids):
    hand = STEPS["Hand both deployment IDs to the publication"]
    assert hand["if"] == PUSH and hand["id"] == "ids"
    (tmp_path / "central-2x.id").write_text(ids[0] + "\n")
    (tmp_path / "central-1x.id").write_text(ids[1] + "\n")
    output = tmp_path / "output"
    result = run(hand, tmp_path, RUNNER_TEMP=str(tmp_path), GITHUB_OUTPUT=str(output))
    if ids[0] == FIRST:
        assert result.returncode == 0, result.stderr
        assert output.read_text() == f"deployment-2x={FIRST}\ndeployment-1x={SECOND}\n"
    else:
        # capture writes lower case; anything else did not come from it.
        assert result.returncode != 0
        assert not output.exists()


@pytest.mark.parametrize("first, second", [
    (FIRST + "\n" + SECOND + "\n", SECOND + "\n"),
    ("", SECOND + "\n"),  # capture creates the file before Maven uploads anything
    (FIRST + "\n", None),
    (None, None),
    (FIRST + "\n", "x=1\n"),
    (FIRST + "\nother=value\n", SECOND + "\n")])
def test_a_line_without_one_captured_id_writes_no_output(tmp_path, first, second):
    for file, text in (("central-2x.id", first), ("central-1x.id", second)):
        if text is not None:
            (tmp_path / file).write_text(text)
    output = tmp_path / "output"
    result = run(STEPS["Hand both deployment IDs to the publication"], tmp_path, RUNNER_TEMP=str(tmp_path),
                 GITHUB_OUTPUT=str(output))
    assert result.returncode != 0
    assert not output.exists()


def test_stage_outputs_carry_both_ids_to_the_publication(tmp_path, just):
    assert JOB["outputs"] == {"deployment-2x": "${{ steps.ids.outputs.deployment-2x }}",
                              "deployment-1x": "${{ steps.ids.outputs.deployment-1x }}"}
    publish = named_steps(JOBS["publish-central"])["Publish or wait for both Central deployments"]
    assert publish["env"]["DEPLOYMENT_2X"] == "${{ needs.stage.outputs.deployment-2x }}"
    assert publish["env"]["DEPLOYMENT_1X"] == "${{ needs.stage.outputs.deployment-1x }}"
    result = run(publish, tmp_path, **just, DEPLOYMENT_2X=FIRST, DEPLOYMENT_1X=SECOND)
    assert result.returncode == 0, result.stderr
    assert calls(tmp_path) == [["central-portal", "publish", FIRST, SECOND]]
    # A timed-out or failed publication fails the job, which "Re-run failed jobs" repeats.
    assert run(publish, tmp_path, **just, STUB_EXIT="1", DEPLOYMENT_2X=FIRST, DEPLOYMENT_1X=SECOND).returncode == 1


# Each publication job's whole program. A new step must be added here deliberately, after
# checking that it stages, signs, and uploads nothing to Central.
PUBLICATION_JOBS = {
    "publish-central": (["actions/checkout", "jdx/mise-action", "taiki-e/install-action", "run"],
                        ["Publish or wait for both Central deployments"], {"contents": "read"}),
    "github-release": (["run"], ["Create the GitHub Release"], {"contents": "write"}),
}


@pytest.mark.parametrize("name", PUBLICATION_JOBS)
def test_publication_jobs_reach_no_staging_signing_or_upload(name):
    job = JOBS[name]
    kinds, names, permissions = PUBLICATION_JOBS[name]
    # Every step, named or not: an unnamed run step counts as much as a named one.
    assert [step["uses"].split("@")[0] if "uses" in step else "run" for step in job["steps"]] == kinds
    assert [step["name"] for step in job["steps"] if "run" in step] == names
    assert job["permissions"] == permissions
    assert "env" not in job
    for step in job["steps"]:
        text = str(step)
        for forbidden in ("release-stage", "release-rehearse", "capture", "drop", "mvn", "MAVEN_GPG", "setup-java"):
            assert forbidden not in text, (name, forbidden)
        # Maven's deploy goal; the deployment-* outputs are not it.
        assert not re.search(r"\bdeploy\b", text), name
        if step.get("uses", "").startswith("actions/checkout@"):
            assert step["with"] == {"ref": "${{ github.sha }}", "persist-credentials": False}


VIEW = ["release", "view", "v1.0.0", "--repo", "flink-gcp/flink-datastream-protobuf", "--json", "isDraft,isPrerelease",
        "--jq", ".isDraft or .isPrerelease"]
CREATE = ["release", "create", "v1.0.0", "--repo", "flink-gcp/flink-datastream-protobuf", "--verify-tag",
          "--generate-notes", "--title", "v1.0.0"]


def run_release(tmp_path, gh, **env):
    step = named_steps(JOBS["github-release"])["Create the GitHub Release"]
    assert step["env"] == {"GH_TOKEN": "${{ github.token }}"}
    return run(step, tmp_path, **{**gh, **env}, GITHUB_REF_NAME="v1.0.0",
               GITHUB_REPOSITORY="flink-gcp/flink-datastream-protobuf")


@pytest.mark.parametrize("exists", [False, True])
def test_the_github_release_is_created_once(tmp_path, gh, exists):
    result = run_release(tmp_path, gh, STUB_RELEASE_VIEW="0" if exists else "1", STUB_RELEASE_UNSTABLE="false")
    assert result.returncode == 0, result.stderr
    # A re-run after a Release that an earlier attempt published leaves it as it is.
    assert calls(tmp_path, "gh-calls") == ([VIEW] if exists else [VIEW, CREATE])


def test_a_draft_or_prerelease_stops_the_job(tmp_path, gh):
    # The stub prints what gh's --jq filter over isDraft and isPrerelease would print; VIEW pins the filter.
    result = run_release(tmp_path, gh, STUB_RELEASE_VIEW="0", STUB_RELEASE_UNSTABLE="true")
    assert result.returncode == 1
    assert "is a draft or a prerelease" in result.stderr
    assert calls(tmp_path, "gh-calls") == [VIEW]


def test_a_failed_release_creation_fails_the_job(tmp_path, gh):
    # Otherwise the run would conclude success without a Release, and the CI hook would publish the docs.
    result = run_release(tmp_path, gh, STUB_RELEASE_VIEW="1", STUB_RELEASE_CREATE="5")
    assert result.returncode == 5
    assert calls(tmp_path, "gh-calls") == [VIEW, CREATE]


def run_guard(tmp_path, gh, attempt, conclusions, **env):
    for number, conclusion in conclusions.items():
        (tmp_path / "jobs" / number).write_text(conclusion + "\n")
    guard = STEPS["Refuse to restage a staged release"]
    return run(guard, tmp_path, **gh, **env, GITHUB_RUN_ATTEMPT=attempt, GITHUB_RUN_ID="41269811182",
               GITHUB_REPOSITORY="flink-gcp/flink-datastream-protobuf", STAGE_JOB=guard["env"]["STAGE_JOB"])


def test_rerun_guard_matches_this_job_and_runs_first():
    guard = STEPS["Refuse to restage a staged release"]
    # First of all steps, named or not: before the checkout and anything signed.
    assert JOB["steps"][0] == guard
    assert guard["if"] == "github.event_name == 'push' && github.run_attempt != '1'"
    # A renamed job would make the guard match nothing and pass silently.
    assert guard["env"] == {"GH_TOKEN": "${{ github.token }}", "STAGE_JOB": JOB["name"]}


@pytest.mark.parametrize("attempt, conclusions, staged", [
    # Re-run all jobs straight after a successful staging.
    ("2", {"1": "success"}, "1"),
    # Re-run failed jobs after that re-run was refused: attempt 2's failure must not hide attempt 1's success.
    ("3", {"1": "success", "2": "failure"}, "1"),
    # Re-run all jobs after a Re-run failed jobs attempt, whose job list carries the reused stage as a success.
    ("3", {"1": "success", "2": "success"}, "1"),
    ("4", {"1": "failure", "2": "success", "3": "failure"}, "2"),
    # Two records with the job's name must not hide the successful one.
    ("2", {"1": "failure\nsuccess"}, "1")])
def test_rerun_guard_refuses_after_any_successful_staging(tmp_path, gh, attempt, conclusions, staged):
    result = run_guard(tmp_path, gh, attempt, conclusions)
    assert result.returncode == 1
    assert f"Attempt {staged} already staged this release" in result.stderr and "by hand" in result.stderr
    assert calls(tmp_path, "gh-calls")[0] == [
        "api", "--paginate", "repos/flink-gcp/flink-datastream-protobuf/actions/runs/41269811182/attempts/1/jobs",
        "--jq", ".jobs[] | select(.name == env.STAGE_JOB) | .conclusion"]


@pytest.mark.parametrize("attempt, conclusions", [
    ("2", {"1": "failure"}), ("2", {"1": "cancelled"}), ("2", {}), ("3", {"1": "failure", "2": "cancelled"})])
def test_rerun_guard_lets_a_stage_that_never_succeeded_run_again(tmp_path, gh, attempt, conclusions):
    result = run_guard(tmp_path, gh, attempt, conclusions)
    assert result.returncode == 0, result.stderr
    queried = [call[2].rsplit("/", 2)[-2] for call in calls(tmp_path, "gh-calls")]
    assert queried == [str(number) for number in range(1, int(attempt))]


def test_rerun_guard_fails_closed_when_github_does_not_answer(tmp_path, gh):
    assert run_guard(tmp_path, gh, "2", {}, STUB_GH_EXIT="1").returncode != 0


def test_job_limits_cover_their_waits():
    # A job stopped by timeout-minutes cannot report a timed-out wait, so each limit holds the waits
    # its job runs, inside the hosted runners' 360 minutes.
    portal = load_script("central-portal.py")
    validation = 30  # central-publishing-maven-plugin's waitMaxTime default
    assert portal.WAIT_TIMEOUT // 60 == 180
    assert 2 * validation + 15 < JOB["timeout-minutes"] <= 360
    # Setup and the helper's last polling round need minutes beyond the wait itself.
    assert portal.WAIT_TIMEOUT // 60 + 10 <= JOBS["publish-central"]["timeout-minutes"] <= 360


def test_docs_follow_a_successful_tag_run():
    # A successful re-run attempt completes the run with event push, which is what the CI
    # documentation hook publishes on; a rename here would unhook it silently.
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yaml").read_text())
    assert ci[True]["workflow_run"] == {"workflows": [WORKFLOW["name"]], "types": ["completed"]}
    guard = "github.event.workflow_run.conclusion == 'success' && github.event.workflow_run.event == 'push'"
    assert guard in ci["jobs"]["source"]["if"] and guard in ci["concurrency"]["group"]


def test_only_staging_publication_and_cleanup_hold_central_credentials():
    assert "secrets." not in str(WORKFLOW.get("env", {})) + str(JOB.get("env", {}))
    # Every step, named or not, in any field: env, with, or run.
    holders = {step.get("name", step.get("uses")) for step in JOB["steps"] if "secrets.CENTRAL_TOKEN" in str(step)}
    assert holders == {"Stage the Flink 2.x line", "Stage the Flink 1.20 LTS line", "Drop dry-run deployments"}
    assert {STEPS[name]["if"] for name in holders} == {NOT_REHEARSAL, DROP}
    signers = {step.get("name", step.get("uses")) for step in JOB["steps"] if "secrets.MAVEN_GPG" in str(step)}
    assert signers == {"Rehearse both lines without uploading", "Stage the Flink 2.x line", "Stage the Flink 1.20 LTS line"}
    token = {step.get("name", step.get("uses")) for step in JOB["steps"] if "github.token" in str(step)}
    assert token == {"Refuse to restage a staged release"}
    publish = JOBS["publish-central"]
    assert publish["environment"] == "release"
    assert [step["name"] for step in publish["steps"] if "secrets." in str(step)] == [
        "Publish or wait for both Central deployments"]
    assert "github.token" not in str(publish)
    # No job holds both a release secret and a write scope.
    release = JOBS["github-release"]
    assert "environment" not in release and "secrets." not in str(release)
    for name, job in JOBS.items():
        assert not ("write" in job["permissions"].values() and "secrets." in str(job)), name
