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

"""Offline tests: a recording gcloud double replaces every cloud operation."""

import io
import json
from pathlib import Path
import sys
import tarfile

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gce_benchmark as tool

SOURCE = "a" * 40
BASE = "b" * 40
SUITE = 'throw new IllegalArgumentException("Usage: BenchmarkSuite <check|smoke|run> <dir>");'
RECIPES = "    case \"$version\" in\n        1.20.4) adapter=flink1 ;;\n        2.2.1|2.3.0) adapter=flink2 ;;\n"
PB4, PB3 = "issue42-test-pb4", "issue42-test-pb3"
GROUPED = [{"protobuf": 4, "variants": [{"name": "a1", "vm": "a", "patch": "output.patch"},
                                        {"name": "b1", "vm": "b", "patch": "output.patch"},
                                        {"name": "a2", "vm": "a", "patch": "output.patch"},
                                        {"name": "base", "patch": "output.patch"}]}]
CHAIN = [{"protobuf": 4, "variants": [{"name": "baseline", "patch": "output.patch"}, {"name": "descriptors"},
                                      {"name": "candidate", "patch": "output.patch"}, {"name": "floor"}],
          "comparisons": [["baseline", "descriptors"], ["descriptors", "candidate"], ["candidate", "floor"]]},
         {"protobuf": 3}]
MANIFEST = (b"gce.machine.type=c3-standard-8\ngce.cpu.platform=Intel Sapphire Rapids\n"
            b"gce.image=ubuntu\nkernel=Linux 7\njava.version=openjdk 17\n")


def matches(resource, expression):
    """Evaluate the label and name filters this tool sends, joined by AND or OR."""
    def term(text):
        key, value = text.split("=", 1)
        if key == "name":
            return resource["name"] == value
        return resource.get("labels", {}).get(key.removeprefix("labels.")) == value
    if " OR " in expression:
        return any(term(part) for part in expression.split(" OR "))
    return all(term(part) for part in expression.split(" AND "))


class FakeGcloud:
    """Models instances, their boot disks, label filters, uploads, and downloads."""

    def __init__(self, instances=(), runner="SUCCEEDED\nboot\nboot\n3\n", fail_on=None, error=None,
                 evidence=None, quotas=None, project_quotas=(), keep_disks=False, fail_after=False):
        self.calls = []
        self.instances = {}
        self.disks = set()
        for name, status, run in instances:
            self.instances[name] = {"name": name, "status": status,
                                    "labels": {"run": run, "purpose": tool.PURPOSE}}
            self.disks.add(name)
        self.runner = runner
        self.fail_on = fail_on
        self.error = error or tool.Failure("synthetic failure")
        self.evidence = evidence
        self.quotas = quotas or [{"metric": "C3_CPUS", "usage": 0.0, "limit": 24.0},
                                 {"metric": "CPUS", "usage": 0.0, "limit": 100.0}]
        self.project_quotas = list(project_quotas)
        self.keep_disks = keep_disks
        self.fail_after = fail_after
        self.uploads = []

    def __call__(self, *args, check=True):
        self.calls.append(args)
        if self.fail_on and self.fail_on(args) and not self.fail_after:
            raise self.error
        result = self.apply(*args)
        if self.fail_on and self.fail_on(args):
            # For example, a create request that times out after the instance exists.
            raise self.error
        return result

    def apply(self, *args):
        if args[:3] == ("compute", "instances", "create"):
            labels = dict(item.split("=") for item in args[args.index("--labels") + 1].split(","))
            self.instances[args[3]] = {"name": args[3], "status": "RUNNING", "labels": labels}
            self.disks.add(args[3])
        elif args[:3] == ("compute", "instances", "delete"):
            for name in args[3:args.index("--project")]:
                self.instances.pop(name, None)
                if not self.keep_disks:
                    self.disks.discard(name)
        elif args[:2] == ("compute", "ssh") and "boot_id" in args[-1]:
            return self.runner
        elif args[:2] == ("compute", "scp"):
            source, target = args[2], args[3]
            if ":" in source:
                evidence = self.evidence
                Path(target).write_bytes(evidence[source.split(":")[0]] if isinstance(evidence, dict) else evidence)
            else:
                self.uploads.append(Path(source).read_bytes())
        return ""

    def ok(self, *args):
        self.calls.append(args)
        return True

    def json(self, *args):
        self.calls.append(args)
        expression = args[args.index("--filter") + 1] if "--filter" in args else ""
        if args[:3] == ("compute", "images", "describe-from-family"):
            return {"name": "ubuntu-2404-noble-amd64-v20260918", "id": 7}
        if args[:3] == ("compute", "machine-types", "describe"):
            return {"guestCpus": 8, "memoryMb": 32768}
        if args[:3] == ("compute", "regions", "describe"):
            return {"quotas": self.quotas}
        if args[:3] == ("compute", "project-info", "describe"):
            return {"quotas": self.project_quotas}
        if args[:3] == ("compute", "instances", "list"):
            return [dict(i) for i in self.instances.values() if matches(i, expression)]
        if args[:3] == ("compute", "disks", "list"):
            return [{"name": d} for d in sorted(self.disks) if matches({"name": d}, expression)]
        raise AssertionError(args)

    def created(self):
        return [c for c in self.calls if c[:3] == ("compute", "instances", "create")]

    def deleted(self):
        return [name for c in self.calls if c[:3] == ("compute", "instances", "delete")
                for name in c[3:c.index("--project")]]


def evidence_archive(files):
    """Lay files out as the runner does: results/<result>/[benchmark/] plus the VM's maintenance log."""
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as tar:
        for name, data in files.items():
            result, base = name.split("/")
            if base == "maintenance-events.log":
                path = "state/" + base
            elif base.endswith(".json") or base.endswith(".csv"):
                path = "results/" + result + "/benchmark/" + base
            else:
                path = "results/" + result + "/" + base
            info = tarfile.TarInfo(path)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return stream.getvalue()


def results(*names):
    """One VM's archive with a minimal passing result directory per name."""
    return evidence_archive({result + "/" + base: data for result in names for base, data in (
        ("console.log", b"synthetic\n"), ("vm-manifest.txt", MANIFEST), ("maintenance-events.log", b"t NONE\n"))})


def tool_csv():
    return b"lane,workload,operation,deterministic,block,ns_per_op,derived_ops_per_second,allocated_bytes_per_op\n"


class Workspace:
    """A temporary root with the default specification, a stubbed git, and the planning helpers."""

    def __init__(self, root):
        self.root = root
        self.ancestor = True
        self.suite = SUITE
        (root / "output.patch").write_text("--- a/x\n+++ b/x\n", encoding="utf-8")
        self.spec = {"runId": "issue42-test", "mode": "run", "source": "HEAD",
                     "profiles": [{"protobuf": 4, "variants": [{"name": "baseline", "patch": "output.patch"},
                                                               {"name": "candidate"}]},
                                  {"protobuf": 3}]}
        self.env = {"GOOGLE_CLOUD_PROJECT": "example-project", "GCE_BENCHMARK_ZONE": "asia-northeast1-b"}

    def fake_git(self, *args):
        if args[0] == "bundle":
            Path(args[2]).write_bytes(b"# v2 git bundle\n")
        if args[0] == "merge-base":
            return BASE
        if args[0] == "rev-parse":
            return BASE if args[-1].startswith(BASE) else SOURCE
        if args[0] == "show":
            return self.suite if args[1].endswith("BenchmarkSuite.java") else RECIPES
        return ""

    def planned(self, gcloud=None, name="plan", **changes):
        path = self.root / (name + ".json")
        path.write_text(json.dumps({**self.spec, **changes}), encoding="utf-8")
        tool.plan(path, self.root / name, self.env, gcloud or FakeGcloud())
        return self.root / name

    def executed(self, gcloud):
        plan = self.planned()
        tool.execute(plan, True, gcloud, delay=0)
        return plan


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    """A Workspace whose git and git_ok replace the tool's; tool output is left to pytest's capture."""
    workspace = Workspace(tmp_path)
    monkeypatch.setattr(tool, "git", workspace.fake_git)
    monkeypatch.setattr(tool, "git_ok", lambda *a: workspace.ancestor)
    return workspace


def events(plan):
    return [json.loads(line)["event"] for line in (plan / "resources.jsonl").read_text().splitlines()]


def test_plan_records_exact_configuration_without_mutations(workspace):
    gcloud = FakeGcloud()
    plan = tool.read_plan(workspace.planned(gcloud))
    assert gcloud.created() + gcloud.deleted() == []
    assert plan["image"]["name"] == "ubuntu-2404-noble-amd64-v20260918"
    assert [vm["name"] for vm in plan["vms"]] == [PB4, PB3]
    assert [v["name"] for v in plan["vms"][0]["variants"]] == ["baseline", "candidate"]
    assert plan["vms"][1]["variants"] == [{"name": "candidate"}]
    # Without declared comparisons, every pair of variants on one VM is comparable.
    assert [vm["comparisons"] for vm in plan["vms"]] == [[["baseline", "candidate"]], []]
    assert "patchSha256" in plan["vms"][0]["variants"][0]
    assert (plan["source"], plan["base"]) == (SOURCE, BASE)
    assert plan["labels"] == {"purpose": tool.PURPOSE, "run": "issue42-test"}
    assert plan["scheduling"] == {"provisioningModel": "STANDARD", "onHostMaintenance": "TERMINATE",
                                  "automaticRestart": False}
    regions = [c for c in gcloud.calls if c[:3] == ("compute", "regions", "describe")]
    assert regions[0][3] == "asia-northeast1"


@pytest.mark.parametrize("change", [
    {"mode": "Fast"}, {"runId": "Bad_Name"}, {"profiles": [{"protobuf": 5}]},
    {"profiles": [{"protobuf": 4}, {"protobuf": 4}]}, {"extra": True},
    {"profiles": [{"protobuf": 4, "variants": [{"name": "a"}, {"name": "a"}]}]},
    *({"profiles": [{"protobuf": 4, "variants": [{"name": "a", "vm": vm}]}]}
      for vm in ("A", "a-b", "1a", "", "a" * 17, 3, "pb3", "pb4x")),
    {"profiles": [{"protobuf": 4, "variants": [{"name": "a", "vm": "a"},
                                               {"name": "b", "vm": "b", "patch": "b.patch"}]}]},
    {"profiles": [{"protobuf": 4, "variants": [{"name": "a", "vm": "a", "patch": "a.patch"},
                                               {"name": "b", "patch": "b.patch"}]}]},
], ids=["mode-uppercase", "run-id-underscore", "protobuf-5", "duplicate-profile", "unknown-field",
        "duplicate-variant", "vm-uppercase", "vm-hyphen", "vm-leading-digit", "vm-empty", "vm-too-long", "vm-number",
        "vm-pb3", "vm-pb4x", "groups-differ-by-patch", "group-and-ungrouped-differ-by-patch"])
def test_plan_rejects_invalid_specifications(workspace, change):
    with pytest.raises(tool.Failure):
        tool.validate_spec({**workspace.spec, **change})


@pytest.mark.parametrize(("comparisons", "message"), [
    ([], "non-empty list"), (3, "non-empty list"), ({"a": "b"}, "non-empty list"),
    ([["a"], ["b", "c"]], "two distinct variants"), ([["a", "b", "c"]], "two distinct variants"),
    ([["a", "a"], ["b", "c"]], "two distinct variants"), ([["a", "x"], ["b", "c"]], "two distinct variants"),
    ([["a", ["b"]], ["b", "c"]], "two distinct variants"), (["ab", ["b", "c"]], "two distinct variants"),
    ([["a", "b"], ["b", "a"], ["b", "c"]], "b/a is declared twice"),
    ([["a", "b"]], "in no comparison: c"),
], ids=["empty", "number", "mapping", "single", "triple", "same-variant", "unknown-variant", "nested-list",
        "string-pair", "declared-twice", "unused-variant"])
def test_plan_rejects_invalid_comparisons(workspace, comparisons, message):
    variants = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
    profile = {"protobuf": 4, "variants": variants, "comparisons": comparisons}
    with pytest.raises(tool.Failure, match=message):
        tool.validate_spec({**workspace.spec, "profiles": [profile]})


def test_plan_rejects_comparisons_with_vm_groups(workspace):
    # Groups measure one tree on several VMs, so declaring a comparison there would compare across VMs.
    grouped = [{"name": "a", "vm": "a"}, {"name": "b", "vm": "a"}]
    with pytest.raises(tool.Failure, match="cannot be combined with vm groups"):
        workspace.planned(profiles=[{"protobuf": 4, "variants": grouped, "comparisons": [["a", "b"]]}])
    assert not (workspace.root / "plan").exists()


@pytest.mark.parametrize(("change", "instances"), [
    ({"mode": "pair"}, None), ({"flinkVersion": "1.19.0"}, None),
    ({}, [("other", "RUNNING", "issue42-test")]),
], ids=["unsupported-mode", "unsupported-flink", "existing-run"])
def test_plan_checks_the_source_commit_and_existing_runs(workspace, change, instances):
    with pytest.raises(tool.Failure):
        workspace.planned(FakeGcloud(instances=instances) if instances else None, **change)


def test_plan_checks_the_base_ancestry_and_reads_supported_modes(workspace):
    workspace.ancestor = False
    with pytest.raises(tool.Failure):
        workspace.planned()
    workspace.ancestor = True
    workspace.suite = SUITE.replace("smoke|run", "smoke|run|pair")
    assert tool.read_plan(workspace.planned(mode="pair"))["mode"] == "pair"


@pytest.mark.parametrize("quota", [{"metric": "C3_CPUS", "usage": 16.0, "limit": 24.0},
                                   {"metric": "CPUS", "usage": 90.0, "limit": 100.0}], ids=["C3_CPUS", "CPUS"])
def test_plan_checks_regional_and_family_quota_before_writing(workspace, quota):
    with pytest.raises(tool.Failure):
        workspace.planned(FakeGcloud(quotas=[quota]))
    assert not (workspace.root / "plan").exists()


def test_grouped_variants_run_sequentially_on_their_own_vms(workspace):
    plan = tool.read_plan(workspace.planned(profiles=GROUPED))
    assert [(vm["name"], [v["name"] for v in vm["variants"]]) for vm in plan["vms"]] == [
        (PB4 + "-a", ["a1", "a2"]), (PB4 + "-b", ["b1"]), (PB4, ["base"])]
    assert plan["vms"][1]["variants"][0]["patch"] == "protobuf-4-b1.patch"
    assert [vm["comparisons"] for vm in plan["vms"]] == [[["a1", "a2"]], [], []]
    assert [vm["estimatedMinutes"] for vm in plan["vms"]] == [25 + 2 * 370, 25 + 370, 25 + 370]
    assert (plan["estimatedVmHours"], plan["estimatedWallHours"]) == (25.92, 12.75)
    gcloud = FakeGcloud()
    plan = workspace.planned(gcloud, name="grouped", profiles=GROUPED)
    tool.execute(plan, True, gcloud, delay=0)
    assert [create[3] for create in gcloud.created()] == [PB4 + "-a", PB4 + "-b", PB4]
    uploaded = []
    for upload in gcloud.uploads:
        with tarfile.open(fileobj=io.BytesIO(upload)) as tar:
            settings = tar.extractfile("input/run.env").read().decode()
            patches = sorted(name for name in tar.getnames() if name.startswith("input/patches/"))
        uploaded.append((dict(line.split("=", 1) for line in settings.splitlines())["VARIANTS"], patches))
    assert uploaded == [("'a1 a2'", ["input/patches/a1.patch", "input/patches/a2.patch"]),
                        ("b1", ["input/patches/b1.patch"]), ("base", ["input/patches/base.patch"])]


def test_declared_comparisons_run_pairwise_and_repeat_shared_variants(workspace):
    roomy = [{"metric": "C3_CPUS", "usage": 0.0, "limit": 64.0}]
    plan = tool.read_plan(workspace.planned(FakeGcloud(quotas=roomy), profiles=CHAIN))
    assert [(vm["name"], [v["name"] for v in vm["variants"]], vm["comparisons"]) for vm in plan["vms"]] == [
        (PB4 + "-c1", ["baseline", "descriptors"], [["baseline", "descriptors"]]),
        (PB4 + "-c2", ["descriptors", "candidate"], [["descriptors", "candidate"]]),
        (PB4 + "-c3", ["candidate", "floor"], [["candidate", "floor"]]),
        (PB3, ["candidate"], [])]
    # Six variant runs instead of four, but the longest VM measures two instead of four.
    assert [vm["estimatedMinutes"] for vm in plan["vms"]] == [25 + 2 * 370] * 3 + [25 + 370]
    assert (plan["estimatedVmHours"], plan["estimatedWallHours"]) == (44.83, 12.75)
    gcloud = FakeGcloud(quotas=roomy)
    plan = workspace.planned(gcloud, name="chain", profiles=CHAIN)
    tool.execute(plan, True, gcloud, delay=0)
    assert [create[3] for create in gcloud.created()] == [PB4 + "-c1", PB4 + "-c2", PB4 + "-c3", PB3]
    uploaded = []
    for upload in gcloud.uploads:
        with tarfile.open(fileobj=io.BytesIO(upload)) as tar:
            settings = tar.extractfile("input/run.env").read().decode()
            patches = sorted(name for name in tar.getnames() if name.startswith("input/patches/"))
        uploaded.append((dict(line.split("=", 1) for line in settings.splitlines())["VARIANTS"], patches))
    assert uploaded == [("'baseline descriptors'", ["input/patches/baseline.patch"]),
                        ("'descriptors candidate'", ["input/patches/candidate.patch"]),
                        ("'candidate floor'", ["input/patches/candidate.patch"]), ("candidate", [])]


def test_longest_group_and_run_id_fit_the_instance_name_limit(workspace):
    variants = [{"name": "a", "vm": "z" * 16}, {"name": "b", "vm": "y"}]
    plan = tool.read_plan(workspace.planned(runId="r" * 32, profiles=[{"protobuf": 4, "variants": variants}]))
    assert [vm["name"] for vm in plan["vms"]] == ["r" * 32 + "-pb4-" + "z" * 16, "r" * 32 + "-pb4-y"]
    assert max(len(vm["name"]) for vm in plan["vms"]) <= 63


@pytest.mark.parametrize(("regional", "project", "metric"), [
    ([{"metric": "C3_CPUS", "usage": 0.0, "limit": 16.0}], [], "C3_CPUS"),
    ([{"metric": "CPUS", "usage": 80.0, "limit": 100.0}], [], "CPUS"),
    ([{"metric": "INSTANCES", "usage": 22.0, "limit": 24.0}], [], "INSTANCES"),
    # The project also lists a global IN_USE_ADDRESSES, which must not replace the regional one.
    ([{"metric": "IN_USE_ADDRESSES", "usage": 6.0, "limit": 8.0}],
     [{"metric": "IN_USE_ADDRESSES", "usage": 0.0, "limit": 100.0}], "IN_USE_ADDRESSES"),
    ([], [{"metric": "CPUS_ALL_REGIONS", "usage": 16.0, "limit": 32.0}], "CPUS_ALL_REGIONS"),
], ids=["C3_CPUS", "CPUS", "INSTANCES", "IN_USE_ADDRESSES", "CPUS_ALL_REGIONS"])
def test_plan_counts_every_vm_against_cpu_instance_and_address_quota(workspace, regional, project, metric):
    with pytest.raises(tool.Failure, match=metric):
        workspace.planned(FakeGcloud(quotas=regional, project_quotas=project), profiles=GROUPED)
    assert not (workspace.root / "plan").exists()


def test_plan_accepts_quota_that_fits_every_vm(workspace):
    regional = [{"metric": "C3_CPUS", "usage": 0.0, "limit": 24.0}, {"metric": "CPUS", "usage": 76.0, "limit": 100.0},
                {"metric": "INSTANCES", "usage": 21.0, "limit": 24.0},
                {"metric": "IN_USE_ADDRESSES", "usage": 5.0, "limit": 8.0}]
    project = [{"metric": "CPUS_ALL_REGIONS", "usage": 8.0, "limit": 32.0},
               {"metric": "IN_USE_ADDRESSES", "usage": 8.0, "limit": 8.0}]
    gcloud = FakeGcloud(quotas=regional, project_quotas=project)
    assert len(tool.read_plan(workspace.planned(gcloud, profiles=GROUPED))["vms"]) == 3


def test_execute_requires_approval_and_creates_credential_free_vms(workspace):
    plan = workspace.planned()
    gcloud = FakeGcloud()
    with pytest.raises(tool.Failure):
        tool.execute(plan, False, gcloud)
    assert gcloud.created() == []
    tool.execute(plan, True, gcloud, delay=0)
    assert len(gcloud.created()) == 2
    for create in gcloud.created():
        for flag in ("--no-service-account", "--no-scopes", "--no-restart-on-failure",
                     "--boot-disk-auto-delete", "--shielded-secure-boot"):
            assert flag in create
        assert create[create.index("--maintenance-policy") + 1] == "TERMINATE"
        assert create[create.index("--provisioning-model") + 1] == "STANDARD"
        assert "--min-cpu-platform" not in create
        assert create[create.index("--labels") + 1] == "purpose=" + tool.PURPOSE + ",run=issue42-test"
    assert events(plan) == ["create-requested", "created"] * 2 + ["started"] * 2
    with pytest.raises(tool.Failure):
        tool.execute(plan, True, gcloud)


def test_planned_cpu_platform_is_requested(workspace):
    plan = tool.read_plan(workspace.planned(minCpuPlatform="Intel Sapphire Rapids"))
    arguments = tool.instance_arguments(plan, plan["vms"][0])
    assert arguments[arguments.index("--min-cpu-platform") + 1] == "Intel Sapphire Rapids"


def test_uploads_contain_only_the_planned_inputs(workspace):
    gcloud = FakeGcloud()
    workspace.executed(gcloud)
    members = []
    for upload in gcloud.uploads:
        with tarfile.open(fileobj=io.BytesIO(upload)) as tar:
            members.append(sorted(tar.getnames()))
            settings = tar.extractfile("input/run.env").read().decode()
        keys = [line.split("=", 1)[0] for line in settings.splitlines()]
        assert keys == ["RUN_ID", "BENCHMARK_MODE", "FLINK_VERSION", "PROTOBUF_MAJOR", "SOURCE_REVISION",
                        "SOURCE_BASE", "VARIANTS", "JAVA_VERSION", "JUST_VERSION", "MISE_VERSION"]
        assert "example-project" not in settings
    common = ["input/gce-benchmark-vm.sh", "input/run.env", "input/source.bundle"]
    assert members == [sorted(common + ["input/patches/baseline.patch"]), common]


def test_execute_never_starts_beside_an_existing_run(workspace):
    plan = workspace.planned()
    gcloud = FakeGcloud(instances=[("foreign", "RUNNING", "issue42-test")])
    with pytest.raises(tool.Failure):
        tool.execute(plan, True, gcloud)
    assert (gcloud.created(), gcloud.deleted()) == ([], [])
    assert "foreign" in gcloud.instances


@pytest.mark.parametrize(("fail_on", "error", "after", "expected"), [
    (lambda a: a[:3] == ("compute", "instances", "create") and a[3] == PB3, None, False, [PB4]),
    (lambda a: a[:3] == ("compute", "instances", "create") and a[3] == PB4, None, True, [PB4]),
    (lambda a: a[:2] == ("compute", "scp"), KeyboardInterrupt(), False, [PB4, PB3]),
], ids=["second-create-fails", "create-fails-after-the-instance-exists", "upload-interrupted"])
def test_failed_or_interrupted_start_deletes_every_created_instance(workspace, fail_on, error, after, expected):
    plan = workspace.planned()
    gcloud = FakeGcloud(fail_on=fail_on, error=error, fail_after=after)
    with pytest.raises(type(error) if error else tool.Failure):
        tool.execute(plan, True, gcloud, delay=0)
    assert sorted(gcloud.deleted()) == sorted(expected)
    assert events(plan)[-1] == "cleanup-verified"


@pytest.mark.parametrize(("instances", "runner", "expected"), [
    ({}, "ok\n", ("ABSENT", "UNKNOWN")),
    ({PB4: "RUNNING"}, "RUNNING baseline\n", ("RUNNING", "UNKNOWN")),
    ({PB4: "TERMINATED"}, "", ("TERMINATED", "INTERRUPTED instance TERMINATED")),
    ({PB4: "RUNNING"}, "RUNNING baseline\nold\nnew\n1\n", ("RUNNING", "INTERRUPTED reboot")),
    ({PB4: "RUNNING"}, "RUNNING baseline\nboot\nboot\n7\n", ("RUNNING", "RUNNING baseline")),
], ids=["absent", "unknown-runner", "terminated", "rebooted", "running"])
def test_status_reports_absent_unknown_stopped_and_rebooted_instances(workspace, instances, runner, expected):
    plan = tool.read_plan(workspace.planned())
    vm = plan["vms"][0]
    state = tool.vm_state(FakeGcloud(runner=runner), plan, vm, instances)
    assert (state["instance"], state["runner"]) == expected


GOOD = {"protobuf-4-candidate/console.log": b"synthetic\n",
        "protobuf-4-candidate/measurements.csv": tool_csv(),
        "protobuf-4-candidate/vm-manifest.txt": MANIFEST,
        "protobuf-4-candidate/maintenance-events.log": b"2026-01-01T00:00:00Z NONE\n"}


@pytest.mark.parametrize(("files", "failure"), [
    (GOOD, None),
    ({**GOOD, "protobuf-4-candidate/unknown.bin": b"x"}, PB4 + "/protobuf-4-candidate: evidence gate"),
    ({**GOOD, "protobuf-4-candidate/maintenance-events.log": b"t NONE\nt TERMINATE_ON_HOST_MAINTENANCE\n"},
     PB4 + "/protobuf-4-candidate: host maintenance"),
    ({}, "no result directories"),
    ({**GOOD, "protobuf-4-candidate/vm-manifest.txt": MANIFEST + b"x\n",
      "protobuf-4-other/vm-manifest.txt": MANIFEST.replace(b"Sapphire", b"Emerald"),
      "protobuf-4-other/console.log": b"synthetic\n"}, "gce.cpu.platform"),
    (None, "download failed"),
], ids=["ok", "gate", "maintenance", "empty", "uncontrolled", "download"])
def test_collect_gates_results_and_always_cleans_up(workspace, files, failure):
    plan = workspace.planned(profiles=[{"protobuf": 4}])
    gcloud = FakeGcloud(instances=[(PB4, "RUNNING", "issue42-test")],
                        evidence=evidence_archive(files) if files is not None else None,
                        fail_on=(lambda a: a[:2] == ("compute", "scp")) if files is None else None)
    output = workspace.root / "collected"
    if failure:
        with pytest.raises(tool.Failure, match=failure):
            tool.collect(plan, output, gcloud)
    else:
        tool.collect(plan, output, gcloud)
        checked = output / "checked" / PB4 / "protobuf-4-candidate"
        assert (checked / "inventory.json").is_file()
        # JMH output from benchmark/ and the VM maintenance log are flattened beside run records.
        assert sorted(p.name for p in (checked / "evidence").iterdir()) == [
            "console.log", "maintenance-events.log", "measurements.csv", "vm-manifest.txt"]
    assert gcloud.deleted() == [PB4]
    assert events(plan)[-1] == "cleanup-verified"


@pytest.mark.parametrize(("other", "failure"), [
    (MANIFEST, None), (MANIFEST.replace(b"Linux 7", b"Linux 8"), "kernel"),
], ids=["same", "differs"])
def test_collect_records_placement_and_checks_hosts_across_vms(workspace, other, failure):
    groups = [{"protobuf": 4, "variants": [{"name": "a1", "vm": "a"}, {"name": "b1", "vm": "b"}]}]
    running = [(PB4 + "-a", "RUNNING", "issue42-test"), (PB4 + "-b", "RUNNING", "issue42-test")]

    def files(result, manifest):
        return evidence_archive({result + "/console.log": b"synthetic\n", result + "/vm-manifest.txt": manifest,
                                 result + "/maintenance-events.log": b"t NONE\n"})
    plan = workspace.planned(profiles=groups)
    gcloud = FakeGcloud(instances=running, evidence={PB4 + "-a": files("protobuf-4-a1", MANIFEST),
                                                     PB4 + "-b": files("protobuf-4-b1", other)})
    output = workspace.root / "collected"
    if failure:
        with pytest.raises(tool.Failure, match=failure):
            tool.collect(plan, output, gcloud)
    else:
        tool.collect(plan, output, gcloud)
    record = json.loads((output / "collection.json").read_text())
    assert record["results"] == {PB4 + "-a": ["protobuf-4-a1"], PB4 + "-b": ["protobuf-4-b1"]}
    assert record["comparisons"] == {PB4 + "-a": [], PB4 + "-b": []}
    assert sorted(gcloud.deleted()) == sorted(name for name, _, _ in running)


def test_collect_keeps_a_repeated_variant_apart_on_each_vm(workspace):
    profiles = [{"protobuf": 3, "variants": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
                 "comparisons": [["a", "b"], ["b", "c"]]}]
    running = [(PB3 + "-c1", "RUNNING", "issue42-test"), (PB3 + "-c2", "RUNNING", "issue42-test")]
    plan = workspace.planned(profiles=profiles)
    gcloud = FakeGcloud(instances=running, evidence={PB3 + "-c1": results("protobuf-3-a", "protobuf-3-b"),
                                                     PB3 + "-c2": results("protobuf-3-b", "protobuf-3-c")})
    output = workspace.root / "collected"
    tool.collect(plan, output, gcloud)
    assert sorted(str(path.relative_to(output / "checked")) for path in (output / "checked").glob("*/*")) == [
        PB3 + "-c1/protobuf-3-a", PB3 + "-c1/protobuf-3-b", PB3 + "-c2/protobuf-3-b", PB3 + "-c2/protobuf-3-c"]
    record = json.loads((output / "collection.json").read_text())
    assert record["results"] == {PB3 + "-c1": ["protobuf-3-a", "protobuf-3-b"],
                                 PB3 + "-c2": ["protobuf-3-b", "protobuf-3-c"]}
    assert record["comparisons"] == {PB3 + "-c1": [["protobuf-3-a", "protobuf-3-b"]],
                                     PB3 + "-c2": [["protobuf-3-b", "protobuf-3-c"]]}
    assert sorted(gcloud.deleted()) == sorted(name for name, _, _ in running)


@pytest.mark.parametrize("name", ["current", "older"])
def test_collect_records_every_pair_of_a_sequential_vm_including_older_plans(workspace, name):
    plan = workspace.planned(name=name, profiles=workspace.spec["profiles"][:1])
    if name == "older":
        data = tool.read_plan(plan)
        del data["vms"][0]["comparisons"]
        (plan / "plan.json").write_text(json.dumps(data), encoding="utf-8")
    gcloud = FakeGcloud(instances=[(PB4, "RUNNING", "issue42-test")],
                        evidence={PB4: results("protobuf-4-baseline", "protobuf-4-candidate")})
    output = workspace.root / (name + "-collected")
    tool.collect(plan, output, gcloud)
    record = json.loads((output / "collection.json").read_text())
    assert record["comparisons"] == {PB4: [["protobuf-4-baseline", "protobuf-4-candidate"]]}


RUNNING = [(PB4, "RUNNING", "issue42-test"), (PB3, "RUNNING", "issue42-test")]


def test_collect_refuses_incomplete_runs_unless_partial(workspace):
    plan = workspace.planned()
    gcloud = FakeGcloud(instances=RUNNING, runner="RUNNING candidate\nboot\nboot\n1\n")
    with pytest.raises(tool.Failure):
        tool.collect(plan, workspace.root / "collected", gcloud)
    assert gcloud.deleted() == []
    assert not (workspace.root / "collected").exists()
    partial = {major: evidence_archive({"protobuf-%d-candidate/console.log" % major: b"partial\n",
                                        "protobuf-%d-candidate/vm-manifest.txt" % major: MANIFEST})
               for major in (3, 4)}
    gcloud = FakeGcloud(instances=RUNNING, runner="FAILED candidate\nboot\nboot\n1\n",
                        evidence={PB4: partial[4], PB3: partial[3]})
    with pytest.raises(tool.Failure, match="diagnosis only"):
        tool.collect(plan, workspace.root / "partial", gcloud, partial=True)
    assert (workspace.root / "partial" / "checked" / PB4 / "protobuf-4-candidate").is_dir()
    assert sorted(gcloud.deleted()) == sorted([PB4, PB3])


@pytest.mark.parametrize("runner", ["RUNNING candidate\nboot\nboot\n1\n", "unreadable\n"],
                         ids=["running", "unreadable"])
def test_collect_partial_refuses_active_runners(workspace, runner):
    plan = workspace.planned()
    gcloud = FakeGcloud(instances=RUNNING, runner=runner)
    with pytest.raises(tool.Failure, match="still active"):
        tool.collect(plan, workspace.root / "active", gcloud, partial=True)
    assert gcloud.deleted() == []
    assert not (workspace.root / "active").exists()


def test_cleanup_deletes_in_one_request_and_reports_remaining_disks(workspace):
    plan = workspace.planned()
    running = [(PB4, "RUNNING", "issue42-test"), (PB3, "RUNNING", "issue42-test"),
               ("other-run", "RUNNING", "other")]
    gcloud = FakeGcloud(instances=running)
    tool.cleanup(plan, gcloud)
    deletes = [c for c in gcloud.calls if c[:3] == ("compute", "instances", "delete")]
    assert len(deletes) == 1
    assert sorted(gcloud.deleted()) == sorted([PB4, PB3])
    assert list(gcloud.instances) == ["other-run"]
    stuck = FakeGcloud(instances=running[:1], keep_disks=True)
    with pytest.raises(tool.Failure, match=PB4):
        tool.cleanup(plan, stuck)
    assert events(plan)[-1] == "cleanup-incomplete"


def test_env_file_exposes_only_reviewed_keys_and_the_environment_wins(tmp_path):
    path = tmp_path / ".env"
    path.write_text("# comment\nCLOUDSDK_CONFIG=/private/config\nGOOGLE_APPLICATION_CREDENTIALS=/key.json\n"
                    "GOOGLE_CLOUD_PROJECT='example'\n", encoding="utf-8")
    assert tool.load_env(path, {}) == {"CLOUDSDK_CONFIG": "/private/config", "GOOGLE_CLOUD_PROJECT": "example"}
    assert tool.load_env(path, {"GOOGLE_CLOUD_PROJECT": "override"})["GOOGLE_CLOUD_PROJECT"] == "override"
    path.write_text("not an assignment\n", encoding="utf-8")
    with pytest.raises(tool.Failure):
        tool.load_env(path, {})


@pytest.mark.parametrize(("name", "kind"), [("../escape", tarfile.REGTYPE), ("/absolute", tarfile.REGTYPE),
                                            ("evidence/link", tarfile.SYMTYPE)],
                         ids=["traversal", "absolute", "symlink"])
def test_safe_extract_rejects_links_traversal_and_absolute_paths(tmp_path, name, kind):
    archive = tmp_path / "unsafe.tar"
    with tarfile.open(archive, "w") as tar:
        info = tarfile.TarInfo(name)
        info.type = kind
        tar.addfile(info, io.BytesIO(b""))
    with pytest.raises(tool.Failure):
        tool.safe_extract(archive, tmp_path / "out")
    assert not (tmp_path / "out").exists()
