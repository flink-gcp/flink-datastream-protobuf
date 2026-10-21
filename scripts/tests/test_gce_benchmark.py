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

import contextlib
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

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


def tool_csv():
    return b"lane,workload,operation,deterministic,block,ns_per_op,derived_ops_per_second,allocated_bytes_per_op\n"


class GceBenchmarkTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.ancestor = True
        self.suite = SUITE
        for name, replacement in (("git", self.fake_git), ("git_ok", lambda *a: self.ancestor)):
            patcher = patch.object(tool, name, side_effect=replacement)
            patcher.start()
            self.addCleanup(patcher.stop)
        (self.root / "output.patch").write_text("--- a/x\n+++ b/x\n", encoding="utf-8")
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
        with contextlib.redirect_stdout(io.StringIO()):
            tool.plan(path, self.root / name, self.env, gcloud or FakeGcloud())
        return self.root / name

    def executed(self, gcloud):
        plan = self.planned()
        with contextlib.redirect_stdout(io.StringIO()):
            tool.execute(plan, True, gcloud, delay=0)
        return plan

    def events(self, plan):
        return [json.loads(line)["event"] for line in (plan / "resources.jsonl").read_text().splitlines()]

    def test_plan_records_exact_configuration_without_mutations(self):
        gcloud = FakeGcloud()
        plan = tool.read_plan(self.planned(gcloud))
        self.assertEqual([], gcloud.created() + gcloud.deleted())
        self.assertEqual("ubuntu-2404-noble-amd64-v20260918", plan["image"]["name"])
        self.assertEqual([PB4, PB3], [vm["name"] for vm in plan["vms"]])
        self.assertEqual(["baseline", "candidate"], [v["name"] for v in plan["vms"][0]["variants"]])
        self.assertEqual([{"name": "candidate"}], plan["vms"][1]["variants"])
        self.assertIn("patchSha256", plan["vms"][0]["variants"][0])
        self.assertEqual((SOURCE, BASE), (plan["source"], plan["base"]))
        self.assertEqual({"purpose": tool.PURPOSE, "run": "issue42-test"}, plan["labels"])
        self.assertEqual({"provisioningModel": "STANDARD", "onHostMaintenance": "TERMINATE",
                          "automaticRestart": False}, plan["scheduling"])
        regions = [c for c in gcloud.calls if c[:3] == ("compute", "regions", "describe")]
        self.assertEqual("asia-northeast1", regions[0][3])

    def test_plan_rejects_invalid_specifications(self):
        for change in ({"mode": "Fast"}, {"runId": "Bad_Name"}, {"profiles": [{"protobuf": 5}]},
                       {"profiles": [{"protobuf": 4}, {"protobuf": 4}]}, {"extra": True},
                       {"profiles": [{"protobuf": 4, "variants": [{"name": "a"}, {"name": "a"}]}]},
                       *({"profiles": [{"protobuf": 4, "variants": [{"name": "a", "vm": vm}]}]}
                         for vm in ("A", "a-b", "1a", "", "a" * 17, 3, "pb3", "pb4x")),
                       {"profiles": [{"protobuf": 4, "variants": [{"name": "a", "vm": "a"},
                                                                  {"name": "b", "vm": "b", "patch": "b.patch"}]}]},
                       {"profiles": [{"protobuf": 4, "variants": [{"name": "a", "vm": "a", "patch": "a.patch"},
                                                                  {"name": "b", "patch": "b.patch"}]}]}):
            with self.subTest(change=change), self.assertRaises(tool.Failure):
                tool.validate_spec({**self.spec, **change})

    def test_plan_checks_the_source_commit_and_existing_runs(self):
        cases = [({"mode": "pair"}, None), ({"flinkVersion": "1.19.0"}, None),
                 ({}, FakeGcloud(instances=[("other", "RUNNING", "issue42-test")]))]
        for change, gcloud in cases:
            with self.subTest(change=change), self.assertRaises(tool.Failure):
                self.planned(gcloud, **change)
        self.ancestor = False
        with self.assertRaises(tool.Failure):
            self.planned()
        self.ancestor = True
        self.suite = SUITE.replace("smoke|run", "smoke|run|pair")
        self.assertEqual("pair", tool.read_plan(self.planned(mode="pair"))["mode"])

    def test_plan_checks_regional_and_family_quota_before_writing(self):
        for quota in ({"metric": "C3_CPUS", "usage": 16.0, "limit": 24.0},
                      {"metric": "CPUS", "usage": 90.0, "limit": 100.0}):
            with self.subTest(metric=quota["metric"]), self.assertRaises(tool.Failure):
                self.planned(FakeGcloud(quotas=[quota]))
            self.assertFalse((self.root / "plan").exists())

    def test_grouped_variants_run_sequentially_on_their_own_vms(self):
        plan = tool.read_plan(self.planned(profiles=GROUPED))
        self.assertEqual([(PB4 + "-a", ["a1", "a2"]), (PB4 + "-b", ["b1"]), (PB4, ["base"])],
                         [(vm["name"], [v["name"] for v in vm["variants"]]) for vm in plan["vms"]])
        self.assertEqual("protobuf-4-b1.patch", plan["vms"][1]["variants"][0]["patch"])
        self.assertEqual([25 + 2 * 300, 25 + 300, 25 + 300], [vm["estimatedMinutes"] for vm in plan["vms"]])
        self.assertEqual((21.25, 10.42), (plan["estimatedVmHours"], plan["estimatedWallHours"]))
        gcloud = FakeGcloud()
        plan = self.planned(gcloud, name="grouped", profiles=GROUPED)
        with contextlib.redirect_stdout(io.StringIO()):
            tool.execute(plan, True, gcloud, delay=0)
        self.assertEqual([PB4 + "-a", PB4 + "-b", PB4], [create[3] for create in gcloud.created()])
        uploaded = []
        for upload in gcloud.uploads:
            with tarfile.open(fileobj=io.BytesIO(upload)) as tar:
                settings = tar.extractfile("input/run.env").read().decode()
                patches = sorted(name for name in tar.getnames() if name.startswith("input/patches/"))
            uploaded.append((dict(line.split("=", 1) for line in settings.splitlines())["VARIANTS"], patches))
        self.assertEqual([("'a1 a2'", ["input/patches/a1.patch", "input/patches/a2.patch"]),
                          ("b1", ["input/patches/b1.patch"]), ("base", ["input/patches/base.patch"])], uploaded)

    def test_longest_group_and_run_id_fit_the_instance_name_limit(self):
        variants = [{"name": "a", "vm": "z" * 16}, {"name": "b", "vm": "y"}]
        plan = tool.read_plan(self.planned(runId="r" * 32, profiles=[{"protobuf": 4, "variants": variants}]))
        self.assertEqual(["r" * 32 + "-pb4-" + "z" * 16, "r" * 32 + "-pb4-y"], [vm["name"] for vm in plan["vms"]])
        self.assertLessEqual(max(len(vm["name"]) for vm in plan["vms"]), 63)

    def test_plan_counts_every_vm_against_cpu_instance_and_address_quota(self):
        # The project also lists a global IN_USE_ADDRESSES, which must not replace the regional one.
        roomy = [{"metric": "IN_USE_ADDRESSES", "usage": 0.0, "limit": 100.0}]
        cases = [([{"metric": "C3_CPUS", "usage": 0.0, "limit": 16.0}], [], "C3_CPUS"),
                 ([{"metric": "CPUS", "usage": 80.0, "limit": 100.0}], [], "CPUS"),
                 ([{"metric": "INSTANCES", "usage": 22.0, "limit": 24.0}], [], "INSTANCES"),
                 ([{"metric": "IN_USE_ADDRESSES", "usage": 6.0, "limit": 8.0}], roomy, "IN_USE_ADDRESSES"),
                 ([], [{"metric": "CPUS_ALL_REGIONS", "usage": 16.0, "limit": 32.0}], "CPUS_ALL_REGIONS")]
        for regional, project, metric in cases:
            with self.subTest(metric=metric), self.assertRaisesRegex(tool.Failure, metric):
                self.planned(FakeGcloud(quotas=regional, project_quotas=project), profiles=GROUPED)
            self.assertFalse((self.root / "plan").exists())
        regional = [{"metric": "C3_CPUS", "usage": 0.0, "limit": 24.0}, {"metric": "CPUS", "usage": 76.0, "limit": 100.0},
                    {"metric": "INSTANCES", "usage": 21.0, "limit": 24.0},
                    {"metric": "IN_USE_ADDRESSES", "usage": 5.0, "limit": 8.0}]
        project = [{"metric": "CPUS_ALL_REGIONS", "usage": 8.0, "limit": 32.0},
                   {"metric": "IN_USE_ADDRESSES", "usage": 8.0, "limit": 8.0}]
        gcloud = FakeGcloud(quotas=regional, project_quotas=project)
        self.assertEqual(3, len(tool.read_plan(self.planned(gcloud, profiles=GROUPED))["vms"]))

    def test_execute_requires_approval_and_creates_credential_free_vms(self):
        plan = self.planned()
        gcloud = FakeGcloud()
        with self.assertRaises(tool.Failure):
            tool.execute(plan, False, gcloud)
        self.assertEqual([], gcloud.created())
        with contextlib.redirect_stdout(io.StringIO()):
            tool.execute(plan, True, gcloud, delay=0)
        self.assertEqual(2, len(gcloud.created()))
        for create in gcloud.created():
            for flag in ("--no-service-account", "--no-scopes", "--no-restart-on-failure",
                         "--boot-disk-auto-delete", "--shielded-secure-boot"):
                self.assertIn(flag, create)
            self.assertEqual("TERMINATE", create[create.index("--maintenance-policy") + 1])
            self.assertEqual("STANDARD", create[create.index("--provisioning-model") + 1])
            self.assertNotIn("--min-cpu-platform", create)
            self.assertEqual("purpose=" + tool.PURPOSE + ",run=issue42-test", create[create.index("--labels") + 1])
        self.assertEqual(["create-requested", "created"] * 2 + ["started"] * 2, self.events(plan))
        with self.assertRaises(tool.Failure):
            tool.execute(plan, True, gcloud)

    def test_planned_cpu_platform_is_requested(self):
        plan = tool.read_plan(self.planned(minCpuPlatform="Intel Sapphire Rapids"))
        arguments = tool.instance_arguments(plan, plan["vms"][0])
        self.assertEqual("Intel Sapphire Rapids", arguments[arguments.index("--min-cpu-platform") + 1])

    def test_uploads_contain_only_the_planned_inputs(self):
        gcloud = FakeGcloud()
        self.executed(gcloud)
        members = []
        for upload in gcloud.uploads:
            with tarfile.open(fileobj=io.BytesIO(upload)) as tar:
                members.append(sorted(tar.getnames()))
                settings = tar.extractfile("input/run.env").read().decode()
            keys = [line.split("=", 1)[0] for line in settings.splitlines()]
            self.assertEqual(["RUN_ID", "BENCHMARK_MODE", "FLINK_VERSION", "PROTOBUF_MAJOR", "SOURCE_REVISION",
                              "SOURCE_BASE", "VARIANTS", "JAVA_VERSION", "JUST_VERSION", "MISE_VERSION"], keys)
            self.assertNotIn("example-project", settings)
        common = ["input/gce-benchmark-vm.sh", "input/run.env", "input/source.bundle"]
        self.assertEqual([sorted(common + ["input/patches/baseline.patch"]), common], members)

    def test_execute_never_starts_beside_an_existing_run(self):
        plan = self.planned()
        gcloud = FakeGcloud(instances=[("foreign", "RUNNING", "issue42-test")])
        with self.assertRaises(tool.Failure):
            tool.execute(plan, True, gcloud)
        self.assertEqual(([], []), (gcloud.created(), gcloud.deleted()))
        self.assertIn("foreign", gcloud.instances)

    def test_failed_or_interrupted_start_deletes_every_created_instance(self):
        cases = [(lambda a: a[:3] == ("compute", "instances", "create") and a[3] == PB3, None, False, [PB4]),
                 (lambda a: a[:3] == ("compute", "instances", "create") and a[3] == PB4, None, True, [PB4]),
                 (lambda a: a[:2] == ("compute", "scp"), KeyboardInterrupt(), False, [PB4, PB3])]
        for index, (fail_on, error, after, expected) in enumerate(cases):
            with self.subTest(case=index):
                plan = self.planned(name="plan-" + str(index))
                gcloud = FakeGcloud(fail_on=fail_on, error=error, fail_after=after)
                with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(
                        type(error) if error else tool.Failure):
                    tool.execute(plan, True, gcloud, delay=0)
                self.assertEqual(sorted(expected), sorted(gcloud.deleted()))
                self.assertEqual("cleanup-verified", self.events(plan)[-1])

    def test_status_reports_absent_unknown_stopped_and_rebooted_instances(self):
        plan = tool.read_plan(self.planned())
        vm = plan["vms"][0]
        cases = [({}, "ok\n", ("ABSENT", "UNKNOWN")),
                 ({PB4: "RUNNING"}, "RUNNING baseline\n", ("RUNNING", "UNKNOWN")),
                 ({PB4: "TERMINATED"}, "", ("TERMINATED", "INTERRUPTED instance TERMINATED")),
                 ({PB4: "RUNNING"}, "RUNNING baseline\nold\nnew\n1\n", ("RUNNING", "INTERRUPTED reboot")),
                 ({PB4: "RUNNING"}, "RUNNING baseline\nboot\nboot\n7\n", ("RUNNING", "RUNNING baseline"))]
        for instances, runner, expected in cases:
            with self.subTest(expected=expected):
                state = tool.vm_state(FakeGcloud(runner=runner), plan, vm, instances)
                self.assertEqual(expected, (state["instance"], state["runner"]))

    def test_collect_gates_results_and_always_cleans_up(self):
        good = {"protobuf-4-candidate/console.log": b"synthetic\n",
                "protobuf-4-candidate/measurements.csv": tool_csv(),
                "protobuf-4-candidate/vm-manifest.txt": MANIFEST,
                "protobuf-4-candidate/maintenance-events.log": b"2026-01-01T00:00:00Z NONE\n"}
        cases = [("ok", good, None),
                 ("gate", {**good, "protobuf-4-candidate/unknown.bin": b"x"}, "evidence gate"),
                 ("maintenance", {**good, "protobuf-4-candidate/maintenance-events.log":
                                  b"t NONE\nt TERMINATE_ON_HOST_MAINTENANCE\n"}, "host maintenance"),
                 ("empty", {}, "no result directories"),
                 ("uncontrolled", {**good, "protobuf-4-candidate/vm-manifest.txt": MANIFEST + b"x\n",
                                   "protobuf-4-other/vm-manifest.txt": MANIFEST.replace(b"Sapphire", b"Emerald"),
                                   "protobuf-4-other/console.log": b"synthetic\n"}, "gce.cpu.platform"),
                 ("download", None, "download failed")]
        for name, files, failure in cases:
            with self.subTest(case=name):
                plan = self.planned(name=name, profiles=[{"protobuf": 4}])
                gcloud = FakeGcloud(instances=[(PB4, "RUNNING", "issue42-test")],
                                    evidence=evidence_archive(files) if files is not None else None,
                                    fail_on=(lambda a: a[:2] == ("compute", "scp")) if files is None else None)
                output = self.root / (name + "-collected")
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    if failure:
                        with self.assertRaisesRegex(tool.Failure, failure):
                            tool.collect(plan, output, gcloud)
                    else:
                        tool.collect(plan, output, gcloud)
                        checked = output / "checked" / "protobuf-4-candidate"
                        self.assertTrue((checked / "inventory.json").is_file())
                        # JMH output from benchmark/ and the VM maintenance log are flattened beside run records.
                        self.assertEqual(["console.log", "maintenance-events.log", "measurements.csv", "vm-manifest.txt"],
                                         sorted(p.name for p in (checked / "evidence").iterdir()))
                self.assertEqual([PB4], gcloud.deleted())
                self.assertEqual("cleanup-verified", self.events(plan)[-1])

    def test_collect_records_placement_and_checks_hosts_across_vms(self):
        groups = [{"protobuf": 4, "variants": [{"name": "a1", "vm": "a"}, {"name": "b1", "vm": "b"}]}]
        running = [(PB4 + "-a", "RUNNING", "issue42-test"), (PB4 + "-b", "RUNNING", "issue42-test")]

        def files(result, manifest):
            return evidence_archive({result + "/console.log": b"synthetic\n", result + "/vm-manifest.txt": manifest,
                                     result + "/maintenance-events.log": b"t NONE\n"})
        for name, other, failure in (("same", MANIFEST, None),
                                     ("differs", MANIFEST.replace(b"Linux 7", b"Linux 8"), "kernel")):
            with self.subTest(case=name):
                plan = self.planned(name=name, profiles=groups)
                gcloud = FakeGcloud(instances=running, evidence={PB4 + "-a": files("protobuf-4-a1", MANIFEST),
                                                                 PB4 + "-b": files("protobuf-4-b1", other)})
                output = self.root / (name + "-collected")
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    if failure:
                        with self.assertRaisesRegex(tool.Failure, failure):
                            tool.collect(plan, output, gcloud)
                    else:
                        tool.collect(plan, output, gcloud)
                record = json.loads((output / "collection.json").read_text())
                self.assertEqual({PB4 + "-a": ["protobuf-4-a1"], PB4 + "-b": ["protobuf-4-b1"]}, record["results"])
                self.assertEqual(sorted(name for name, _, _ in running), sorted(gcloud.deleted()))

    def test_collect_refuses_incomplete_runs_unless_partial(self):
        plan = self.planned()
        running = [(PB4, "RUNNING", "issue42-test"), (PB3, "RUNNING", "issue42-test")]
        gcloud = FakeGcloud(instances=running, runner="RUNNING candidate\nboot\nboot\n1\n")
        with self.assertRaises(tool.Failure):
            tool.collect(plan, self.root / "collected", gcloud)
        self.assertEqual([], gcloud.deleted())
        self.assertFalse((self.root / "collected").exists())
        partial = {major: evidence_archive({"protobuf-%d-candidate/console.log" % major: b"partial\n",
                                            "protobuf-%d-candidate/vm-manifest.txt" % major: MANIFEST})
                   for major in (3, 4)}
        gcloud = FakeGcloud(instances=running, runner="RUNNING candidate\nboot\nboot\n1\n",
                            evidence={PB4: partial[4], PB3: partial[3]})
        for runner in ("RUNNING candidate\nboot\nboot\n1\n", "unreadable\n"):
            gcloud = FakeGcloud(instances=running, runner=runner)
            with self.subTest(runner=runner), self.assertRaisesRegex(tool.Failure, "still active"):
                tool.collect(plan, self.root / "active", gcloud, partial=True)
            self.assertEqual([], gcloud.deleted())
        gcloud = FakeGcloud(instances=running, runner="FAILED candidate\nboot\nboot\n1\n",
                            evidence={PB4: partial[4], PB3: partial[3]})
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(tool.Failure, "diagnosis only"):
            tool.collect(plan, self.root / "partial", gcloud, partial=True)
        self.assertTrue((self.root / "partial" / "checked" / "protobuf-4-candidate").is_dir())
        self.assertEqual(sorted([PB4, PB3]), sorted(gcloud.deleted()))

    def test_cleanup_deletes_in_one_request_and_reports_remaining_disks(self):
        plan = self.planned()
        running = [(PB4, "RUNNING", "issue42-test"), (PB3, "RUNNING", "issue42-test"),
                   ("other-run", "RUNNING", "other")]
        gcloud = FakeGcloud(instances=running)
        with contextlib.redirect_stdout(io.StringIO()):
            tool.cleanup(plan, gcloud)
        deletes = [c for c in gcloud.calls if c[:3] == ("compute", "instances", "delete")]
        self.assertEqual(1, len(deletes))
        self.assertEqual(sorted([PB4, PB3]), sorted(gcloud.deleted()))
        self.assertEqual(["other-run"], list(gcloud.instances))
        stuck = FakeGcloud(instances=running[:1], keep_disks=True)
        with self.assertRaisesRegex(tool.Failure, PB4):
            tool.cleanup(plan, stuck)
        self.assertEqual("cleanup-incomplete", self.events(plan)[-1])

    def test_env_file_exposes_only_reviewed_keys_and_the_environment_wins(self):
        path = self.root / ".env"
        path.write_text("# comment\nCLOUDSDK_CONFIG=/private/config\nGOOGLE_APPLICATION_CREDENTIALS=/key.json\n"
                        "GOOGLE_CLOUD_PROJECT='example'\n", encoding="utf-8")
        self.assertEqual({"CLOUDSDK_CONFIG": "/private/config", "GOOGLE_CLOUD_PROJECT": "example"},
                         tool.load_env(path, {}))
        self.assertEqual("override", tool.load_env(path, {"GOOGLE_CLOUD_PROJECT": "override"})["GOOGLE_CLOUD_PROJECT"])
        path.write_text("not an assignment\n", encoding="utf-8")
        with self.assertRaises(tool.Failure):
            tool.load_env(path, {})

    def test_safe_extract_rejects_links_traversal_and_absolute_paths(self):
        for name, kind in (("../escape", tarfile.REGTYPE), ("/absolute", tarfile.REGTYPE),
                           ("evidence/link", tarfile.SYMTYPE)):
            archive = self.root / "unsafe.tar"
            with tarfile.open(archive, "w") as tar:
                info = tarfile.TarInfo(name)
                info.type = kind
                tar.addfile(info, io.BytesIO(b""))
            with self.subTest(name=name), self.assertRaises(tool.Failure):
                tool.safe_extract(archive, self.root / "out")
            self.assertFalse((self.root / "out").exists())


if __name__ == "__main__":
    unittest.main()
