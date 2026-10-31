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

"""Plan, run, collect, and clean up serializer benchmarks on temporary GCE VMs.

Only this operator process uses cloud credentials. Benchmark VMs have no service account and
receive a source bundle, optional variant patches, and non-secret run settings.
Each Protobuf profile runs on one VM by default; variants that name a VM group run on that group's VM,
and a profile that declares comparisons runs each comparison on its own VM.
"""

import argparse
import datetime
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
PLAN_VERSION = 1
PURPOSE = "flink-protobuf-benchmark"
VM_ROOT = "/srv/benchmark"
VM_SCRIPT = "gce-benchmark-vm.sh"
ENV_KEYS = ("CLOUDSDK_CONFIG", "GOOGLE_CLOUD_PROJECT", "GCE_BENCHMARK_ZONE",
            "GCE_BENCHMARK_MACHINE_TYPE")
# Planning estimates per variant, including two five-minute load gates; not a price quote.
# Modes without an estimate, such as one added by a later source commit, use the longest.
VARIANT_MINUTES = {"check": 20, "smoke": 25, "pair": 95, "run": 370}
SUITE = "src/benchmark/java/io/github/flink/gcp/protobuf/benchmark/BenchmarkSuite.java"
SETUP_MINUTES = 25
DEFAULTS = {
    "machineType": "c3-standard-8",
    "image": {"family": "ubuntu-2404-lts-amd64", "project": "ubuntu-os-cloud"},
    "bootDiskGb": 50,
    "bootDiskType": "pd-balanced",
    "flinkVersion": "2.3.0",
    "java": "temurin-17",
    "just": "1",
    "mise": "2026.9.15",
    "minCpuPlatform": None,
}
# Results of one plan are comparable only when every VM reports the same values for these keys.
CONTROLLED = ("gce.machine.type", "gce.cpu.platform", "gce.image", "kernel", "java.version")
NAME = re.compile(r"[a-z][a-z0-9-]{0,30}[a-z0-9]")
VARIANT = re.compile(r"[a-z][a-z0-9-]{0,23}")
# A group suffix keeps instance names within the 63-character limit and free of a trailing hyphen;
# a pb<digit> prefix could reproduce another run's <runId>-pb<major> name.
GROUP = re.compile(r"(?!pb[0-9])[a-z][a-z0-9]{0,15}")


class Failure(Exception):
    """An operator-facing error whose message contains no credential material."""


def require(condition, message):
    if not condition:
        raise Failure(message)


def now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_env(path, environ=os.environ):
    """Read the reviewed operator keys from a gitignored KEY=VALUE file; the environment wins."""
    values = {key: environ[key] for key in ENV_KEYS if environ.get(key)}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        require(separator == "=", "Malformed line in " + path.name)
        if key in ENV_KEYS and key not in values:
            values[key] = value.strip().strip("'\"")
    return values


class Gcloud:
    """Runs gcloud with the operator configuration; tests substitute a recording double."""

    def __init__(self, environment):
        self.environment = dict(os.environ)
        self.environment.update(environment)

    def __call__(self, *args, check=True):
        result = subprocess.run(["gcloud", *args], env=self.environment, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if check and result.returncode != 0:
            raise Failure("gcloud " + " ".join(args[:3]) + " failed: " + result.stderr.strip()[-400:])
        return result.stdout

    def ok(self, *args):
        """Returns whether a probe succeeds, without echoing expected readiness failures."""
        return subprocess.run(["gcloud", *args], env=self.environment, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL).returncode == 0

    def json(self, *args):
        return json.loads(self(*args, "--format=json") or "null")


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, check=True, text=True,
                          stdout=subprocess.PIPE).stdout.strip()


def git_ok(*args):
    return subprocess.run(["git", *args], cwd=ROOT, stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL).returncode == 0


def source_support(source):
    """Read the benchmark modes and Flink versions from the measured commit itself."""
    usage = re.search(r"Usage: BenchmarkSuite <([a-z|]+)>", git("show", source + ":" + SUITE))
    versions = re.findall(r"^\s*([0-9.|]+)\) adapter=", git("show", source + ":benchmarks.just"), re.M)
    require(usage is not None and versions, "The source commit has no recognizable benchmark harness")
    return usage.group(1).split("|"), [v for group in versions for v in group.split("|")]


def read_plan(directory):
    plan = json.loads((Path(directory) / "plan.json").read_text(encoding="utf-8"))
    require(plan.get("version") == PLAN_VERSION, "Unsupported plan version")
    return plan


def ledger(directory, event, **fields):
    """Append resource ownership and lifecycle events, including failed attempts."""
    with open(Path(directory) / "resources.jsonl", "a", encoding="utf-8") as out:
        out.write(json.dumps({"time": now(), "event": event, **fields}, sort_keys=True) + "\n")


def validate_spec(spec):
    require(isinstance(spec, dict), "The specification must be a JSON object")
    allowed = {"runId", "project", "zone", "machineType", "minCpuPlatform", "image", "bootDiskGb",
               "bootDiskType", "flinkVersion", "mode", "source", "base", "java", "just", "mise", "profiles"}
    require(set(spec) <= allowed, "Unknown specification fields: " + ", ".join(sorted(set(spec) - allowed)))
    require(NAME.fullmatch(str(spec.get("runId", ""))) is not None,
            "runId must be lowercase letters, digits, or hyphens (2-32 characters)")
    require(re.fullmatch(r"[a-z]+", str(spec.get("mode", ""))) is not None, "mode must name a BenchmarkSuite mode")
    require(isinstance(spec.get("source"), str) and spec["source"], "source must name a commit")
    profiles = spec.get("profiles")
    require(isinstance(profiles, list) and profiles, "profiles must be a non-empty list")
    majors = [profile.get("protobuf") for profile in profiles]
    require(all(major in (3, 4) for major in majors) and len(set(majors)) == len(majors),
            "Each profile needs a distinct protobuf major of 3 or 4")
    for profile in profiles:
        require(set(profile) <= {"protobuf", "variants", "comparisons"}, "Unknown profile fields")
        variants = profile_variants(profile)
        names = [variant.get("name") for variant in variants]
        require(variants and all(VARIANT.fullmatch(str(name)) for name in names) and len(set(names)) == len(names),
                "Variant names must be distinct lowercase identifiers")
        for variant in variants:
            require(set(variant) <= {"name", "patch", "vm"}, "Unknown variant fields")
            require("vm" not in variant or GROUP.fullmatch(str(variant["vm"])) is not None,
                    "A variant's vm must be 1-16 lowercase letters and digits, start with a letter, and not start with pb<digit>")
        # ADR-0004 rejects comparing variants across VMs, so groups only calibrate placement with one tree.
        require(not any("vm" in variant for variant in variants)
                or len({variant.get("patch") for variant in variants}) == 1,
                "Variants on separate VMs must measure the same tree, with one patch or none")
        if "comparisons" in profile:
            comparisons = profile["comparisons"]
            require(isinstance(comparisons, list) and comparisons, "comparisons must be a non-empty list")
            # Groups measure one tree, so a declared comparison is always placed pairwise instead.
            require(not any("vm" in variant for variant in variants), "comparisons cannot be combined with vm groups")
            pairs = set()
            for pair in comparisons:
                require(isinstance(pair, list) and len(pair) == 2 and all(isinstance(name, str) for name in pair)
                        and pair[0] != pair[1] and set(pair) <= set(names),
                        "Each comparison must name two distinct variants of its profile")
                require(frozenset(pair) not in pairs, "Comparison " + "/".join(pair) + " is declared twice")
                pairs.add(frozenset(pair))
            unused = [name for name in names if not any(name in pair for pair in comparisons)]
            require(not unused, "Variants in no comparison: " + ", ".join(unused))


def profile_variants(profile):
    return profile.get("variants", [{"name": "candidate"}])


def all_pairs(variants):
    return [[a["name"], b["name"]] for a, b in itertools.combinations(variants, 2)]


def result_name(major, variant):
    return "protobuf-" + str(major) + "-" + variant


def placement(run_id, profile):
    """Place a profile's variants on VMs, each with the comparisons it measures.

    Declared comparisons run pairwise, one VM each, so a variant shared by several comparisons is repeated on
    each of their VMs. Otherwise variants group by VM in specification order, variants without a group share the
    default VM, and every pair on one VM is comparable.
    """
    name = run_id + "-pb" + str(profile["protobuf"])
    variants = profile_variants(profile)
    if "comparisons" in profile:
        named = {variant["name"]: variant for variant in variants}
        return [(name + "-c" + str(index), [named[side] for side in pair], [pair])
                for index, pair in enumerate(profile["comparisons"], 1)]
    groups = {}
    for variant in variants:
        groups.setdefault(variant.get("vm"), []).append(variant)
    return [(name + ("-" + group if group else ""), members, all_pairs(members)) for group, members in groups.items()]


def plan(spec_path, output, environment, gcloud):
    """Resolve and record the exact configuration; this command creates no cloud resources."""
    output = Path(output)
    require(not output.exists(), "Plan output already exists: " + str(output))
    spec_path = Path(spec_path)
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    validate_spec(spec)
    settings = {**DEFAULTS, **{k: v for k, v in spec.items() if k != "profiles"}}
    settings["project"] = spec.get("project") or environment.get("GOOGLE_CLOUD_PROJECT")
    settings["zone"] = spec.get("zone") or environment.get("GCE_BENCHMARK_ZONE")
    settings["machineType"] = spec.get("machineType") or environment.get("GCE_BENCHMARK_MACHINE_TYPE") or DEFAULTS["machineType"]
    require(settings["project"] and settings["zone"], "Set project and zone in the specification or .env")
    source = git("rev-parse", "--verify", settings["source"] + "^{commit}")
    base = git("rev-parse", "--verify", (spec.get("base") or git("merge-base", source, "origin/main")) + "^{commit}")
    # The VM receives only the source history, so its retained patch needs an ancestor base.
    require(git_ok("merge-base", "--is-ancestor", base, source), "base must be an ancestor of source")
    modes, versions = source_support(source)
    require(settings["mode"] in modes, "The source commit supports modes: " + ", ".join(modes))
    require(settings["flinkVersion"] in versions, "The source commit supports Flink: " + ", ".join(versions))
    project, zone = settings["project"], settings["zone"]

    image = gcloud.json("compute", "images", "describe-from-family", settings["image"]["family"],
                        "--project", settings["image"]["project"])
    machine = gcloud.json("compute", "machine-types", "describe", settings["machineType"],
                          "--project", project, "--zone", zone)
    existing = gcloud.json("compute", "instances", "list", "--project", project,
                           "--filter", "labels.run=" + spec["runId"])
    require(not existing, "Instances labelled run=" + spec["runId"] + " already exist")
    region = gcloud.json("compute", "regions", "describe", zone.rsplit("-", 1)[0], "--project", project)
    placements = [(profile, name, variants, pairs) for profile in spec["profiles"]
                  for name, variants, pairs in placement(spec["runId"], profile)]
    family = settings["machineType"].split("-")[0].upper() + "_CPUS"
    cpus = machine["guestCpus"] * len(placements)
    # Every VM also holds one instance slot and an ephemeral external address for SSH. The project lists
    # metrics such as IN_USE_ADDRESSES globally too, so each scope is checked only for its own metrics.
    project_info = gcloud.json("compute", "project-info", "describe", "--project", project)
    checks = [(region, {"CPUS": cpus, family: cpus, "INSTANCES": len(placements),
                        "IN_USE_ADDRESSES": len(placements)}),
              (project_info, {"CPUS_ALL_REGIONS": cpus})]
    for scope, needed in checks:
        quotas = {quota["metric"]: quota for quota in scope.get("quotas", [])}
        for metric, amount in needed.items():
            if metric in quotas:
                require(quotas[metric]["usage"] + amount <= quotas[metric]["limit"],
                        metric + " quota is insufficient for " + str(amount) + " more")

    output.mkdir(parents=True)
    (output / "patches").mkdir()
    vms = []
    minutes = 0
    for profile, name, planned, pairs in placements:
        variants = []
        for variant in planned:
            entry = {"name": variant["name"]}
            if variant.get("patch"):
                patch = (spec_path.parent / variant["patch"]).resolve()
                require(patch.is_file(), "Missing patch for variant " + variant["name"])
                target = output / "patches" / (result_name(profile["protobuf"], variant["name"]) + ".patch")
                target.write_bytes(patch.read_bytes())
                entry.update(patch=target.name, patchSha256=sha256(target))
            variants.append(entry)
        vm_minutes = SETUP_MINUTES + VARIANT_MINUTES.get(settings["mode"], max(VARIANT_MINUTES.values())) * len(variants)
        minutes += vm_minutes
        vms.append({"name": name, "protobuf": profile["protobuf"], "variants": variants,
                    "comparisons": pairs, "estimatedMinutes": vm_minutes})
    result = {
        "version": PLAN_VERSION, "createdAt": now(), "runId": spec["runId"],
        "project": project, "zone": zone, "machineType": settings["machineType"],
        "guestCpus": machine["guestCpus"], "memoryMb": machine["memoryMb"],
        "minCpuPlatform": settings["minCpuPlatform"],
        "image": {"name": image["name"], "id": str(image["id"]), "project": settings["image"]["project"],
                  "family": settings["image"]["family"]},
        "bootDiskGb": settings["bootDiskGb"], "bootDiskType": settings["bootDiskType"],
        "scheduling": {"provisioningModel": "STANDARD", "onHostMaintenance": "TERMINATE",
                       "automaticRestart": False},
        "labels": {"purpose": PURPOSE, "run": spec["runId"]},
        "source": source, "base": base, "mode": settings["mode"], "flinkVersion": settings["flinkVersion"],
        "tools": {"java": settings["java"], "just": settings["just"], "mise": settings["mise"]},
        "vms": vms, "estimatedVmHours": round(minutes / 60, 2),
        "estimatedWallHours": round(max(vm["estimatedMinutes"] for vm in vms) / 60, 2),
    }
    (output / "plan.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("Planned " + str(len(vms)) + " " + settings["machineType"] + " VM(s) in " + project + "/" + zone
          + " from image " + image["name"] + ".")
    print("Estimated usage: " + str(result["estimatedVmHours"]) + " VM-hours over about "
          + str(result["estimatedWallHours"]) + " hours, plus "
          + str(settings["bootDiskGb"]) + " GB " + settings["bootDiskType"] + " per VM.")
    print("Check current pricing and project ownership, then run: execute " + str(output) + " --approve")
    return result


def run_env(plan_data, vm):
    values = {
        "RUN_ID": plan_data["runId"], "BENCHMARK_MODE": plan_data["mode"],
        "FLINK_VERSION": plan_data["flinkVersion"], "PROTOBUF_MAJOR": str(vm["protobuf"]),
        "SOURCE_REVISION": plan_data["source"], "SOURCE_BASE": plan_data["base"],
        "VARIANTS": " ".join(variant["name"] for variant in vm["variants"]),
        "JAVA_VERSION": plan_data["tools"]["java"], "JUST_VERSION": plan_data["tools"]["just"],
        "MISE_VERSION": plan_data["tools"]["mise"],
    }
    return "".join(key + "=" + shlex.quote(value) + "\n" for key, value in values.items())


def instance_arguments(plan_data, vm):
    labels = ",".join(key + "=" + value for key, value in sorted(plan_data["labels"].items()))
    scheduling = plan_data["scheduling"]
    return ["compute", "instances", "create", vm["name"], "--project", plan_data["project"],
            "--zone", plan_data["zone"], "--machine-type", plan_data["machineType"],
            "--image", plan_data["image"]["name"], "--image-project", plan_data["image"]["project"],
            "--boot-disk-size", str(plan_data["bootDiskGb"]) + "GB",
            "--boot-disk-type", plan_data["bootDiskType"], "--boot-disk-auto-delete",
            "--no-service-account", "--no-scopes",
            "--provisioning-model", scheduling["provisioningModel"],
            "--maintenance-policy", scheduling["onHostMaintenance"],
            "--restart-on-failure" if scheduling["automaticRestart"] else "--no-restart-on-failure",
            "--shielded-secure-boot", "--labels", labels,
            *(["--min-cpu-platform", plan_data["minCpuPlatform"]] if plan_data.get("minCpuPlatform") else [])]


def run_instances(gcloud, plan_data):
    """Map the name of every instance labelled with this run to its status, in one query."""
    return {instance["name"]: instance["status"] for instance in gcloud.json(
        "compute", "instances", "list", "--project", plan_data["project"],
        "--filter", "labels.run=" + plan_data["runId"])}


def ssh_arguments(plan_data, vm, command):
    return ["compute", "ssh", vm["name"], "--project", plan_data["project"],
            "--zone", plan_data["zone"], "--quiet", "--command", command]


def ssh(gcloud, plan_data, vm, command, check=True):
    return gcloud(*ssh_arguments(plan_data, vm, command), check=check)


def execute(directory, approve, gcloud, attempts=30, delay=10):
    """Create each VM, upload credential-free inputs, and start the detached runner."""
    directory = Path(directory)
    plan_data = read_plan(directory)
    require(approve, "Provisioning requires --approve after reviewing plan.json and current pricing")
    require(not (directory / "resources.jsonl").exists(), "This plan was already executed; plan a new run ID")
    # Failure cleanup deletes by run label, so it must never start beside another plan's VMs.
    require(not run_instances(gcloud, plan_data), "Instances labelled run=" + plan_data["runId"] + " already exist")
    bundle = directory / "source.bundle"
    reference = "refs/gce-benchmark/" + plan_data["runId"]
    git("update-ref", reference, plan_data["source"])
    try:
        git("bundle", "create", str(bundle), reference)
    finally:
        git("update-ref", "-d", reference)
    created = []
    try:
        for vm in plan_data["vms"]:
            ledger(directory, "create-requested", instance=vm["name"], zone=plan_data["zone"])
            created.append(vm)
            gcloud(*instance_arguments(plan_data, vm))
            ledger(directory, "created", instance=vm["name"], zone=plan_data["zone"])
        for vm in plan_data["vms"]:
            for attempt in range(attempts):
                if gcloud.ok(*ssh_arguments(plan_data, vm, "true")):
                    break
                require(attempt < attempts - 1, "SSH did not become available on " + vm["name"])
                time.sleep(delay)
            with tempfile.TemporaryDirectory() as temporary:
                settings = Path(temporary) / "run.env"
                settings.write_text(run_env(plan_data, vm), encoding="utf-8")
                archive = Path(temporary) / "input.tar"
                with tarfile.open(archive, "w") as tar:
                    tar.add(settings, arcname="input/run.env")
                    tar.add(bundle, arcname="input/source.bundle")
                    tar.add(ROOT / "scripts" / VM_SCRIPT, arcname="input/" + VM_SCRIPT)
                    for variant in vm["variants"]:
                        if "patch" in variant:
                            tar.add(directory / "patches" / variant["patch"],
                                    arcname="input/patches/" + variant["name"] + ".patch")
                gcloud("compute", "scp", str(archive), vm["name"] + ":/tmp/flink-benchmark-input.tar",
                       "--project", plan_data["project"], "--zone", plan_data["zone"], "--quiet")
            ssh(gcloud, plan_data, vm,
                "sudo mkdir -p " + VM_ROOT + " && sudo tar -xf /tmp/flink-benchmark-input.tar -C " + VM_ROOT
                + " && sudo systemd-run --unit=flink-benchmark --property=Nice=-5 /bin/bash "
                + VM_ROOT + "/input/" + VM_SCRIPT + " run")
            ledger(directory, "started", instance=vm["name"])
    except BaseException:
        if created:
            try:
                cleanup(directory, gcloud)
            except Failure as error:
                print("gce-benchmark: cleanup after failure: " + str(error), file=sys.stderr)
        raise
    print("Started " + str(len(plan_data["vms"])) + " VM(s); poll with: status " + str(directory))


def vm_state(gcloud, plan_data, vm, instances):
    if vm["name"] not in instances:
        return {"instance": "ABSENT", "runner": "UNKNOWN"}
    instance = instances[vm["name"]]
    if instance != "RUNNING":
        return {"instance": instance, "runner": "INTERRUPTED instance " + instance}
    output = ssh(gcloud, plan_data, vm,
                 "sudo cat " + VM_ROOT + "/state/status " + VM_ROOT + "/state/boot-id /proc/sys/kernel/random/boot_id"
                 " && sudo find " + VM_ROOT + "/results -name '*.json' -path '*/benchmark/*' | wc -l",
                 check=False) or ""
    lines = output.strip().splitlines()
    if len(lines) < 4:
        return {"instance": instance, "runner": "UNKNOWN"}
    runner = lines[0]
    if lines[1] != lines[2]:
        runner = "INTERRUPTED reboot"
    return {"instance": instance, "runner": runner, "results": int(lines[3])}


def status(directory, gcloud):
    plan_data = read_plan(directory)
    instances = run_instances(gcloud, plan_data)
    states = {}
    for vm in plan_data["vms"]:
        states[vm["name"]] = vm_state(gcloud, plan_data, vm, instances)
        print(vm["name"] + ": " + json.dumps(states[vm["name"]], sort_keys=True))
    return states


def safe_extract(archive, destination):
    """Extract only regular files and directories below the destination."""
    with tarfile.open(archive) as tar:
        for member in tar.getmembers():
            path = Path(member.name)
            require(not path.is_absolute() and ".." not in path.parts, "Unsafe path in VM archive")
            require(member.isfile() or member.isdir(), "Unsupported member in VM archive")
        for member in tar.getmembers():
            target = destination / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(member) as source, open(target, "wb") as out:
                    shutil.copyfileobj(source, out)


def manifest(result):
    path = result / "vm-manifest.txt"
    lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    return dict(line.split("=", 1) for line in lines if "=" in line)


def uncontrolled(results):
    """Name the controlled keys whose values differ, or are missing, across a plan's results."""
    values = [manifest(result) for result in results]
    return [key for key in CONTROLLED if len({value.get(key) for value in values}) > 1
            or any(key not in value for value in values)]


def maintenance_events(result):
    """Return host-maintenance events other than NONE or a logged unavailable request."""
    log = result / "maintenance-events.log"
    lines = log.read_text(encoding="utf-8").splitlines() if log.is_file() else []
    return [line for line in lines if line.split()[-1:] not in (["NONE"], ["UNAVAILABLE"])]


def flatten(raw, evidence):
    """Combine each result's JMH output, run records, and maintenance log into one directory."""
    events = raw / "state" / "maintenance-events.log"
    results = raw / "results"
    for result in sorted(path for path in results.iterdir() if path.is_dir()) if results.is_dir() else []:
        target = evidence / result.name
        target.mkdir(parents=True)
        for path in [*sorted((result / "benchmark").glob("*")), *sorted(result.glob("*")), events]:
            if path.is_file():
                shutil.copyfile(path, target / path.name)


def download(gcloud, plan_data, vm, output, failures):
    """Fetch, flatten, and gate one VM's results, recording rather than raising failures."""
    raw = output / "raw" / vm["name"]
    evidence = output / "evidence" / vm["name"]
    try:
        # A run that failed during preparation has no results directory yet.
        ssh(gcloud, plan_data, vm, "sudo mkdir -p " + VM_ROOT + "/results " + VM_ROOT + "/state && sudo tar -C "
            + VM_ROOT + " -cf /tmp/flink-benchmark-results.tar results state"
            " && sudo chmod 644 /tmp/flink-benchmark-results.tar")
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / "results.tar"
            gcloud("compute", "scp", vm["name"] + ":/tmp/flink-benchmark-results.tar", str(archive),
                   "--project", plan_data["project"], "--zone", plan_data["zone"], "--quiet")
            safe_extract(archive, raw)
        flatten(raw, evidence)
    except (Failure, OSError, tarfile.TarError) as error:
        failures.append(vm["name"] + ": download failed: " + type(error).__name__)
        return
    results = sorted(path for path in evidence.iterdir() if path.is_dir()) if evidence.is_dir() else []
    if not results:
        failures.append(vm["name"] + ": no result directories")
    for result in results:
        if maintenance_events(result):
            failures.append(vm["name"] + "/" + result.name + ": host maintenance was recorded")
        checked = output / "checked" / vm["name"] / result.name
        code = subprocess.run([sys.executable, str(ROOT / "scripts" / "benchmark-evidence.py"),
                               "check", str(result), str(checked)]).returncode
        if code != 0:
            failures.append(vm["name"] + "/" + result.name + ": evidence gate rejected the directory")


def collect(directory, output, gcloud, partial=False, keep=False):
    """Download raw results privately, gate each result directory, then delete the VMs."""
    directory, output = Path(directory), Path(output)
    plan_data = read_plan(directory)
    require(not output.exists(), "Collection output already exists: " + str(output))
    instances = run_instances(gcloud, plan_data)
    states = {vm["name"]: vm_state(gcloud, plan_data, vm, instances) for vm in plan_data["vms"]}
    incomplete = {name: state for name, state in states.items() if state["runner"] != "SUCCEEDED"}
    require(partial or not incomplete, "Runs are not complete: " + json.dumps(incomplete, sort_keys=True))
    # Collection deletes the VMs, so a runner that is or may still be working (a running instance whose
    # state could not be read) must finish or be cleaned up instead.
    active = [name for name, state in states.items() if state["runner"].startswith(("RUNNING", "PREPARING"))
              or (state["instance"] == "RUNNING" and state["runner"] == "UNKNOWN")]
    require(not active, "Runners are still active; wait or use cleanup: " + ", ".join(active))
    (output / "raw").mkdir(parents=True)
    (output / "checked").mkdir()
    failures = []
    try:
        for vm in plan_data["vms"]:
            if states[vm["name"]]["instance"] != "RUNNING":
                failures.append(vm["name"] + ": instance not running")
            else:
                download(gcloud, plan_data, vm, output, failures)
        checked = sorted(path for path in (output / "evidence").glob("*/*") if path.is_dir())
        differing = uncontrolled(checked) if checked else []
        if differing:
            failures.append("results differ or lack controlled values: " + ", ".join(differing))
        # Record placement beside the states, including VMs that produced no result. A repeated variant's
        # results on different VMs are never compared, so each comparison names its VM's result directories.
        placed = {vm["name"]: sorted(path.name for path in (output / "evidence" / vm["name"]).glob("*")
                                     if path.is_dir()) for vm in plan_data["vms"]}
        # A plan written before comparisons were recorded used the sequential placement.
        compared = {vm["name"]: [[result_name(vm["protobuf"], side) for side in pair]
                                 for pair in vm.get("comparisons", all_pairs(vm["variants"]))]
                    for vm in plan_data["vms"]}
        record = {"time": now(), "states": states, "results": placed, "comparisons": compared,
                  "partial": bool(incomplete), "failures": failures}
        (output / "collection.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n",
                                                encoding="utf-8")
        ledger(directory, "collected", output=str(output), partial=bool(incomplete), failures=len(failures))
    finally:
        if not keep:
            cleanup(directory, gcloud)
    require(not failures, "Collection completed with failures: " + "; ".join(failures))
    require(not incomplete, "Partial results were collected for diagnosis only")
    print("Collected checked evidence under " + str(output / "checked") + "; human review remains required.")


def cleanup(directory, gcloud):
    """Delete every instance labelled with this run and verify that no instance or disk remains."""
    plan_data = read_plan(directory)
    project, zone, run = plan_data["project"], plan_data["zone"], plan_data["runId"]
    names = [vm["name"] for vm in plan_data["vms"]]
    instances = gcloud.json("compute", "instances", "list", "--project", project,
                            "--filter", "labels.run=" + run + " AND labels.purpose=" + PURPOSE)
    if instances:
        targets = [instance["name"] for instance in instances]
        ledger(directory, "delete-requested", instances=targets)
        # One request deletes the instances concurrently.
        gcloud("compute", "instances", "delete", *targets, "--project", project,
               "--zone", zone, "--delete-disks=all", "--quiet")
        ledger(directory, "deleted", instances=targets)
    disk_filter = " OR ".join("name=" + name for name in names)
    remaining = {
        "instances": [i["name"] for i in gcloud.json("compute", "instances", "list", "--project", project,
                                                     "--filter", "labels.run=" + run)],
        "disks": [d["name"] for d in gcloud.json("compute", "disks", "list", "--project", project,
                                                 "--filter", disk_filter)],
    }
    ledger(directory, "cleanup-verified" if not any(remaining.values()) else "cleanup-incomplete", **remaining)
    require(not any(remaining.values()), "Resources remain after cleanup: " + json.dumps(remaining, sort_keys=True))
    print("No instances or disks remain for run " + run + ".")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("plan", help="resolve a specification without creating resources")
    command.add_argument("spec")
    command.add_argument("output")
    command = commands.add_parser("execute", help="create VMs and start the planned runs")
    command.add_argument("plan")
    command.add_argument("--approve", action="store_true")
    command = commands.add_parser("status", help="report instance and runner states")
    command.add_argument("plan")
    command = commands.add_parser("collect", help="download, gate, and clean up results")
    command.add_argument("plan")
    command.add_argument("output")
    command.add_argument("--partial", action="store_true", help="collect incomplete runs for diagnosis")
    command.add_argument("--keep", action="store_true", help="leave the VMs for inspection")
    command = commands.add_parser("cleanup", help="delete and verify every resource of a run")
    command.add_argument("plan")
    args = parser.parse_args(argv)
    environment = load_env(Path(os.environ.get("GCE_BENCHMARK_ENV", ROOT / ".env")))
    gcloud = Gcloud(environment)
    try:
        if args.command == "plan":
            plan(args.spec, args.output, environment, gcloud)
        elif args.command == "execute":
            execute(args.plan, args.approve, gcloud)
        elif args.command == "status":
            status(args.plan, gcloud)
        elif args.command == "collect":
            collect(args.plan, args.output, gcloud, args.partial, args.keep)
        else:
            cleanup(args.plan, gcloud)
    except Failure as error:
        print("gce-benchmark: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
