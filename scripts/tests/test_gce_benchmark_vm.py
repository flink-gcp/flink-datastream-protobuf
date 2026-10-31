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

"""Offline tests of the VM runner: a loopback metadata server and command stubs replace the VM.

The stubs in gce-benchmark-vm-stubs.sh stand in for root-only commands, the benchmark user, and the
benchmark tools; Bash, Git, curl, and the text tools are real. Each load-gate `sleep 2` advances a fake /proc by one configured
sample, so the gate runs without waiting and the tests assert no timings.
"""

import contextlib
import hashlib
import http.server
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import urllib.parse

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import gce_benchmark as tool

RUNNER = Path(tool.__file__).with_name(tool.VM_SCRIPT)
TOOLS = {"java": "temurin-17", "just": "1.40.0", "mise": "2025.1.0"}
PROTOBUF = 4
EVENT = "TERMINATE_ON_HOST_MAINTENANCE"
INTERRUPTED = "INTERRUPTED maintenance-event " + EVENT
METADATA = {
    "zone": "projects/1/zones/asia-northeast1-b", "name": "offline-pb4",
    "machine-type": "projects/1/machineTypes/c3-standard-8", "cpu-platform": "Intel Sapphire Rapids",
    "image": "projects/ubuntu-os-cloud/global/images/ubuntu-2404-noble-amd64-v20261001",
    "scheduling/on-host-maintenance": "TERMINATE",
}
RESULT_FILES = {"vm-manifest.txt", "invocation.txt", "source-base.txt", "measurement-source.patch",
                "load-before-build.log", "build.log", "load-after-build.log", "start-utc.txt",
                "console.log", "end-utc.txt"}
# Bounds that stop a broken runner from hanging CI: a stub's wait for the runner (30 s) and one run.
TIMEOUT = 60

STUBS = Path(__file__).with_name("gce-benchmark-vm-stubs.sh")
# Commands that the runner finds on its own PATH; the benchmark user's stubs are mise, Java, and just.
RUNNER_COMMANDS = ("runuser", "pkill", "apt-get", "useradd", "chown", "mv", "sleep")

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="the VM runner needs Linux with Bash 4.4 or later")
# After one run times out, later tests fail at once, so a hanging runner costs one timeout, not one per test.
timed_out = False


class Metadata(http.server.ThreadingHTTPServer):
    """Serve instance metadata, hanging maintenance-event requests, and the mise release."""

    daemon_threads = True

    def handle_error(self, request, client_address):
        """Ignore requests that a stopped watcher abandoned, and report every other error."""
        if not isinstance(sys.exc_info()[1], ConnectionError):
            super().handle_error(request, client_address)

    def __init__(self, values, unavailable, files):
        super().__init__(("127.0.0.1", 0), MetadataHandler)
        self.values = values
        self.unavailable = set(unavailable)
        self.files = files
        self.paths = []
        self.maintenance_requests = 0
        self.event, self.version, self.closing = "NONE", 1, False
        self.changed = threading.Condition()
        self.url = "http://127.0.0.1:" + str(self.server_port)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def etag(self):
        return "etag-" + str(self.version)

    def announce(self):
        with self.changed:
            self.event, self.version = EVENT, self.version + 1
            self.changed.notify_all()

    def close(self):
        with self.changed:
            self.closing = True
            self.changed.notify_all()
        self.shutdown()
        self.server_close()


class MetadataHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, code, body="", etag=None):
        self.send_response(code)
        if etag:
            self.send_header("ETag", etag)
        self.send_header("Content-Length", str(len(body.encode())))
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        server = self.server
        server.paths.append(self.path)
        url = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(url.query)
        if url.path == "/control/maintenance":
            server.announce()
            return self.reply(200)
        if url.path in server.files:
            return self.reply(200, server.files[url.path])
        key = url.path.removeprefix("/instance/")
        if key == url.path or self.headers.get("Metadata-Flavor") != "Google":
            return self.reply(403)
        if key != "maintenance-event":
            return self.reply(200, server.values[key]) if key in server.values else self.reply(404)
        with server.changed:
            server.maintenance_requests += 1
            if server.maintenance_requests in server.unavailable:
                return self.reply(503)
            if query.get("wait_for_change") == ["true"]:
                last = query.get("last_etag", [""])[0]
                server.changed.wait_for(lambda: server.closing or server.etag() != last)
            if server.closing:
                return self.reply(503)
            event, etag = server.event, server.etag()
        self.reply(200, event, etag)


def git(directory, *args):
    environment = {**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
                   "GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.com",
                   "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.com"}
    return subprocess.run(["git", "-C", str(directory), *args], check=True, capture_output=True, text=True,
                          env=environment).stdout.strip()


class Source:
    """A base commit, a planned source commit in a bundle under a non-branch ref, and variant patches."""

    def __init__(self, directory):
        repository = directory / "repository"
        repository.mkdir()
        git(repository, "init", "--quiet")
        (repository / "payload.txt").write_text("base\n")
        git(repository, "add", "-A")
        git(repository, "commit", "--quiet", "-m", "base")
        self.base = git(repository, "rev-parse", "HEAD")
        (repository / "payload.txt").write_text("source\n")
        (repository / "planned.txt").write_text("planned\n")
        git(repository, "add", "-A")
        git(repository, "commit", "--quiet", "-m", "source")
        self.revision = git(repository, "rev-parse", "HEAD")
        (repository / "payload.txt").write_text("candidate\n")
        (repository / "added.txt").write_text("added\n")
        git(repository, "add", "-A")
        self.patches = {"candidate": directory / "candidate.patch", "broken": directory / "broken.patch"}
        self.patches["candidate"].write_text(git(repository, "diff", "--cached") + "\n")
        self.patches["broken"].write_text(
            self.patches["candidate"].read_text().replace("-source", "-something else"))
        git(repository, "reset", "--quiet", "--hard")
        git(repository, "update-ref", "refs/flink-benchmark/source", self.revision)
        self.bundle = directory / "source.bundle"
        git(repository, "bundle", "create", "--quiet", str(self.bundle), "refs/flink-benchmark/source")


class VirtualMachine:
    """One runner invocation with its own VM root, fake /proc, stubs, and metadata server."""

    def __init__(self, directory, source, variants=("baseline",), patches=None, actions=None, samples=(),
                 event_at=None, unavailable=(), watched=1, hold_watcher=False, metadata=METADATA, checksum=None,
                 settings=""):
        self.root = directory / "srv"
        self.input = self.root / "input"
        self.state = self.root / "state"
        self.results = self.root / "results"
        self.system = directory / "system"
        self.stubs = directory / "stubs"
        self.home = directory / "home"
        for path in (self.input / "patches", self.system / "proc/sys/kernel/random", self.system / "etc",
                     self.stubs / "bin", self.stubs / "actions", self.stubs / "benchmark-processes",
                     self.home):
            path.mkdir(parents=True)
        java_home = directory / "mise/installs/java" / TOOLS["java"]
        just_home = directory / "mise/installs/just" / TOOLS["just"]
        # The runner downloads the mise wrapper from the server and checks it against SHASUMS256.txt.
        mise = self.wrapper("mise")
        binary = "mise-v" + TOOLS["mise"] + "-linux-x64"
        release = "/mise/v" + TOOLS["mise"] + "/"
        digest = checksum or hashlib.sha256(mise.encode()).hexdigest()
        self.server = Metadata(dict(metadata), unavailable,
                               {release + "SHASUMS256.txt": digest + "  ./" + binary + "\n", release + binary: mise})
        values = {"state": self.state, "system": self.system,
                  "boot_id": self.system / "proc/sys/kernel/random/boot_id", "control": self.server.url,
                  "real_sleep": shutil.which("sleep"), "real_curl": shutil.which("curl"), "real_mv": shutil.which("mv"),
                  "java_home": java_home, "just_home": just_home}
        (self.stubs / "config.sh").write_text(
            "".join(name + "=" + shlex.quote(str(value)) + "\n" for name, value in values.items()))
        for name in RUNNER_COMMANDS:
            self.executable(self.stubs / "bin" / name, self.wrapper(name))
        self.executable(java_home / "bin/java", self.wrapper("java"))
        self.executable(just_home / "bin/just", self.wrapper("just"))

        for name, action in (actions or {}).items():
            (self.stubs / "actions" / name).write_text(action + "\n")
        (self.stubs / "samples").write_text("".join(" ".join(map(str, sample)) + "\n" for sample in samples))
        (self.stubs / "observations").write_text("0\n")
        (self.stubs / "watched").write_text(str(watched) + "\n")
        (self.stubs / "counters").write_text("0 0 0 0\n")
        if event_at:
            (self.stubs / "event-at").write_text(str(event_at) + "\n")
        if hold_watcher:
            (self.stubs / "hold-watcher").touch()
        (self.system / "proc/stat").write_text("cpu  0 0 0 0 0 0 0 0 0 0\n")
        (self.system / "proc/loadavg").write_text("0.10 0.10 0.05 1/100 1000\n")
        (self.system / "proc/vmstat").write_text("pswpin 0\npswpout 0\n")
        (self.system / "proc/cpuinfo").write_text("processor\t: 0\nmodel name\t: Intel(R) Xeon(R) Platinum 8481C\n")
        (self.system / "proc/meminfo").write_text("MemTotal:       32864356 kB\n")
        (self.system / "proc/sys/kernel/random/boot_id").write_text("boot-1\n")
        (self.system / "etc/os-release").write_text('NAME="Ubuntu"\nPRETTY_NAME="Ubuntu 24.04.3 LTS"\n')

        plan = {"runId": "offline-1", "mode": "smoke", "flinkVersion": "2.3.0", "source": source.revision,
                "base": source.base, "tools": TOOLS}
        vm = {"protobuf": PROTOBUF, "variants": [{"name": variant} for variant in variants]}
        (self.input / "run.env").write_text(tool.run_env(plan, vm) + settings)
        shutil.copyfile(source.bundle, self.input / "source.bundle")
        shutil.copyfile(RUNNER, self.input / tool.VM_SCRIPT)
        for variant, patch in (patches or {}).items():
            shutil.copyfile(source.patches[patch], self.input / "patches" / (variant + ".patch"))

    def wrapper(self, command):
        """Return a script for the replaced command's location that runs its stub for this run."""
        stub = (shutil.which("bash"), STUBS, self.stubs, command)
        return "#!/bin/bash\nexec " + " ".join(shlex.quote(str(part)) for part in stub) + ' "$@"\n'

    @staticmethod
    def executable(path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        path.chmod(0o755)

    def run(self):
        """Run the runner to completion and return its exit code, stopping leftover stub processes."""
        environment = {"PATH": str(self.stubs / "bin") + os.pathsep + os.environ["PATH"],
                       "HOME": str(self.home), "LANG": "C.UTF-8",
                       "GCE_BENCHMARK_VM_ROOT": str(self.root), "GCE_BENCHMARK_VM_SYSTEM_ROOT": str(self.system),
                       "GCE_BENCHMARK_VM_METADATA_URL": self.server.url + "/instance",
                       "GCE_BENCHMARK_VM_MISE_URL": self.server.url + "/mise"}
        # A file, not a pipe: a stopped watcher's hanging request must not hold the output open.
        with (self.stubs / "runner.log").open("wb") as log:
            process = subprocess.Popen([shutil.which("bash"), str(self.input / tool.VM_SCRIPT), "run"],
                                       env=environment, stdin=subprocess.DEVNULL, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
        try:
            return process.wait(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            raise RunnerTimeout("The runner did not finish within " + str(TIMEOUT) + " s\n" + self.report())
        finally:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            self.server.close()

    def status(self):
        return (self.state / "status").read_text().strip()

    def events(self):
        return [line.split(" ", 1)[1] for line in (self.state / "events.log").read_text().splitlines()]

    def calls(self, prefix=""):
        log = self.stubs / "calls.log"
        lines = log.read_text().splitlines() if log.is_file() else []
        return [line for line in lines if line.startswith(prefix)]

    def result(self, variant):
        return self.results / tool.result_name(PROTOBUF, variant)

    def gate(self, variant, name="load-before-build"):
        return (self.result(variant) / (name + ".log")).read_text().splitlines()

    def report(self):
        """Describe the run for a failure message: its status, events, stub calls, and output."""
        files = {"status": self.state / "status", "events": self.state / "events.log",
                 "maintenance events": self.state / "maintenance-events.log",
                 "stub calls": self.stubs / "calls.log", "runner output": self.stubs / "runner.log"}
        return "".join("--- " + name + "\n" + (path.read_text(errors="replace") if path.is_file() else "(none)\n")
                       for name, path in files.items())


class RunnerTimeout(AssertionError):
    pass


@pytest.fixture(scope="module")
def source(tmp_path_factory):
    return Source(tmp_path_factory.mktemp("source"))


def virtual_machine(directory, source, **options):
    if timed_out:
        pytest.fail("An earlier runner timed out; see that failure")
    directory.mkdir()
    return VirtualMachine(directory, source, **options)


@pytest.fixture
def new_vm(tmp_path, source):
    """Return a factory of the test's VM."""
    return lambda **options: virtual_machine(tmp_path / "vm", source, **options)


def finished(vm, status):
    global timed_out
    try:
        code = vm.run()
    except RunnerTimeout:
        timed_out = True
        raise
    assert (vm.status(), vm.events()[-1], code == 0, vm.calls("timed out")) == (
        status, status, status == "SUCCEEDED", []), vm.report()
    return vm


def interrupted(vm):
    finished(vm, INTERRUPTED)
    assert "FAILED" not in " ".join(vm.events()), "A later failure replaced the interruption"
    events = vm.events()
    assert (INTERRUPTED, INTERRUPTED) not in set(zip(events, events[1:])), vm.report()
    assert tool.maintenance_events(vm.state)[-1].split()[-1] == EVENT, vm.report()
    return vm


@pytest.fixture(scope="module")
def successful_run(tmp_path_factory, source):
    """Run two variants once; the tests below check the run and each variant's results."""
    vm = virtual_machine(tmp_path_factory.mktemp("successful-run") / "vm", source,
                         variants=("baseline", "candidate"), patches={"candidate": "candidate"})
    return finished(vm, "SUCCEEDED")


def test_successful_run_records_every_step_and_the_measured_source(successful_run):
    vm = successful_run
    assert vm.events() == ["PREPARING", "RUNNING baseline", "RUNNING candidate", "SUCCEEDED"]
    assert vm.calls("just") == [
        "just benchmark-tools baseline source", "just benchmark-build baseline source",
        "just benchmark-measure baseline source",
        "just benchmark-build candidate candidate", "just benchmark-measure candidate candidate"]
    assert tool.maintenance_events(vm.state) == []
    assert tool.uncontrolled([vm.result("baseline"), vm.result("candidate")]) == []
    assert "chown -R root: " + str(vm.input) + " " + str(vm.state) in vm.calls("chown")
    # The retained source patch includes the planned commit and the files that a variant adds.
    baseline = (vm.result("baseline") / "measurement-source.patch").read_text()
    candidate = (vm.result("candidate") / "measurement-source.patch").read_text()
    assert "+planned" in baseline
    assert "added.txt" not in baseline
    assert "+planned" in candidate
    assert "+added" in candidate
    assert "+candidate" in candidate


@pytest.mark.parametrize("variant", ["baseline", "candidate"])
def test_successful_run_retains_every_result_file_and_the_manifest(successful_run, source, variant):
    vm = successful_run
    runner = hashlib.sha256(RUNNER.read_bytes()).hexdigest()
    patch_hash = hashlib.sha256(source.patches["candidate"].read_bytes()).hexdigest() if variant == "candidate" else ""
    result = vm.result(variant)
    assert RESULT_FILES <= {path.name for path in result.iterdir()}
    assert (result / "benchmark/result.json").is_file()
    assert vm.gate(variant)[-1].split()[1:3] == ["gate", "passed"]
    assert vm.gate(variant, "load-after-build")[-1].split()[1:3] == ["gate", "passed"]
    assert "chown benchmark: " + str(result) in vm.calls("chown")
    manifest = tool.manifest(result)
    assert {key: manifest[key] for key in (
        "run.id", "variant", "gce.zone", "gce.instance", "gce.machine.type", "gce.image",
        "gce.scheduling", "os.release", "cpu.model", "memory.kib", "java.version",
        "java.tool", "just.version", "thrift.version", "runner.sha256",
        "variant.patch.sha256", "boot.id", "source.revision", "source.base")} == {
        "run.id": "offline-1", "variant": variant, "gce.zone": "asia-northeast1-b",
        "gce.instance": "offline-pb4", "gce.machine.type": "c3-standard-8",
        "gce.image": "ubuntu-2404-noble-amd64-v20261001", "gce.scheduling": "TERMINATE",
        "os.release": "Ubuntu 24.04.3 LTS", "cpu.model": "Intel(R) Xeon(R) Platinum 8481C",
        "memory.kib": "32864356", "java.version": 'openjdk version "17.0.16" 2025-07-15',
        "java.tool": TOOLS["java"], "just.version": "just 1.40.0",
        "thrift.version": "Thrift version 0.25.0", "runner.sha256": runner,
        "variant.patch.sha256": patch_hash, "boot.id": "boot-1",
        "source.revision": source.revision, "source.base": source.base}
    assert (result / "source-base.txt").read_text().strip() == source.base


def test_gate_resets_on_load_and_swap_and_waits_for_a_low_sample_to_leave_the_window(new_vm):
    # In the first gate, a load of 10, a swap-out, and a swap-in above 1 MiB each follow nine good
    # samples, so only a reset delays the pass to the 40th sample; a 1 MiB swap-in is still good.
    good = [(99, 0.5, 0, 0)] * 9
    samples = good + [(99, 10.0, 0, 0)] + good + [(99, 0.5, 0, 1)] + good + [(99, 0.5, 257, 0)]
    samples += good + [(99, 0.5, 256, 0)]
    # In the second, an 84% sample keeps the gate closed until it leaves the window of ten.
    samples += [(84, 0.5, 0, 0)] + good + [(99, 0.5, 0, 0)]
    vm = finished(new_vm(samples=samples), "SUCCEEDED")
    before = vm.gate("baseline")
    assert [line.split()[-1] for line in before[:40]] == [
        "sample_ok=" + ("0" if index in (9, 19, 29) else "1") for index in range(40)]
    assert "swapin_bytes=1048576" in before[39]
    assert before[40:] == [before[40].split()[0] + " gate passed mean_idle=99 min_idle=99.00"]
    after = vm.gate("baseline", "load-after-build")
    assert len(after) == 12
    assert after[11].split()[1:] == ["gate", "passed", "mean_idle=99", "min_idle=99.00"]


def test_gate_passes_at_its_mean_and_minimum_boundaries(new_vm):
    # The first gate's first window averages 89.9; the second gate's averages 90 with a minimum of 85.
    samples = [(89, 0.5, 0, 0)] * 9 + [(98, 0.5, 0, 0), (99, 0.5, 0, 0)]
    samples += [(85, 0.5, 0, 0)] * 5 + [(95, 0.5, 0, 0)] * 5
    vm = finished(new_vm(samples=samples), "SUCCEEDED")
    before = vm.gate("baseline")
    assert len(before) == 12
    assert before[11].split()[1:3] == ["gate", "passed"]
    after = vm.gate("baseline", "load-after-build")
    assert len(after) == 11
    assert after[10].split()[1:] == ["gate", "passed", "mean_idle=90", "min_idle=85.00"]


def test_gate_timeout_fails_the_run_before_building(new_vm):
    vm = finished(new_vm(settings="GATE_TIMEOUT_MINUTES=0\n"), "FAILED baseline")
    assert vm.gate("baseline")[-1].split()[1:] == ["gate", "timed", "out"]
    assert vm.calls("just benchmark-build") == []


def test_event_during_root_only_steps_stops_benchmark_work_launched_after_it(new_vm):
    # Preparation does not check between steps; the watcher must stop the mise install, which
    # would otherwise block, possibly before the stub records its start.
    vm = interrupted(new_vm(actions={"apt-get": "event", "mise-install": "block"}))
    assert (vm.state / "tools.log").is_file()
    assert vm.calls("just") == []
    assert not (vm.results.exists() and any(vm.results.iterdir()))


@pytest.mark.parametrize("observation", [1, 10], ids=["observation-1", "observation-10"])
def test_event_during_the_gate_stops_it_and_withholds_its_pass(new_vm, observation):
    vm = interrupted(new_vm(event_at=observation))
    gate = vm.gate("baseline")
    assert len(gate) == observation
    assert all("sample_ok=1" in line for line in gate)
    assert vm.calls("just benchmark-build") == []


def stops_before_measuring(vm):
    assert vm.calls("just benchmark-measure") == []
    assert not (vm.result("baseline") / "load-after-build.log").exists()


def records_the_end(vm):
    assert (vm.result("baseline") / "end-utc.txt").is_file()


def records_one_interruption(vm):
    assert vm.events() == ["PREPARING", "RUNNING baseline", INTERRUPTED]


def records_the_interruption_again(vm):
    assert vm.events() == ["PREPARING", "RUNNING baseline", INTERRUPTED, "RUNNING baseline", INTERRUPTED]


def stops_the_measurement_and_later_variants(vm):
    assert not (vm.result("baseline") / "end-utc.txt").exists()
    assert not vm.result("candidate").exists()


@pytest.mark.parametrize(("options", "check"), [
    # The process returns.
    ({"actions": {"benchmark-build": "event"}}, stops_before_measuring),
    # The measurement returns.
    ({"actions": {"benchmark-measure": "event"}}, records_the_end),
    # The runner stops the watcher before it records the interruption.
    ({"hold_watcher": True, "actions": {"benchmark-build": "event"}}, records_one_interruption),
    # A runner status update replaces the watcher's status file.
    ({"actions": {"benchmark-build": "event-status-replaced"}}, records_one_interruption),
    # A runner status update follows the watcher's event.
    ({"actions": {"benchmark-build": "event-logged-late"}}, records_the_interruption_again),
    # The process keeps running.
    ({"variants": ("baseline", "candidate"), "actions": {"benchmark-measure": "event-block"}},
     stops_the_measurement_and_later_variants),
], ids=["process-returns", "measurement-returns", "watcher-stopped", "status-replaced", "status-follows-event",
        "process-keeps-running"])
def test_event_during_a_benchmark_process_ends_the_run_at_the_next_step(new_vm, options, check):
    check(interrupted(new_vm(**options)))


def test_unavailable_metadata_requests_are_logged_and_retried(new_vm):
    # The first two requests fail, and so does the first held request after the third returns.
    vm = new_vm(unavailable=(1, 2, 4), watched=4, actions={"apt-get": "watched"})
    finished(vm, "SUCCEEDED")
    log = (vm.state / "maintenance-events.log").read_text().splitlines()
    assert [line.split()[-1] for line in log] == ["UNAVAILABLE", "UNAVAILABLE", "NONE", "UNAVAILABLE"]
    assert tool.maintenance_events(vm.state) == []
    requests = [path for path in vm.server.paths if path.startswith("/instance/maintenance-event")]
    held = "/instance/maintenance-event?wait_for_change=true&last_etag=etag-1"
    assert requests[:3] == ["/instance/maintenance-event"] * 3
    assert set(requests[3:]) == {held}, "A retry after a failed held request lost the ETag"


# Each case names the first call that the failure must prevent and a file it must leave.
@pytest.mark.parametrize(("options", "status", "unreached", "kept"), [
    ({"actions": {"apt-get.update": "fail"}}, "FAILED prepare", "apt-get install", None),
    ({"actions": {"apt-get.install": "fail"}}, "FAILED prepare", "mise", None),
    ({"checksum": "0" * 64}, "FAILED prepare", "mise", None),
    ({"actions": {"mise-install": "fail"}}, "FAILED prepare", "mise where", None),
    ({"variants": ("baseline", "candidate"), "patches": {"candidate": "broken"}},
     "FAILED candidate", "just benchmark-build candidate", "baseline/end-utc.txt"),
    ({"actions": {"benchmark-tools": "fail"}}, "FAILED baseline", "just benchmark-build", None),
    ({"metadata": {key: value for key, value in METADATA.items() if key != "image"}},
     "FAILED baseline", "just benchmark-build", None),
    ({"actions": {"benchmark-build": "fail"}}, "FAILED baseline", "just benchmark-measure", "baseline/build.log"),
    ({"variants": ("baseline", "candidate"), "actions": {"benchmark-measure.baseline": "fail"}},
     "FAILED baseline", "just benchmark-build candidate", "baseline/start-utc.txt"),
    ({"variants": ("baseline", "candidate"), "actions": {"benchmark-measure": "reboot"}},
     "FAILED baseline", "just benchmark-build candidate", "baseline/end-utc.txt"),
], ids=["package-index", "package-installation", "mise-checksum", "tool-installation", "checkout", "thrift-build",
        "metadata-lookup", "build", "measurement", "boot-id"])
def test_failures_record_the_step_in_progress(new_vm, options, status, unreached, kept):
    vm = finished(new_vm(**options), status)
    assert vm.calls(unreached) == [], vm.report()
    if kept:
        variant, file = kept.split("/")
        assert (vm.result(variant) / file).is_file(), kept
