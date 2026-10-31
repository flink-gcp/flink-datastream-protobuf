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

import 'dev-tools.just'
import 'benchmarks.just'

set shell := ["bash", "-euo", "pipefail", "-c"]

dev_tools_revision := "c3784bce1f95dc75848b1fc2de0f370e5c0a09ea"

mvn := "./mvnw -ntp"

# The standard-library tools run in a temporary environment without the test dependencies.
# Call uv from PATH: a nested `mise x` would replace the caller's JDK for Java subprocesses.
python := "uv run --locked --isolated --no-dev python"

# Show the available development commands.
default:
    @just --list

# Refresh the four shared workflow skills from the reviewed upstream commit.
skills-sync: (install-skills dev_tools_revision)

# Run Maven verification; arguments precede the final lifecycle phase.
[positional-arguments]
verify *args:
    {{ mvn }} "$@" verify

# Clean when switching generated code and runtime between Protobuf majors.
[positional-arguments]
verify-protobuf major *args:
    test "{{ major }}" = 3 || test "{{ major }}" = 4
    {{ mvn }} clean -Pprotobuf{{ major }} "${@:2}" verify

# Select the matching source adapter and clean the paired Protobuf build.
[positional-arguments]
verify-flink version major="4":
    bash scripts/verify-flink.sh "$1" "$2"

# Build a line's jar with Protobuf 4, then run it unchanged on the 2.x ceiling and with Protobuf 3 gencode and runtime.
[positional-arguments]
binary-compat line ceiling="":
    {{ python }} scripts/binary-compat.py "$1" "$2"

# Capture an unpublished development bundle from a clean committed checkout on JDK 17.
[positional-arguments]
baseline-capture line major output:
    java src/test/java/io/github/flink/gcp/protobuf/BaselineTool.java capture "$1" "$2" "$3"

# Check a retained bundle without Maven, recompilation, or network access.
[positional-arguments]
baseline-check bundle:
    java -cp "$1/tools.jar" io.github.flink.gcp.protobuf.BaselineTool check "$1"

# Download, verify, and extract retained release bundles into a new directory outside the repository.
# Arguments after the output are bundle names and an optional --archives <directory>.
[positional-arguments]
baseline-fetch tag output *args:
    {{ python }} scripts/baseline-fetch.py fetch "$@"

# Exercise baseline retrieval with synthetic releases only.
baseline-fetch-test: (test-scripts "scripts/tests/test_baseline_fetch.py")

# Read, review, assemble, or verify a milestone's retained bundles in an external work directory.
# See "Publish a milestone baseline" in docs/validation/development-baselines.md.
[positional-arguments]
baseline-release +args:
    {{ python }} scripts/baseline-release.py "$@"

# Exercise baseline release assembly with synthetic bundles only.
baseline-release-test: (test-scripts "scripts/tests/test_baseline_release.py")

# Run retained bytecode and state under the selected runtime and the current JDK.
[positional-arguments]
baseline-read bundle version output:
    java -cp "$1/tools.jar" io.github.flink.gcp.protobuf.BaselineTool read "$1" "$2" "$3"

# The run selects the bundle's Protobuf runtime and artifact line and resolves this build's dependencies.
# Restore a verified bundle's retained state with the current library and new-schema applications.
[positional-arguments]
baseline-upgrade bundle version:
    #!/usr/bin/env bash
    set -euo pipefail
    bundle=$(cd "$1" && pwd -P)
    line=$(sed -n 's/^line=//p' "$bundle/manifest.properties")
    major=$(sed -n 's/^protobuf.major=//p' "$bundle/manifest.properties")
    protobuf=$(sed -n 's/^protobuf.version=//p' "$bundle/manifest.properties")
    just baseline-check "$bundle"
    {{ mvn }} clean "-Pprotobuf$major" "-Dprotobuf.version=$protobuf" "-Dflink.compat=$line" "-Dflink.version=$2" \
        "-Dprotobuf.test.retained.bundle=$bundle" -Dtest.excluded.groups= \
        -Dtest=RetainedBaselineUpgradeITCase test

# Run a retained bundle's compiled consumers against a candidate library without recompiling them.
[positional-arguments]
baseline-consume bundle candidate version output:
    java src/test/java/io/github/flink/gcp/protobuf/BaselineTool.java consume "$1" "$2" "$3" "$4"

# Compare the annotated API with a retained bundle's library through japicmp.
[positional-arguments]
api-compat bundle:
    bash scripts/api-compat.sh "$1"

# Re-version this checkout and stage one artifact line on the Central Portal; run it only in a disposable tree.
[positional-arguments]
release-stage version *args:
    [[ "$1" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-1\.20)?$ ]] || { echo "Expected X.Y.Z or X.Y.Z-1.20" >&2; exit 1; }
    {{ mvn }} versions:set -DnewVersion="$1" -DgenerateBackupPoms=false
    {{ mvn }} clean deploy -Drelease -Dmaven.test.skip=true -Dmaven.install.skip=true {{ if version =~ '-1\.20$' { '-Dflink.compat=flink1' } else { '' } }} "${@:2}"

# Stage and inspect both lines' Central bundles from the committed HEAD without uploading them.
[positional-arguments]
release-rehearse version output *args:
    {{ python }} scripts/release-artifacts.py "$@"

# Capture a staging deployment ID, or inspect, publish, or drop Central Portal deployments (ADR-0006).
[positional-arguments]
central-portal +args:
    {{ python }} scripts/central-portal.py "$@"

# Run the release-artifact, Portal helper, and Release workflow checks without Maven or Central.
release-artifacts-test: (test-scripts "scripts/tests/test_release_artifacts.py" "scripts/tests/test_central_portal.py" "scripts/tests/test_release_workflow.py")

# Opt in to the isolated Chill comparison as well as the native probes.
probe-chill:
    {{ mvn }} clean -Pchill -Dtest.excluded.groups=retained-baseline verify

# Compare the Flink and Protobuf pins with Maven Central; fail when a newer release overtakes one.
upstream-versions:
    {{ python }} scripts/upstream-versions.py

# Exercise the upstream version check with synthetic pins and metadata only.
upstream-versions-test: (test-scripts "scripts/tests/test_upstream_versions.py")

# Run the scripts/tests suite, or only the given test files, through the locked pytest.
[positional-arguments]
test-scripts *tests:
    PYTHONDONTWRITEBYTECODE=1 uv run --locked pytest -v "$@"

# Apply the Java formatter before committing.
format:
    {{ mvn }} spotless:apply

# Lint workflows, shell scripts, and Markdown with the same tool versions as CI.
lint:
    mise x actionlint shellcheck -- actionlint
    mise x shellcheck -- shellcheck scripts/*.sh scripts/tests/*.sh
    mise x npm:markdownlint-cli2 -- markdownlint-cli2
    @# The development bundle archives every tracked file; pull requests can skip that capture.
    git ls-files -z | {{ python }} -c 'import sys, unicodedata; names = [(n, n.decode("utf-8", "replace")) for n in sys.stdin.buffer.read().split(b"\0") if n]; bad = [t for n, t in names if t.encode() != n or ":" in t or "\\" in t or any(unicodedata.category(c) == "Cc" for c in t)]; bad and print(*bad, sep="\n"); sys.exit("Rename tracked files that are not UTF-8 or contain a colon, backslash, or control character" if bad else 0)'

# Pin new or updated GitHub Actions to commit SHAs.
pin-actions:
    mise x pinact -- pinact run

# Verify the independent application consumer with a local development build, or with a published version from Maven Central.
[positional-arguments]
examples-verify version major repository published="":
    bash scripts/verify-examples.sh "$1" "$2" "$3" "$4"

# Generate the site's API reference in target/apidocs with the POM's Javadoc configuration, which the release Javadoc jar also uses.
[positional-arguments]
docs-javadoc compat="flink2" major="4":
    test "$1" = flink1 || test "$1" = flink2
    test "$2" = 3 || test "$2" = 4
    {{ mvn }} "-Dflink.compat=$1" "-Pprotobuf$2" clean javadoc:javadoc '-DreportOutputDirectory=${project.build.directory}'

# Render the existing Markdown with the pinned shared design.
docs:
    mise x hugo-extended go -- hugo --source docs --cleanDestinationDir --gc --minify --panicOnWarning
    {{ python }} scripts/docs-site.py check-sources docs/public

# Preview prose; run docs-validate first to include the generated API reference.
docs-serve:
    mise x hugo-extended go -- hugo server --source docs

# Validate launch versions and examples, generate the API reference, then render.
docs-validate:
    #!/usr/bin/env bash
    set -euo pipefail
    {{ mvn }} -Dtest=DocumentationExamplesTest test
    repository=$(mktemp -d)
    trap 'rm -rf "$repository"' EXIT
    floor=$({{ python }} -c 'import xml.etree.ElementTree as E; print(E.parse("pom.xml").getroot().findtext("{*}properties/{*}flink.version"))')
    just examples-verify "$floor" 4 "$repository"
    just docs-javadoc
    just docs-site prepare-api target/apidocs
    just docs

# Check release selection, provenance, rendering and navigation.
test-doc-versions:
    mise x hugo-extended go node python uv -- uv run --locked pytest docs/tests

# Plan, build and assemble independently built documentation versions.
[positional-arguments]
docs-site +args:
    {{ python }} scripts/docs-site.py "$@"
