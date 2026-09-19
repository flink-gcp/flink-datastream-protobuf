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
"""Render marked Example sources through the pinned theme, without Maven substitutes."""

import os
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
JAVA = "examples/datastream/src/main/java/io/github/flink/gcp/protobuf/examples/ExplicitExample.java"


class Text(HTMLParser):
    def __init__(self, value):
        super().__init__()
        self.parts = []
        self.feed(value)

    def handle_data(self, value):
        self.parts.append(value)


@pytest.fixture
def site(tmp_path):
    docs = tmp_path / "docs"
    (docs / "content").mkdir(parents=True)
    (docs / "content/docs").mkdir()
    (docs / "content/docs/_index.md").write_text("---\ntitle: Documentation\n---\n")
    for name in ("hugo.toml", "go.mod", "go.sum"):
        shutil.copyfile(ROOT / "docs" / name, docs / name)
    shutil.copytree(ROOT / "docs/layouts", docs / "layouts")
    (tmp_path / "LICENSE").write_text("Fixture license")
    (docs / "content/_index.md").write_text(
        '---\ntitle: Fixture\n---\n'
        '{{< example "ExplicitExample#configuration" >}}\n\n'
        '{{< example "config.yaml#configuration" >}}\n\n'
        '```text {filename="plain.txt"}\nORDINARY_BLOCK\n```\n'
    )
    java = tmp_path / JAVA
    java.parent.mkdir(parents=True)
    java.write_text(
        '// excluded wrapper\n    // docs:start configuration\n'
        '    var message = "SOURCE_JAVA";\n'
        '    if (true) {\n        use(message);\n    }\n'
        '    // docs:end configuration\n// excluded footer\n'
    )
    config = tmp_path / "examples/datastream/config/config.yaml"
    config.parent.mkdir(parents=True)
    config.write_text(
        '# excluded header\n# docs:start configuration\n'
        'message: SOURCE_YAML\n# docs:end configuration\n'
    )
    return tmp_path


def render(site, version="development"):
    return subprocess.run(
        ["hugo", "--source", str(site / "docs"), "--minify", "--panicOnWarning"],
        env=dict(os.environ, HUGO_BASEURL=f"https://example.org/project/{version}/"),
        text=True, capture_output=True, check=False,
    )


@pytest.mark.parametrize("version", ["development", "1.0"])
def test_site_renders_its_own_source_without_copied_markdown(site, version):
    markdown = (site / "docs/content/_index.md").read_text()
    assert "SOURCE_" not in markdown
    java = site / JAVA
    java.write_text(java.read_text().replace("SOURCE_JAVA", f"SOURCE_{version}"))
    result = render(site, version)
    assert result.returncode == 0, result.stdout + result.stderr
    text = "".join(Text((site / "docs/public/index.html").read_text()).parts)
    assert f'SOURCE_{version}' in text
    assert "message: SOURCE_YAML" in text
    assert "excluded wrapper" not in text and "docs:start" not in text
    assert "if (true) {\n    use(message);\n}" in text
    assert "ORDINARY_BLOCK" in text and "plain.txt" in text
    java.write_text(java.read_text().replace(f"SOURCE_{version}", "UPDATED_SOURCE"))
    result = render(site, version)
    assert result.returncode == 0, result.stdout + result.stderr
    updated = "".join(Text((site / "docs/public/index.html").read_text()).parts)
    assert "UPDATED_SOURCE" in updated and f"SOURCE_{version}" not in updated
    assert (site / "docs/content/_index.md").read_text() == markdown
    assert not list((site / "docs/public").rglob("*.java"))
    public = site / "docs/public"
    assert (public / "favicon.svg").is_file()
    assert (public / "fuse.min.mjs").is_file()
    for unused in ("katex", "asciinema", "mermaid.min.js"):
        assert not (public / unused).exists()


@pytest.mark.parametrize("source, diagnostic", [
    (None, "was not found"),
    ("no markers", "exactly one start marker"),
    ("// docs:start configuration\nbody", "exactly one end marker"),
    ("// docs:start configuration\n// docs:start configuration\nbody\n// docs:end configuration", "exactly one start marker"),
    ("// docs:start configuration\nbody\n// docs:end configuration\n// docs:end configuration", "exactly one end marker"),
    ("// docs:end configuration\nbody\n// docs:start configuration", "end marker before"),
    ("// docs:start configuration\n   \n// docs:end configuration", "empty region"),
])
def test_invalid_regions_fail_site_generation(site, source, diagnostic):
    java = site / JAVA
    if source is None:
        java.unlink()
    else:
        java.write_text(source)
    result = render(site)
    assert result.returncode != 0
    assert diagnostic in result.stdout + result.stderr


@pytest.mark.parametrize("identity", ["../ExplicitExample#configuration", "ExplicitExample", "Other#missing"])
def test_invalid_or_missing_references_fail(site, identity):
    page = site / "docs/content/_index.md"
    page.write_text(page.read_text().replace("ExplicitExample#configuration", identity))
    result = render(site)
    assert result.returncode != 0
    assert identity in result.stdout + result.stderr
