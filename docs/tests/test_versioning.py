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
"""Render synthetic Hugo sites and check the published navigation JavaScript."""

import importlib.util
import json
import os
import re
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


class ScriptLinks(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.sources = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        if tag == "script" and "src" in dict(attrs):
            self.sources.append(dict(attrs)["src"])


@pytest.mark.parametrize(
    "line,kind,label",
    [
        ("1.1", "release", "1.1.0"),
        ("1.2", "release", "1.2.0"),
        ("0.1", "milestone", "0.1 (development milestone)"),
    ],
)
def test_build_scopes_the_real_theme_search_to_its_version(
    tmp_path, monkeypatch, line, kind, label
):
    spec = importlib.util.spec_from_file_location(
        "docs_site", ROOT / "scripts/docs-site.py"
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    source = tmp_path / "source"
    docs = source / "docs"
    (docs / "content/docs").mkdir(parents=True)
    (docs / "content/_index.md").write_text(
        "---\ntitle: Home\n---\nThis is **{{< param DocsVersion >}}** documentation.\n"
    )
    (docs / "content/docs/page.md").write_text(
        f"---\ntitle: Version {line}\n---\nSearch content for {line}.\n"
    )
    for name in ("go.mod", "go.sum"):
        shutil.copyfile(ROOT / "docs" / name, docs / name)
    (docs / "hugo.toml").write_text(
        'baseURL = "https://example.org/unused/"\n'
        'disableKinds = ["taxonomy", "term"]\n'
        '[params]\nBookRepo = "https://github.com/example/connector"\n'
        'BookEditLink = "https://github.com/example/connector/edit/main/docs/{{ .Path }}"\n'
        "[module]\n[[module.imports]]\n"
        'path = "github.com/flink-gcp/flink-gcp-dev-tools/hugo"\n'
    )
    # Stub source checkout validation and Maven/Javadoc work. The production
    # build() hands its actual environment to the real pinned Hugo/theme here.
    monkeypatch.setattr(helper, "validate_source", lambda *args: None)

    def run(*args, cwd, env):
        assert args[-1] == "docs-validate"
        api = docs / "static/api/java"
        api.mkdir(parents=True)
        (api / "index.html").write_text(
            "<!doctype html><html><head></head><body>API</body></html>"
        )
        subprocess.run(
            ["hugo", "--source", str(docs), "--minify", "--panicOnWarning"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )

    monkeypatch.setattr(helper, "run", run)
    version = {
        "id": line,
        "kind": kind,
        "label": label,
        "ref": "v" + line + ".0",
        "sha": "a" * 40,
    }
    manifest = {"base_url": "https://example.org/project/", "versions": [version]}
    helper.build(manifest, line, source, tmp_path / "output")
    public = tmp_path / "output" / line
    assert json.loads((public / "docs-build.json").read_text()) == version
    prefix = "/project/" + line + "/"
    home = (public / "index.html").read_text()
    assert f"<strong>{label}</strong>" in home
    text = (public / "docs/page/index.html").read_text()
    assert "/edit/main/" not in text
    scripts = [url for url in ScriptLinks(text).sources if ".search." in url]
    assert len(scripts) == 1 and scripts[0].startswith(prefix)
    search = (public / scripts[0].removeprefix(prefix)).read_text()
    indexes = list(public.glob("*.search-data.*.json"))
    assert len(indexes) == 1
    assert prefix + indexes[0].name in search
    records = json.loads(indexes[0].read_text())
    assert records and all(record["href"].startswith(prefix) for record in records)
    assert any(
        record["href"] == prefix + "docs/page/"
        and f"content for {line}" in record["content"]
        for record in records
    )
    assert "/unused/" not in text + search
    # Check the insertion against the real pinned theme, including its minified
    # sidebar markup, rather than relying only on synthetic HTML shapes.
    manifest.update(repository="example/connector", current=line)
    decorated = helper.decorate(
        text,
        manifest,
        version,
        Path("docs/page/index.html"),
        {line: {Path("docs/page/index.html")}},
    )
    assert decorated.index("</nav>") < decorated.index('class="docs-version"')
    assert decorated.index('class="docs-version"') < decorated.index("</aside>")
    assert f'href="{prefix}api/java/">API reference (Javadoc)</a>' in decorated


@pytest.mark.parametrize(
    "path", ["/", "/project/", "/project/1.0/", "/project/development/"]
)
def test_hugo_links_follow_the_selected_base_url(tmp_path, path):
    for directory in ("content", "layouts"):
        (tmp_path / directory).mkdir()
    (tmp_path / "content/_index.md").write_text(
        '---\ntitle: Home\n---\n[API]({{< api-docs-url >}})\n[Other]({{< relref "other" >}})\n'
    )
    (tmp_path / "content/other.md").write_text("---\ntitle: Other\n---\nExample\n")
    (tmp_path / "layouts/home.html").write_text(
        "<!doctype html><html><body>{{ .Content }}</body></html>"
    )
    (tmp_path / "layouts/single.html").write_text(
        "<!doctype html><html><body>{{ .Content }}</body></html>"
    )
    shortcode = tmp_path / "api-docs-url.html"
    shortcode.write_text('{{ "api/java/" | relURL }}')
    (tmp_path / "hugo.toml").write_text(
        'baseURL = "https://example.org/unused/"\n'
        'disableKinds = ["taxonomy", "term"]\n'
        '[module]\n[[module.mounts]]\nsource = "content"\ntarget = "content"\n'
        '[[module.mounts]]\nsource = "layouts"\ntarget = "layouts"\n'
        f"[[module.mounts]]\nsource = {json.dumps(str(shortcode))}\n"
        'target = "layouts/_shortcodes/api-docs-url.html"\n'
    )
    result = subprocess.run(
        ["hugo", "--source", str(tmp_path), "--panicOnWarning"],
        check=False,
        env=dict(os.environ, HUGO_BASEURL="https://example.org" + path),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    text = (tmp_path / "public/index.html").read_text()
    assert f'href="{path}api/java/"' in text
    assert f'href="{path}other/"' in text
    assert "/unused/" not in text


def test_legacy_redirect_preserves_encoded_query_and_api_fragment():
    spec = importlib.util.spec_from_file_location(
        "docs_site", ROOT / "scripts/docs-site.py"
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    page = helper.redirect("/project/1.0/api/java/Type.html")
    script = page.split("<script>", 1)[1].split("</script>", 1)[0]
    # The script appends the current query and fragment to the target unchanged, so
    # `?q=A%26B` stays encoded and a Javadoc member fragment reaches the API page.
    assert script == (
        'location.replace("/project/1.0/api/java/Type.html"+location.search+location.hash)'
    )


EXPECTED_DOCS_VERSION = (
    "document.querySelectorAll('.docs-version select').forEach((select) => {"
    " const current = select.querySelector('option[selected]').value;"
    " window.addEventListener('pageshow', () => { select.value = current; });"
    " select.addEventListener('change', () => {"
    " const option = select.selectedOptions[0];"
    " const target = new URL(option.value, location.href);"
    " if (option.hasAttribute('data-same-page')) {"
    " target.search = location.search;"
    " target.hash = location.hash;"
    " }"
    " location.assign(target.href);"
    " });"
    " select.hidden = false;"
    " });"
)


def normalized_script(source):
    """Drop the leading license block and comment lines, then collapse whitespace."""
    license_block = re.match(r"/\*.*?\*/", source, flags=re.S)
    assert license_block, "docs-version.js no longer starts with its license block"
    lines = source[license_block.end():].splitlines()
    code = [line for line in lines if not line.lstrip().startswith("//")]
    return " ".join(" ".join(code).split())


def test_docs_version_script_is_the_reviewed_script():
    script = normalized_script((ROOT / "docs/versioning/docs-version.js").read_text())
    assert script == EXPECTED_DOCS_VERSION, (
        "docs/versioning/docs-version.js changed. Check that it keeps these behaviors, "
        "then update EXPECTED_DOCS_VERSION in the same diff: each select is shown only "
        "after the script runs; only options with data-same-page keep the query and "
        "fragment; pageshow restores the selection rendered with the page."
    )
