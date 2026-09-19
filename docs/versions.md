---
title: Documentation versions
weight: 70
---
<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Documentation versions

Before 1.0.0, the site contains only Development from `main`.
Unpublished 0.x milestones do not receive separate sites or selector entries.
Development documentation does not imply that artifacts are available on Maven Central.
Existing development evidence and upgrade guidance remain in the source tree.

From 1.0.0, the site keeps the latest patch of the current and previous published minor series, plus Development.
For example, 1.2.1 and the latest 1.1.x appear together; a later 1.1.x patch does not make that series current.
The two-series window crosses major boundaries: 2.0 and the last published 1.x minor occupy its slots.
The Flink 1.20 artifact suffix does not create another documentation version.

Each release uses its exact tagged source, dependencies, examples, and Javadoc.
The Source link identifies the resolved commit.
The API reference uses the default Flink 2.x / Protobuf 3 build; both adapters and Protobuf profiles remain checked in CI.
Release pages have no edit-on-main link.

URLs such as `1.0/` track the latest patch in that minor.
Unversioned pages redirect to the current release when it contains that page, or to Development before the first release.
A page with no current counterpart returns the root 404 page.
The version selector preserves the page, query, and fragment when a corresponding page exists; otherwise it opens that version's homepage.
Search stays within the selected version.
On mobile, the API link and selector are inside the menu drawer.
Without JavaScript, ordinary version links remain available.

When a series leaves the window, its hosted pages disappear.
The [release archive](https://github.com/flink-gcp/flink-datastream-protobuf/releases) retains links to its tagged documentation.
Hosting documentation does not promise continued maintenance of that library version.
Development pages are marked `noindex`.

[Site licenses](licenses.md) retain the design and third-party notices.
