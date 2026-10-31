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

"""Credential patterns that the evidence gates reject in retained and published files."""

# These deliberately bounded patterns supplement human review, not arbitrary-secret detection.
CREDENTIAL_RULES = {
    "private-key": r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----",
    "github-token": r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b",
    "aws-access-key": r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
    "google-api-key": r"\bAIza[A-Za-z0-9_-]{35}\b",
    "google-oauth": r"\bya29\.[A-Za-z0-9_-]{20,}",
    "slack-token": r"\bxox[baprs]-[A-Za-z0-9-]{10,}",
    "authorization": r"(?i)\b(?:authorization\s*[:=]\s*|bearer\s+|basic\s+)[\"']?(?:bearer\s+|basic\s+)?[A-Za-z0-9+/_=.-]{8,}",
    "credential-assignment": r"(?i)(?:[\"']?(?:\b|-D)(?:[A-Za-z0-9]{1,64}[_.-]){0,8}(?:password|passwd|token|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|secret[_-]?access[_-]?key|private[_-]?key)\b[\"']?\s*[:=]\s*[\"']?)[^\s\"',;}]{4,}",
}
