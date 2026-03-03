<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache Beam Skills

This directory contains skills that help the agent perform specialized tasks in the Apache Beam codebase. For more information, see the [Agent Skills Documentation](http://antigravity.google/docs/skills).

## Available Skills

| Skill | Description |
|-------|-------------|
| [beam-concepts](beam-concepts/SKILL.md) | Core Beam programming model (PCollections, PTransforms, windowing, triggers) |
| [ci-cd](ci-cd/SKILL.md) | GitHub Actions workflows, debugging CI failures, triggering tests |
| [contributing](contributing/SKILL.md) | PR workflow, issue management, code review, release cycles |
| [gradle-build](gradle-build/SKILL.md) | Build commands, flags, publishing, troubleshooting |
| [io-connectors](io-connectors/SKILL.md) | 51+ I/O connectors, testing patterns, usage examples |
| [java-development](java-development/SKILL.md) | Java SDK development, building, testing, project structure |
| [license-compliance](license-compliance/SKILL.md) | Apache 2.0 license headers for all new files |
| [python-development](python-development/SKILL.md) | Python SDK environment setup, testing, building pipelines |
| [runners](runners/SKILL.md) | Direct, Dataflow, Flink, Spark runner configuration |

## How Skills Work

1. **Discovery**: The agent scans skill descriptions to find relevant ones
2. **Activation**: When a skill matches the task, the agent reads the full `SKILL.md`
3. **Execution**: The agent follows the skill's instructions

## Skill Structure

Each skill folder contains:
- `SKILL.md` - Main instruction file with YAML frontmatter

```yaml
---
name: skill-name
description: Concise description for when to use this skill
---
# Skill Content
Detailed instructions...
```

## Adding New Skills

1. Create a new folder under `.agent/skills/`
2. Add a `SKILL.md` with YAML frontmatter (`name`, `description`)
3. Write clear, actionable instructions in the markdown body
