# Apache Beam Skills

This directory contains skills that help the agent perform specialized tasks in the Apache Beam codebase.

## Available Skills

| Skill | Description |
|-------|-------------|
| [beam-concepts](beam-concepts/SKILL.md) | Core Beam programming model (PCollections, PTransforms, windowing, triggers) |
| [ci-cd](ci-cd/SKILL.md) | GitHub Actions workflows, debugging CI failures, triggering tests |
| [contributing](contributing/SKILL.md) | PR workflow, issue management, code review, release cycles |
| [gradle-build](gradle-build/SKILL.md) | Build commands, flags, publishing, troubleshooting |
| [io-connectors](io-connectors/SKILL.md) | 51+ I/O connectors, testing patterns, usage examples |
| [java-development](java-development/SKILL.md) | Java SDK development, building, testing, project structure |
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
