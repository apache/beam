Hullo @apache 👋

I ran your skills through `tessl skill review` at work and found some targeted improvements. Here's the full before/after:

| Skill | Before | After | Change |
|-------|--------|-------|--------|
| license-compliance | 70% | 96% | +26% |
| gradle-build | 77% | 96% | +19% |
| io-connectors | 77% | 94% | +17% |
| beam-concepts | 77% | 90% | +13% |
| contributing | 81% | 94% | +13% |
| python-development | 77% | 90% | +13% |
| ci-cd | 81% | 89% | +8% |
| java-development | 83% | 90% | +7% |
| runners | 85% | 90% | +5% |

![Score Card](score_card.png)

<details>
<summary>Changes summary</summary>

**Descriptions (all 9 skills)**
- Expanded action verbs beyond generic "Guides understanding" to specific actions like "Configures, debugs, implements"
- Added natural trigger terms users would actually type (e.g., "build.gradle", "gradlew", "pull request", "CLA", "RAT check")
- Ensured every description has an explicit "Use when..." clause with multiple trigger scenarios

**beam-concepts**: Removed explanatory prose Claude already knows (historical context, property definitions), tightened PCollection/PTransform descriptions, added verification step to Dead Letter Queue pattern

**ci-cd**: Replaced verbose workflow tables with compact naming convention reference, added concrete `gh` CLI commands for listing/rerunning workflows, added executable debugging workflow with copy-paste ready commands

**contributing**: Added validation checkpoint to run pre-commit tests locally before pushing, expanded trigger terms to include "pull request", "CLA", "how to contribute"

**gradle-build**: Replaced flat error list with structured troubleshooting workflow including explicit verification steps for each error type

**io-connectors**: Replaced bare component list for creating new connectors with step-by-step workflow including test and verification checkpoints

**java-development**: Added artifact verification step after `publishToMavenLocal`

**license-compliance**: Added explicit 5-step compliance workflow with RAT check validation loop, consolidated repetitive license headers (8 near-identical blocks) into grouped format by comment style

**python-development**: Added tarball verification step after building source distribution

**runners**: Added structured debugging workflow: start with DirectRunner to isolate logic errors, then escalate to target runner

</details>

---

 - [x] No issue referenced (skill improvements only, no functional code changes)
 - [ ] Update `CHANGES.md` with noteworthy changes. *(N/A — skill files only)*
 - [ ] If this contribution is large, please file an Apache [Individual Contributor License Agreement](https://www.apache.org/licenses/icla.pdf). *(Small contribution — skill metadata and content improvements only)*

---

Honest disclosure — I work at @tesslio where we build tooling around skills like these. Not a pitch - just saw room for improvement and wanted to contribute.

Want to self-improve your skills? Just point your agent (Claude Code, Codex, etc.) at [this Tessl guide](https://docs.tessl.io/evaluate/optimize-a-skill-using-best-practices) and ask it to optimize your skill. Ping me - [@popey](https://github.com/popey) - if you hit any snags.

Thanks in advance 🙏
