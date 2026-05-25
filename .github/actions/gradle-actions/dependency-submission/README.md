## The `dependency-submission` action

Generates and submits a dependency graph for a Gradle project, allowing GitHub to alert about reported vulnerabilities in your project dependencies.

The following workflow will generate a dependency graph for a Gradle project and submit it immediately to the repository via the
Dependency Submission API. For most projects, this default configuration should be all that you need.

Simply add this as a new workflow file to your repository (eg `.github/workflows/dependency-submission.yml`).

```yaml
name: Dependency Submission

on:
  push:
    branches: ['main']

permissions:
  contents: write

jobs:
  dependency-submission:
    runs-on: ubuntu-latest
    steps:
    - name: Checkout sources
      uses: actions/checkout@v4
    - name: Setup Java
      uses: actions/setup-java@v4
      with:
        distribution: 'temurin'
        java-version: 17
    - name: Generate and submit dependency graph
      uses: gradle/actions/dependency-submission@v4
```

See the [full action documentation](../docs/dependency-submission.md) for more advanced usage scenarios.
