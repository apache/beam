## The `wrapper-validation` action

The `wrapper-validation` action validates the checksums of _all_ [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) JAR files present in the repository and fails if any unknown Gradle Wrapper JAR files are found.

The action should be run in the root of the repository, as it will recursively search for any files named `gradle-wrapper.jar`.

> [!NOTE]
> Starting with v4 the `setup-gradle` action will automatically [perform wrapper validation](../docs/setup-gradle.md#gradle-wrapper-validation)
> on each execution.
> 
> If you are using `setup-gradle` in your workflows, it is unlikely that you will need to use the `wrapper-validation` action.

### Example workflow

```yaml
name: "Validate Gradle Wrapper"

on:
  push:
  pull_request:

jobs:
  validation:
    name: "Validation"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: gradle/actions/wrapper-validation@v4
```

See the [full action documentation](../docs/wrapper-validation.md) for more advanced usage scenarios.
