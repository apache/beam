## Building

The `build` script in the project root provides a convenient way to perform many local build tasks:
1. `./build` will lint and compile typescript sources
2. `./build all` will lint and compile typescript and run unit tests
3. `./build install` will install npm packages followed by lint and compile
4. `./build init-scripts` will run the init-script integration tests
5. `./build act <act-commands>` will run `act` after building local changes (see below)

## Using `act` to run integ-test workflows locally

It's possible to run GitHub Actions workflows locally with https://nektosact.com/.
Many of the test workflows from this repository can be run in this way, making it easier to
test local changes without pushing to a branch.

This feature is most useful to run a single `integ-test-*` workflow. Avoid running `ci-quick-test` or other aggregating workflows unless you want to use your local machine as a heater!

Example running a single workflow:
`./build act -W .github/workflows/integ-test-caching-config.yml`

Example running a single job:
`./build act -W .github/workflows/integ-test-caching-config.yml -j cache-disabled-pre-existing-gradle-home`

Known issues:
- `integ-test-detect-java-toolchains.yml` fails when running on a `linux/amd64` container, since the expected pre-installed JDKs are not present. Should be fixed by #89.
- `act` is not yet compatible with `actions/upload-artifact@v4` (or related toolkit functions)
    - See https://github.com/nektos/act/pull/2224
- Workflows run by `act` cannot submit to the dependency-submission API, as no `GITHUB_TOKEN` is available by default.

Tips:
- Add the following lines to `~/.actrc`:
    - `--container-daemon-socket -` : Prevents "error while creating mount source path", and yes that's a solitary dash at the end
    - `--matrix os:ubuntu-latest` : Avoids a lot of logging about unsupported runners being skipped
- Runners don't have `java` installed by default, so all workflows that run Gradle require a `setup-java` step.
