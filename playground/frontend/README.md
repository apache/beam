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

# Apache Beam Playground

## About

Beam Playground is an interactive environment to try out Beam transforms and examples.
The vision for the Playground is to be a web application where users can try out Beam
without having to install/initialize a Beam environment.

## Getting Started

### Run

See [playground/README.md](../README.md) for details on requirements and setup.

The following command is used to build and serve the frontend app locally:

```bash
flutter run -d chrome
```

### Build

Run the following command to generate a release build:

```bash
flutter build web
```

This produces `build/web` directory with static files. Deploy them to your web server.

### Backend Lookup

The file [playground_components/lib/src/constants/backend_urls.dart](playground_components/lib/src/constants/backend_urls.dart)
is the location for backend-related constants.

If the `backendUrlOverrides` map contains a URL for a server then only it will be attempted
for the given container. This is useful for running backend locally.

Otherwise following patterns are tried when looking up the backend servers:
1. Prepending the frontend host with `router.`, `go.`, `java.`, `python.`, `scio.`.
2. Prepending the the default production frontend URL with the same.

### Docker

The app is deployed to production as a Docker container.
You can also run it in Docker locally. This is useful if:

1. You do not have Flutter and do not want to install it.
2. You want to mimic the release environment in the closest way possible.

To run the frontend app with Docker, run this in the frontend directory:

```bash
docker build -t playground-frontend .
docker run -p 1234:8080 playground-frontend
```

The container sets up NGINX on port 8080.
This example exposes it as port 1234 on the host,
and the app can be accessed at http://localhost:1234

## Code Generation

This project relies on generated code for some functionality:
deserializers, test mocks, constants for asset files,
extracted Beam symbols for the editor, etc.

All generated files are version-controlled, so after checkout the project is immediately runnable.
However, after changes you may need to re-run code generation.

### Standard Dart Code Generator

Most of the generated files are produced by running the standard Dart code generator.
This only requires Flutter, but must be called on multiple locations.
For convenience, run this single command:

```bash
./gradlew :playground:frontend:generateCode
```

### Generating Beam Symbols Dictionaries

Requirements:

- Python 3.8+ with packages: `ast`, `pyyaml`.

Other SDKs will add more requirements as we add extraction scripts for them.

To generate all project's generated files including symbol dictionaries, run:

```bash
./gradlew :playground:frontend:generate
```

### Deleting Generated Files

For consistency, it is recommended that you delete and re-generate all files before committing
if you have all required tools on your machine. To delete all generated files, run:

```bash
./gradlew :playground:frontend:cleanGenerated
```

## Validation

### Pre-commit Checks

To run all pre-commit checks, execute this in the beam root:

```bash
./gradlew :playground:frontend:precommit
```

This includes:

- Tests.
- Linter.

### Code Style

Code can be automatically reformatted using:

```bash
flutter format ./lib
```

### Unit Tests

To delete all generated files and re-generate them again and then run tests:

```bash
./gradlew :playground:frontend:playground_components:test
./gradlew :playground:frontend:test
```

To run tests without re-generating files:

```bash
cd playground/frontend/playground_components
flutter test
cd ..
flutter test
```

### Integration Tests

1. Install Google Chrome: https://www.google.com/chrome/
2. Install Chrome Driver: https://chromedriver.chromium.org/downloads
   - Note: This GitHub action installs both Chrome and Chrome Driver:
     https://github.com/nanasess/setup-chromedriver/blob/a249caaaad10fd12103028fd509853c2229eb6e6/lib/setup-chromedriver.sh
3. Run it on port 4444: `chromedriver --port=4444`
4. Run:

```bash
# To run in a visible Chrome window:
./gradlew :playground:frontend:integrationTest

# Headless run without a browser window:
./gradlew :playground:frontend:integrationTest -PdeviceId=web-server
```

By default, tests do not expect specific code and output from most examples.
This is because we get the expected example files from GitHub itself at runtime.
Examples in the default GitHub branch may differ from the deployed ones,
and this will break the tests.

To expect specific code and output, run tests like this using any repository owner/name
and commit reference to load the examples from:

```bash
./gradlew :playground:frontend:integrationTest -PexamplesRepository=apache/beam -PexamplesRef=master
```

## Localization

The project is in the process of migrating from
[the built-in Flutter localization](https://docs.flutter.dev/development/accessibility-and-localization/internationalization)
to [easy_localization](https://pub.dev/packages/easy_localization).
It temporarily uses both ways.

### Flutter Built-in Localization

To add a new localization, follow next steps:

1. Create app_YOUR_LOCALE_CODE.arb file with your key-translation pairs, except description tags, in lib/l10n directory (use app_en.arb as example).

2. Add Locale('YOUR_LOCALE_CODE') object to static const locales variable in lib/l10n/l10n.dart file.

3. Run the following command to generate a build and localization files:

```bash
flutter build web
```

### easy_localization

To add a new localization (using `fr` as an example):

1. Create `playground_components/assets/translations/fr.yaml`.
   Fill it with content copied from an existing translation file in another language.

2. Create `assets/translations/fr.yaml`.
   Fill it with content copied from an existing translation file in another language.

3. Add the locale to the list in `lib/l10n/l10n.dart`.

## URLs and Embedding

### Linking to a single example

#### 1. Linking to a catalog example by path

`https://play.beam.apache.org/?path=SDK_JAVA_AggregationMax&sdk=java`

Handled by `StandardExampleLoader`.

#### 2. Linking to the default catalog example

`https://play.beam.apache.org/?sdk=python&default=true`

Handled by `CatalogDefaultExampleLoader`.

#### 3. Linking to a user-shared example

`https://play.beam.apache.org/?sdk=java&shared=sdFdNV324HC`

Handled by `UserSharedExampleLoader`.

#### 4. Linking to an example by URL

`https://play.beam.apache.org/?sdk=go&url=https://raw.githubusercontent.com/golang/go/master/src/fmt/format.go`

Handled by `HttpExampleLoader`. The server with the example file must allow
the cross-origin access. GitHub is known to allow it.
Some servers may have different cross-origin policies when requested from localhost
and other domains.

#### 5. Linking to an empty editor

`https://play.beam.apache.org/?sdk=go&empty=true`

Handled by `EmptyExampleLoader`.

### Passing view options

Additional options may be passed with any of the above URL patterns.
For them to work, the example must contain sections with the following syntax:

```
// [START section_name]
void method() {
...
}
// [END section_name]
```

See more on the syntax and limitations in the
[README of the editor](https://pub.dev/packages/flutter_code_editor)
that Playground uses.

These options can be combined.

#### Read-only sections

Add `readonly` parameter with comma-separated section names:

`https://play.beam.apache.org/?sdk=go&url=https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go&readonly=iam_get_role`

#### Folding everything except sections

Add `unfold` parameter with comma-separated section names:

`https://play.beam.apache.org/?sdk=go&url=https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go&unfold=iam_get_role`

This folds all foldable blocks that do not overlap with
any of the given sections.

#### Hiding everything except a section

Add `show` parameter with a single section name:

`https://play.beam.apache.org/?sdk=go&url=https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go&show=iam_get_role`

It is still the whole snippet that is sent for execution although only the given section
is visible.

This also makes the editor read-only so the user cannot add code that conflicts
with the hidden text.

### Linking to multiple examples

With the above URLs, when the SDK is switched the following will be shown:

- The catalog default example for the new SDK in the standalone playground.
- The empty editor for the new SDK in the embedded playground.

This can be changed by linking to multiple examples, up to one per SDK.

For this purpose, make a JSON array with any combination of parameters that
are allowed for loading single examples, for instance:

```json
[
   {
      "sdk": "java",
      "path": "SDK_JAVA_AggregationMax"
   },
   {
      "sdk": "go",
      "url": "https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go",
      "readonly": "iam_get_role"
   }
]
```

Then pass it in`examples` query parameter like this:

`https://play.beam.apache.org/?sdk=go&examples=[{"sdk":"java","path":"SDK_JAVA_AggregationMax"},{"sdk":"go","url":"https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go","readonly":"iam_get_role"}]`

This starts with the Go example loaded from the URL.
If SDK is then switched to Java, the `AggregationMax` catalog example is loaded for it.
If SDK is switched to any other one, the default example for that SDK is loaded
because no override was provided.

### Embedded vs Standalone Playground URLs

Embedded Playground is a simplified interface supporting all the same features
as the primary interface which is also known as the Standalone Playground.

The embedded Playground URLs start with `https://play.beam.apache.org/embedded`
and use the same query string parameters as the standalone playground.

Additionally the Embedded playground supports the following parameters:

- `editable=0` to make the editor read-only.

### Embedding into HTML

Use the `<iframe>` tag to embed playground with any of the above URL patterns, for example:

```html
<iframe
  src="https://play-dev.beam.apache.org/embedded?path=SDK_JAVA_AggregationMax&sdk=java"
  width="800px"
  height="500px"
  allow="clipboard-write"
/>
```

Note that for the simplified embedded interface you need to use `/embedded` in the URL
otherwise the more complex default interface is shown.

Such URLs and `iframe` code can also be generated by clicking "Share my code" button
in the Standalone Playground.

### Embedding into the Beam documentation

Beam documentation is written in Markdown and uses [Hugo Markdown preprocessor](https://gohugo.io).
Use the custom shortcodes to embed Playground into the documentation:

- `playground` shortcode, see [a comment in it](https://github.com/apache/beam/blob/master/website/www/site/layouts/shortcodes/playground.html) for a complete example.
- `playground_snippet` shortcode, see [a comment in it](https://github.com/apache/beam/blob/master/website/www/site/layouts/shortcodes/playground_snippet.html) for all supported options.

These shortcodes generate an `iframe` with the URLs described above.

## Contribution guide

If you'd like to contribute to the Apache Beam Playground website, read
our [contribution guide](CONTRIBUTE.md) where you can find detailed instructions on how to work with
the playground.
