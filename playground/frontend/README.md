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

Running, debugging, and testing all require this first step that fetches
dependencies and generates code. Run this in the Beam root:

```bash
$ ./gradlew :playground:frontend:configure
```

### Run

See [playground/README.md](../README.md) for details on requirements and setup.

The following command is used to build and serve the frontend app locally:

`$ flutter run`

### Build

Run the following command to generate a release build:

`$ flutter build web`

This produces `build/web` directory with static files. Deploy them to your web server.

### Docker

To run the frontend app with Docker, run this in the frontend directory:

```bash
$ docker build -t playground-frontend .
$ docker run -p 1234:8080 playground-frontend
```

The container sets up NGINX on port 8080.
This example exposes it as port 1234 on the host,
and the app can be served at http://localhost:1234

### Pre-commit Checks

To run all pre-commit checks, execute this in the beam root:

```bash
./gradlew :playground:frontend:precommit
```

This includes:

- Tests.
- Linter.

### Code style

Code can be automatically reformatted using:

`$ flutter format ./lib`

### Localization

The project is in the process of migrating from
[the built-in Flutter localization](https://docs.flutter.dev/development/accessibility-and-localization/internationalization)
to [easy_localization](https://pub.dev/packages/easy_localization).
It temporarily uses both ways.

#### Flutter Built-in Localization

To add a new localization, follow next steps:

1. Create app_YOUR_LOCALE_CODE.arb file with your key-translation pairs, except description tags, in lib/l10n directory (use app_en.arb as example).

2. Add Locale('YOUR_LOCALE_CODE') object to static const locales variable in lib/l10n/l10n.dart file.

3. Run the following command to generate a build and localization files:

`$ flutter build web`

#### easy_localization

To add a new localization (using `fr` as an example):

1. Create `playground_components/assets/translations/fr.yaml`.
   Fill it with content copied from an existing translation file in another language.

2. Create `assets/translations/fr.yaml`.
   Fill it with content copied from an existing translation file in another language.

3. Add the locale to the list in `lib/l10n/l10n.dart`.

### Configuration

The app could be configured using gradle task (e.g. api url)

```
cd beam
./gradlew :playground:frontend:createConfig
```

For more information see See [CONTRIBUTE.md](CONTRIBUTE.md)

### Additional

The full list of commands can be found [here](https://flutter.dev/docs/reference/flutter-cli)

## Contribution guide

If you'd like to contribute to the Apache Beam Playground website, read
our [contribution guide](CONTRIBUTE.md) where you can find detailed instructions on how to work with
the playground.
