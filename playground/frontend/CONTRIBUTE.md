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

# Contribution Guide

This guide consists of:

- [Project structure](#project-structure)
- [State Management](#state-management)
- [Configuration](#configuration)
- [Theming](#theming)
- [Adding a new page](#adding-a-new-page)
- [Accessibility](#accessibility)
- [Unit testing](#unit-testing)
- [Generated Files](#generated-files)

## Project structure

```
frontend/
├── web
├── lib
│   ├── api                 # generated dart client for the grpc api
│   ├── components          # general UI components used across the app
│   ├── config              # general configs e.g. theme
│   ├── constants           # different consts file like colors,sizes
│   ├── modules             # different parts of the app
│   │   └── actions                 # common actions for the pages like new example, reset
│   │   └── editor                  # editor text field and run code
│   │   └── examples                # everything related to loading/showing examples
│   │   └── notifications           # common notications system
│   │   └── output                  # component to show log/output/graph result of running code
│   │   └── sdk                     # sdk selector
│   │   └── shortcuts               # shortcuts modal and definitions
│   ├── pages               # playground pages
│   │   └── playground              # main playground editor paage
│   │   └── embedded_playground     # embedded version of the editor
├──  test               # folder with unit tests
├── pubspec.yaml        # infromation about application and dependencies
├── build.gradle        # gradle tasks for playground frontends
├── gradle.properties   # default properties for project
```

## State Management

Playground uses [provider](https://pub.dev/packages/provider) package for state management. We have
top level providers like `ThemeProvider`, common page level provider `PlaygroundPageProviders` which
contains decoupled playground page state and module providers like `OutputPlacementState`.

For quick start up, please take a look
to [this guide](https://docs.flutter.dev/development/data-and-backend/state-mgmt/simple)

## Configuration

The app could be configured using gradle task (e.g. api url)

```
cd beam
./gradlew :playground:frontend:createConfig
```

The command generates [./lib/config.g.dart](./lib/config.g.dart) file with constants from
gradle.properties which can be used in the app

To add a new constant do the following steps:

- Add a new line with const to the `:playground:frontend:createConfig` task
  inside [build.gradle](./build.gradle);
- Update [gradle.properties](./gradle.properties);
- Run task to test it and commit [./lib/config.g.dart](./lib/config.g.dart) file.

## Theming

Playground app supports light and dark themes. Component themes are declared
in [theme.dart](./lib/config/theme.dart) file.

To use specific color inside component you can use helper `ThemeColors` utility:

`ThemeColors.of(context).greyColor`

[colors.dart](./lib/constants/colors.dart) contains color declarations.

## Adding a new page

To add a new page do the following steps:

- Add a page component to the `pages` folder
- Add a url to the [routes.dart](./lib/pages/routes.dart) class as a static property
- Add a case clause to the same class with your component

```
case Routes.page_url:
    return Routes.renderRoute(const PageComponent());
```

## Accessibility

Please, read the following guides about the accessibility:

- [Flutter Doc](https://docs.flutter.dev/development/accessibility-and-localization/accessibility)
- [Medium Article](https://medium.com/flutter-community/a-deep-dive-into-flutters-accessibility-widgets-eb0ef9455bc)

## Unit testing

Unit tests are on `tests` folder. It keeps the project structure from the `lib` folder
with `_test.dart` file postfix.

We are using standard flutter library for unit testing
and [Mockito](https://pub.dev/packages/mockito) for generating mocks. To generate mocks for class
you need to add `@GenerateMocks([ExampleClient])` annotation to a test file and
run `$ flutter pub run build_runner build` command.

Playground tests may be run using next commands:

`$ flutter test`

## Generated Files

All generated files (generated api clients, mocks) should be published to the repository.
