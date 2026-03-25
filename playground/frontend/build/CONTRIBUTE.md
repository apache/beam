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

- [Project Structure](#project-structure)
- [State Management](#state-management)
- [Example Loading](#example-loading)
- [Theming](#theming)
- [Adding a New Page](#adding-a-new-page)
- [Accessibility](#accessibility)

## Project Structure

The frontend consists of 3 projects:

- `frontend` is the playground app itself.
- `frontend/playground_components` is the package with common code for Playground and Tour of Beam.
- `frontend/playground_components_dev` is common code for tests of Playground and Tour of Beam.

## State Management

Playground uses the [app_state](https://pub.dev/packages/app_state) package for state management.
The standalone playground and the embedded playground are two screens within the same app,
chosen by the URL at runtime.

The main state object is `PlaygroundController`, created in both of those screens.
It is hung in the widget tree with the [provider](https://pub.dev/packages/provider) package
for historical reasons, and we aim to remove that if we do further refactoring.
New code should pass it directly from widget to widget for compile-time safety.

## Example Loading

A URL is parsed into one of the subclasses of `PagePath`, most of which contain
an `ExamplesLoadingDescriptor` object, which in turn may contain multiple `ExampleLoadingDescriptor`
objects.

This is passed to the `ExamplesLoader` object. It constructs an `ExampleLoader` object
for each example's descriptor, and that loader performs the actual loading.

To add a new source for examples:

1. Subclass `ExampleLoadingDescriptor` with state and a method to parse it from a map of query string parameters.
2. Add it to `ExamplesLoadingDescriptorFactory`.
3. Subclass `ExampleLoader` and load an example there.
4. Add it to the `ExampleLoaderFactory` in `ExamplesLoader`.

## Theming

Playground app supports light and dark themes. Component themes are declared
in [theme.dart](playground_components/lib/src/theme/theme.dart) file.

To use specific color inside component you can use helper `ThemeColors` utility:

`ThemeColors.of(context).greyColor`

[colors.dart](./lib/constants/colors.dart) contains color declarations.

## Adding a New Page

To add a new page, do the following steps:

1. Read [the overview](https://pub.dev/packages/app_state) of the `app_state` package.
2. Create a new `PagePath` subclass that parses the new URL.
3. Create a new `ChangeNotifier with PageStateMixin` that holds the state of the page.
4. Create a new `StatelessWidget` as the main one for the page.
5. Create a new `StatefulMaterialPage` subclass that binds the three together.

See the example in [lib/pages/standalone_playground](lib/pages/standalone_playground).

## Accessibility

Please, read the following guides about the accessibility:

- [Flutter Doc](https://docs.flutter.dev/development/accessibility-and-localization/accessibility)
- [Medium Article](https://medium.com/flutter-community/a-deep-dive-into-flutters-accessibility-widgets-eb0ef9455bc)
