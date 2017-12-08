---
layout: section
title: "Beam Eclipse Tips"
permalink: /contribute/eclipse/
section_menu: section-menu/contribute.html
---

# Eclipse Tips

> These are best-effort community-contributed tips, and are not...
>
> - ... guaranteed to work with any particular Eclipse setup.
> - ... the only or best way to work.
> - ... an endorsement of Eclipse over alternatives.
>
> Mastering Eclipse is, ultimately, your responsibility.

## Eclipse version

Use a recent Eclipse version that includes m2e. Currently we recommend Eclipse
Neon. Start Eclipse with a fresh workspace in a separate directory from your
checkout.

## Initial setup

1. Install m2e-apt: Beam uses apt annotation processing to provide auto
   generated code. One example is the usage of [Google
   AutoValue](https://github.com/google/auto/tree/master/value). By default m2e
   does not support this and you will see compile errors.

	Help
	-> Eclipse Marketplace
	-> Search for "m2 apt"
	-> Install m2e-apt 1.2 or higher

2. Activate the apt processing

	Window
	-> Preferences
	-> Maven
	-> Annotation processing
	-> Switch to Experimental: Delegate annotation processing ...
	-> Ok

3. Import the beam projects

	File
	-> Import...
	-> Existing Maven Projects
	-> Browse to the directory you cloned into and select "beam"
	-> make sure all beam projects are selected
	-> Finalize

You now should have all the beam projects imported into Eclipse and should see
no compile errors.

## Checkstyle

Eclipse supports checkstyle within the IDE using the Checkstyle plugin.

1. Install the [Checkstyle
   plugin](https://marketplace.eclipse.org/content/checkstyle-plug).
2. Configure the Checkstyle plugin by going to Preferences -> Checkstyle.
    1. Click "New..."
    2. Select "External Configuration File" for type
    3. Click "Browse..." and select
       `sdks/java/build-tools/src/main/resources/beam/checkstyle.xml`
    4. Enter "Beam Checks" under "Name:"
    5. Click "OK", then "OK"

## Code Style

Eclipse supports code styles within the IDE. Use one or both of the following
to ensure your code style matches the project's checkstyle enforcements.

1. The simplest way to have uniform code style is to use the [Google
   Java Format plugin](https://github.com/google/google-java-format#eclipse)
2. You can also configure Eclipse to use `beam-codestyle.xml`
    1. Go to Preferences -> Java -> Code Style -> Formatter
    2. Click "Import..." and select
       `sdks/java/build-tools/src/main/resources/beam/beam-codestyle.xml`
    3. Click "Apply" and "OK"

