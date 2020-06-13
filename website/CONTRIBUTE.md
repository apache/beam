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
- [Configuration walkthrough](#configuration-walkthrough)
- [How to add a new doc](#how-to-add-a-new-doc)
- [How to add a new blogpost](#how-to-add-a-new-blogpost)
- [How to add a new landing page](#how-to-add-a-new-landing-page)
- [How to write in Hugo way](#how-to-write-in-hugo-way)
  - [Define TableOfContents](#define-tableofcontents)
  - [Language switching](#language-switching)
  - [Code highlighting](#code-highlighting)
  - [Adding class to markdown text](#paragraph)
  - [Table](#table)
  - [Github sample](#github-sample)
  - [Others](#others)
- [What to be replaced in Jekyll](#what-to-be-replaced-in-jekyll)
- [Translation guide](#translation-guide)

## Project structure

```
www/
├── dist                                  # bundle files
├── site
│   ├── archetypes                        # frontmatter template
│   ├── assets
│   │   └── scss                          # styles
│   ├── content                           # pages
│   │   └── en
│   │       ├── blog
│   │       ├── community
│   │       ├── contribute
│   │       ├── documentation
│   │       ├── get-started
│   │       ├── privacy_policy
│   │       ├── roadmap
│   │       └── security
│   │       └── _index.md
│   ├── data
│   ├── layouts                           # content template
│   ├── static
│   │   ├── downloads                     # downloaded files
│   │   └── fonts
│   │   └── images
│   │   └── js
│   └── themes
│       └── docsy
├── build_code_samples.sh
├── check-links.sh                        # links checker
└── package.json
```

## Configuration walkthrough

If you prefer to experience locally instead of using our gradle commands, this [Hugo installation](https://gohugo.io/getting-started/installing/) is a good start.

When we mention the `config file` in this documentation, we mean the Hugo configuration file located at `/www/site/config.toml`.

Most of the setup are self-explained in the comments. Please refer to the [Hugo documentation](https://gohugo.io/getting-started/configuration/) for more details.

You should notice at `[params]`, they are considered as global variables. For instance, when you'd like to replace the release latest version, make a change on `release_latest = ""` to replace it everywhere in the project.

## How to add a new doc

Let's start with an example as you'd like to add a new doc named `new-doc` in `/www/site/content/en/documentation/runtime/`. Locate to `/www/site/` and run:

```
$ hugo new documentation/runtime/new-doc.md
```

A markdown file will be created  with pre-filled frontmatter:

```
---
title: "New Doc"
---
```

Then, put your content below the frontmatter. Do not worry about its layout, since its inside `documentation`, the layout at `/www/site/layouts/documentation/` will be shared with its children.

**Note**: if you'd like to add a new doc in another language, apart from English version. For example, a new doc inside `pl` directory. Use:

```
$ hugo new -c content/pl documentation/runtime/new-doc.md
```

## How to add a new blogpost

To add a new blogpost with pre-filled frontmatter, in `/www/site/` run:

```
$ hugo new blog/my-new-blogpost.md
```

That will create a markdown file `/www/site/content/en/blog/my-new-blogpost.md` with following content:

```
---
title: "My New Blogpost"
date: "2020-04-20T14:02:57+02:00"
categories: 
  - blog
authors: 
  - "Your Name"
---
```

Below frontmatter, put your blogpost content. The filename will also serve as URL for your blogpost as `/blog/{filename}`. Don't forget to add `<!--more-->`, which is the delimiter between summary and the main content.

**Note**: if you'd like to add a new blog in another language, apart from English version. For example, a new blog inside `pl` directory. Use:

```
$ hugo new -c content/pl blog/my-new-blogpost.md
```

## How to add a new landing page

For example, you would like to add a new `About` page.

First, you need to create a markdown file in `/www/site/content/<LANGUAGE_VERSION>/about/_index.md` with following content:

```
---
title: "Your page title"
---
```

Below frontmatter, put your page content. The filename will also serve as URL for your page as `/about`.

Second, define your page layout in the `layout` section with the same structure `/www/site/layout/about/{your_template}`. Hugo will help you to pick up the template behind the scene. Please refer to the [Hugo documentation](https://gohugo.io/templates/) for the usage of templates.

You can also create a new page with pre-filled frontmatter, for instance, in `/www/site/` run:

```
$ hugo new about/_index.md
```

**Note**: if you'd like to add a new page in another language, apart from English version. For example, a new page inside `pl` directory. Use:

```
$ hugo new -c content/pl about/_index.md
```

## How to write in Hugo

This section will guide you how to use Hugo shortcodes in Apache Beam website. Please refer to the [Hugo documentation](https://gohugo.io/content-management/shortcodes/) for more details of usage.

### Define TableOfContents

To automatically generate table of contents in a markdown file. Simply use:

```
{{< toc >}}
```

### Language switching

To have a programming language tab switcher, for instance of java, python and go. Use:

```
{{< language-switchers java py go >}}
```

The purpose is to switch languages of codeblocks.

### Code highlighting

To be consistent, please prefer to use `{{< highlight >}}` syntax instead of ` ``` `, for code-blocks or syntax-highlighters.

1. To apply code highlighting to java, python or go. Use:

```
{{< highlight java >}}
// This is java
{{< /highlight >}}
```

2. To apply code highlighting to a wrapper class

Usage:

```
{{< highlight class="class-name" >}}
Write some code here.
{{< /highlight >}}
```

Render:

```
<div class="class-name">
  <pre>
    <code>
    "Write some code here."
    </code>
  </pre>
</div>
```

The purpose of adding classes or programming languages (java, py or go) in code highlighting is to activate the language switching feature.

### Adding class to markdown text

1. To add a class to a single line in mardown. Use:

```
{{< paragraph class="java-language">}}
This is an inline markdown text.
{{< /paragraph >}}
```

2. To add a class to multi lines in markdown. Use:

```
{{< paragraph class="java-language" wrap="span">}}
- This is the first text.
- This is the second text.
- This is the third text.
{{< /paragraph >}}
```

The purpose of adding classes in markdown text is to activate the language switching feature.

### Table

If you would like to use the table markdown syntax but also have bootstrap table styles. Wrap your table markdown inside:

```
{{< table >}}
A table markdown here.
{{< /table >}}
```

### Code sample

To retrieve a piece of code from Beam project.

Usage:

```
{{< code_sample "path/to/file" selected_tag >}}
```

Example:

```
{{< code_sample "sdks/python/apache_beam/examples/complete/game/user_score.py" extract_and_sum_score >}}
```

### Others

To get released latest version in markdown:

```
{{< param release_latest >}}
```

To get branch of the repository in markdown:

```
{{< param branch_repo >}}
```

To render capability matrix, please take a look at [this example](/www/site/content/en/documentation/runners/capability-matrix/#beam-capability-matrix).

## What to be replaced in Jekyll

This section will briefly let you know the replaced features of Jekyll in terms of writing a new blog post or documentation in Hugo.

1. Redirect to:

The `redirect_to` feature will no longer be used since Hugo doesn't support it. You can solve this in Hugo by replacing the external URLs directly in links, instead of using markdown file to be the third-person.

Currently, there are 3 removed `redirect_to` links which were used in Jekyll:

```
/contribute/project/team/         # https://home.apache.org/phonebook.html?pmc=beam
/contribute/team/                 # https://home.apache.org/phonebook.html?pmc=beam
/contribute/design-documents/     # https://cwiki.apache.org/confluence/display/BEAM/Design+Documents
```

2. Redirect from:

Fortunately, Hugo supports `redirect_from` with `aliases` in the frontmatter.

```
aliases:
  - /path/example.html
```

3. IALs:

IALs feature is used in Jekyll to add a class to markdown paragraph, `{:.myclass}` as an example. And to show this matter, we use Hugo shortcodes to [add a class to inline texts](#adding-class-to-markdown-text) or [blocks](#code-highlighting).

4. Filenames of blog posts:

In Jekyll, filenames included the typical date prefix as part of the filename and it will cause some issues when we'd like to change the date later. Hugo prefers to get rid of them and add date as metadata in frontmatter.

5. Relative URLs:

`{{ site.baseurl }}` will no longer be used, since Hugo handle the relative or absolute path in the config file.

6. Global variables:

The `param` - global variables are placed in the [config file](#configuration-walkthrough).

In Jekyll:

```
{{ site.release_latest }}
{{ site.branch_repo }}
```

In Hugo:

```
{{< param release_latest >}}
{{< param branch_repo >}}
```

## Translation guide

In order to add a new language into Apache Beam website, please follow this guide. You could take a look at an [example branch](https://github.com/PolideaInternal/beam/tree/example/i18n/) to see how we completely translate the whole website.

For more details of syntax, please refer to the [Hugo documentation](https://gohugo.io/content-management/multilingual/). Below is a step-by-step instructions of translating our website to Polish as an example.

1. Configuring a new language
  
Firstly, we add the following params to our config file `/www/site/config.toml`.

```
[languages.pl]
contentDir = "content/pl"
title = "Apache Beam title"
description = "Apache Beam description"
languageName = "Polish"
weight = 2
```

2. Translating markdown contents

The `www/site/content/pl` directory will be your main workspace of contents here. Therefore, you need to translate all of the markdown files inside `/www/site/content/en` and place them into your workspace. Remember to keep the same project structure for both, since they're sharing the same layouts.

3. Localizing our strings

Some of the texts are placed into layouts which are html files, you need to translate all of these phrases inside `www/site/i18n`. Afterwards from our templates, Hugo's `i18n` function does the localization job. Please follow [our example](https://github.com/PolideaInternal/beam/tree/example/i18n/website/www/site/i18n) to understand the structure.

4. Data files

Consider the following structure for your data directories `/www/site/data` where `en` and `pl` are your website’s languages’ respective codes.

```
data
  ├── en
  │   └── people.yaml
  └── pl
      └── people.yaml
```

Now from your template:

```
{{ $data := index .Site.Data .Site.Language.Lang }}
{{ range $data.people }}
  <a href="{{ .url }}">{{ .name }}</a>
{{ end }}
```

5. Section menus

Similar to markdown content translation, there are two separated section menus `/www/site/layouts/partials/section-menu` corresponding to your languages. Your job is to take the section menus in `en` directory, translate and place them inside your `pl` directory.

**Note**: if you get stuck at adding translation, please refer to [our example](https://github.com/PolideaInternal/beam/tree/example/i18n/).
