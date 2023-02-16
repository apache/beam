# How to Get Your Code into Apache Beam Playground

Getting your code into the Playground is a three-step process:

1. Prepare your code.
2. Place your code somewhere the Playground can load it from.
3. Create a link to view it or a code to embed.

For the last two steps there are multiple options -- pick any source and use any embedding:

![Workflow](doc/get_your_code/images/workflow.png)

They can be combined, e.g.
"put the code to the Playground catalog, embed the Playground to any HTML", or
"upload the code to any website, embed the Playground to Apache Beam docs with Markdown".

# Table of Contents
- [Step 1. Prepare Your Code](#step-1-prepare-your-code)
    * [Named Sections](#named-sections)
- [Step 2. Place Your Code](#step-2-place-your-code)
    * [Playground Visible Catalog](#playground-visible-catalog)
        + [1. Put the file to the directory](#1-put-the-file-to-the-directory)
        + [2. Add metadata](#2-add-metadata)
        + [3. Make a PR](#3-make-a-pr)
        + [4. Save the snippet path](#4-save-the-snippet-path)
    * [Playground Unlisted Database](#playground-unlisted-database)
    * [Tour of Beam unit](#tour-of-beam-unit)
    * [User-shared Code](#user-shared-code)
    * [HTTPS](#https)
- [Step 3. Create a link or embed](#step-3-create-a-link-or-embed)
    * [Direct Link to the Standalone Playground Web App](#direct-link-to-the-standalone-playground-web-app)
        + [1. Playground Visible Catalog](#1-playground-visible-catalog)
        + [2. Playground Unlisted Database](#2-playground-unlisted-database)
        + [3. Tour of Beam Unit](#3-tour-of-beam-unit)
        + [4. User-shared Code](#4-user-shared-code)
        + [5. HTTPS](#5-https)
        + [6. Empty Editor](#6-empty-editor)
        + [Passing View Options](#passing-view-options)
            - [Read-only sections](#read-only-sections)
            - [Folding everything except sections](#folding-everything-except-sections)
            - [Hiding everything except a section](#hiding-everything-except-a-section)
        + [Linking to multiple examples](#linking-to-multiple-examples)
        + [Embedded vs Standalone Playground URLs](#embedded-vs-standalone-playground-urls)
    * [Embed into HTML](#embed-into-html)
        + [1. Playground Visible Catalog](#1-playground-visible-catalog-1)
        + [2. User-shared Code](#2-user-shared-code)
        + [3. All Other Code Sources](#3-all-other-code-sources)
    * [Embedding into the Beam documentation](#embedding-into-the-beam-documentation)


## Step 1. Prepare Your Code

Playground has multiple features to focus users to certain parts of the code.
It does the following to all snippets:

- Folds a comment if a snippet starts with one.
- Folds imports.

Additionally it can apply the following view options:

- Fold all blocks except certain ones.
- Completely hide all code except certain parts.
- Make certain parts read-only.

If you do not need any of those view options, skip to the next step.

### Named Sections

For those features to work, the snippet must contain sections with the following syntax:

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

Create a named section for each part of your code that you want that features for.

## Step 2. Place Your Code

### Playground Visible Catalog

If you think your snippet is beneficial to many users, it's a good idea to add it to
the Playground Catalog.

Benefits:

- Anyone can find your snippet in the dropdown when they open Playground.
- The CI of the Beam repository guarantees your snippet builds and runs correctly.
- Output and graph are cached so the viewers of your snippet will not wait when they run it.

#### 1. Put the file to the directory

Snippets for the Playground Catalog are automatically picked from predefined categories
by a workflow **(WHICH ONE?)** after merging a PR.

This directories are:

| SDK | Snippet path root |
|-|-|
| Java | **?** |
| Python | **?** |
| Go | **?** |

Place the file anywhere under the given directory.

**TODO: NAMING, DIRECTORY STRUCTURE?**

Snippets in Scala are not yet supported for by the workflows.
All existing Scala examples are added manually by the team.
Please use other options to place your Scala snippets.

#### 2. Add metadata

Playground needs metadata to put a snippet to the database. These are stored as comments.
See [this](https://github.com/apache/beam/blob/3e080ff212d8ed7208c8486b515bb73c5d294475/examples/java/src/main/java/org/apache/beam/examples/MinimalWordCount.java#L20-L36) for an example.
This comment block is cut from the text before putting it to the database and so is not visible
to end users. The block is in the format of a YAML map.

The following attributes are required:

| Attribute | Description | Values |
|-|-|-|
| `description` | Description text to show on mouse over. | Text | Required |
| `multifile` | Whether this is a file of a multi-file example. | `true`, `false` | Required |
| `name` | Name to show in the dropdown. | Text| Required |

The following optional attributes are supported:

| Attribute | Description | Values |
|-|-|-|
| `categories` | Titles of categories to list this snippet in. Non-existent categories will be created. | Array of strings |
| `complexity` | How hard is the snippet to understand. | `BASIC`, `MEDIUM`, `ADVANCED` |
| `context_line` | The line number to scroll to when the snippet is loaded. This applies after the metadata block is removed from the file. | Integer, 1-based |
| `default_example` | Whether this is the default example in its SDK. If multiple snippets set this to `true` the behavior is undefined. | `false` (default), `true` |
| `tags` | Tags by which this snippet can be found in the dropdown. | Array of strings |

**TODO: ARE THE REQUIRED AND OPTIONAL ATTRIBUTES IDENTIFIED CORRECTLY?**

**TODO: ARE THERE ANY OTHER SUPPORTED ATTRIBUTES?**

#### 3. Make a PR

Make a PR into [the Beam repository](https://github.com/apache/beam).
Make sure all checks are green.

A reviewer will be assigned. After merge the snippet will be visible in the Playground dropdown.

The CI we have will also check that the example compiles and runs alright.

#### 4. Save the snippet path

The snippet will be assigned a path to identify it.
You can find it in the address bar of the browser when you select it in the dropdown.

For example, this URL:

```
https://play.beam.apache.org/?path=SDK_JAVA%2FPRECOMPILED_OBJECT_TYPE_EXAMPLE%2FMinimalWordCount&sdk=java
```

after [URL decoding](https://www.urldecoder.org) indicates this snippet path:

```
SDK_JAVA/PRECOMPILED_OBJECT_TYPE_EXAMPLE/MinimalWordCount
```

You will need it to embed the Playground code.

### Playground Unlisted Database

If your snippet is less useful to public for learning but still beneficial to some group
of people, you can put it into the same database as the Playground Visible Catalog
but make it unlisted.

Benefits:

- The CI of the Beam repository guarantees your snippet builds and runs correctly.
- Output and graph are cached so the viewers of your snippet will not wait when they run it.

Proceed the same way as with Playground Visible Catalog, but do not use these attributes:

- `categories`
- `default_example`
- `tags`

**TODO: How to get the path of the example?**

### Tour of Beam unit

If your snippet fills a gap in the Tour of Beam tutorial, you can add it there
as a learning unit. This also requires a textual learning material that your snippet will accompany.

Benefits:

- Anyone can find your snippet in the Tour of Beam tutorial.
- The CI of the Beam repository guarantees your snippet builds and runs correctly.
- Output and graph are cached so the viewers of your snippet will not wait when they run it.

**TODO: HOWTO**

### User-shared Code

Code can be uploaded directly to the database with "Share my code" button in Playground:

![Workflow](doc/get_your_code/images/share-my-code.png)

Benefits:

- No approval required.
- Fast.

Drawbacks:

- The code may be deleted if it is not loaded by anyone during a 6 months rolling window.
- No validation.
- No cached output and graph.
- A snippet is immutable. If you edit the code and re-share, you get a new link.
- You cannot delete the code uploaded by accident.

After clicking the button you will get a shareable link or embeddable HTML code.
Note the `shared` parameter in the link query string.
It contains the ID of your snippet that you can later use with other sharing methods.

### HTTPS

You can upload a snippet file to any HTTPS-server you have access to.
Then you refer to it by a URL to load into Playground.

Benefits:

- No approval required.
- Fast.
- You can change the snippet without changing a link.

Drawbacks:

- No validation.
- No cached output and graph.

For Playground to be able to load the snippet over HTTPS,
your server need to allow the access by sending the following header:

```
Access-Control-Allow-Origin: *
```

at least when requested with `*.beam.apache.org` as
[`referer`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Referer).

To understand the reasons, read about
[CORS (Cross-Origin Resource Sharing)](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS).

The most convenient hosting for snippets is your GitHub repository.
GitHub is known to allow such access on direct links to raw file content.

## Step 3. Create a link or embed

Once you uploaded or otherwise prepared your code, you can get it shown in the Playground.
Choose any of the following ways.

### Direct Link to the Standalone Playground Web App

You can link to the Playground app so that it opens your code. The link depends on
where you placed the code.

#### 1. Playground Visible Catalog

1. Open your snippet in the dropdown menu.
2. Without changing it, click "Share my code".
3. Copy the link.

The link contains the `path` to your snippet in the database. It is in the following format:
```
https://play.beam.apache.org/?path=SDK_JAVA/PRECOMPILED_OBJECT_TYPE_KATA/AggregationMax&sdk=java
```

A special case is the default snippet for an SDK. It can be loaded by the following link:

```
`https://play.beam.apache.org/?sdk=python&default=true`
```

This way if another snippet is ever made default, the links you shared will lead
to the new snippet.

#### 2. Playground Unlisted Database

The code can be accessed with the same link as with Playground Visible Catalog.
Since the snippet is unlisted, you cannot select it in the dropdown and so you should
manually edit the link. Use the example above and replace the `path` and `sdk` with yours.

#### 3. Tour of Beam Unit

**TODO**

#### 4. User-shared Code

You get the link when you click "Share my code" button. It is in the following format:

```
https://play.beam.apache.org/?sdk=java&shared=SNIPPET_ID
```

#### 5. HTTPS

Add the URL to the `url` parameter, for example:

```
https://play.beam.apache.org/?sdk=go&url=https://raw.githubusercontent.com/golang/go/master/src/fmt/format.go
```

#### 6. Empty Editor

You can link to an empty editor to make uses start their snippets from scratch:

```
https://play.beam.apache.org/?sdk=go&empty=true
```

#### Passing View Options

If your code contains named sections as described in the beginning of this page,
you can apply view options to those sections. Otherwise skip this.

##### Read-only sections

Add `readonly` parameter with comma-separated section names:

`https://play.beam.apache.org/?sdk=go&url=https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go&readonly=iam_get_role`

##### Folding everything except sections

Add `unfold` parameter with comma-separated section names:

`https://play.beam.apache.org/?sdk=go&url=https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go&unfold=iam_get_role`

This folds all foldable blocks that do not overlap with
any of the given sections.

##### Hiding everything except a section

Add `show` parameter with a single section name:

`https://play.beam.apache.org/?sdk=go&url=https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go&show=iam_get_role`

It is still the whole snippet that is sent for execution although only the given section
is visible.

This also makes the editor read-only so the user cannot add code that conflicts
with the hidden text.

#### Linking to multiple examples

The above URLs load a snippet that you want. But what happens if the user switches SDK?
Normally this will be shown:

- The catalog default example for the new SDK in the Standalone Playground.
- The empty editor for the new SDK in the Embedded Playground.

This can be changed by linking to multiple examples, up to one per SDK.

For this purpose, make a JSON array with any combination of parameters that
are allowed for loading single examples, for instance:

```json
[
   {
      "sdk": "java",
      "path": "SDK_JAVA/PRECOMPILED_OBJECT_TYPE_KATA/AggregationMax"
   },
   {
      "sdk": "go",
      "url": "https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go",
      "readonly": "iam_get_role"
   }
]
```

Then pass it in`examples` query parameter like this:

`https://play.beam.apache.org/?sdk=go&examples=[{"sdk":"java","path":"SDK_JAVA/PRECOMPILED_OBJECT_TYPE_KATA/AggregationMax"},{"sdk":"go","url":"https://raw.githubusercontent.com/GoogleCloudPlatform/golang-samples/main/iam/snippets/roles_get.go","readonly":"iam_get_role"}]`

This starts with the Go example loaded from the URL.
If SDK is then switched to Java, the `AggregationMax` catalog example is loaded for it.
If SDK is switched to any other one, the default example for that SDK is loaded
because no override was provided.

#### Embedded vs Standalone Playground URLs

Embedded Playground is a simplified interface of the same Playground app.
It supports most of the features of the full "Standalone Playground" interface.
It is designed to be embedded into an `<iframe>` in web pages.

The embedded Playground URLs start with `https://play.beam.apache.org/embedded`
and use the same query string parameters as the standalone playground.

Additionally the Embedded playground supports the following parameters:

- `editable=0` to make the editor read-only.

### Embed into HTML

#### 1. Playground Visible Catalog

1. Open your snippet in the dropdown menu.
2. Without changing it, click "Share my code".
3. Go to "Embed" tab.
4. Copy the HTML code.

#### 2. User-shared Code

1. Open your code by the link that you got when you shared it.
2. Again click "Share my code".
3. Go to "Embed" tab.
4. Copy the HTML code.

#### 3. All Other Code Sources

1. Follow the instructions to get a link to your code.
2. Optionally make the link to the Embedded Playground by replacing `beam.apache.org/?...`
   with `beam.apache.org/embedded?...` because the embedded interface is simpler.
3. Insert this link into an `<iframe>` HTML element like this:

```html
<iframe
  src="https://play.beam.apache.org/embedded?sdk=go&url=https://raw.githubusercontent.com/golang/go/master/src/fmt/format.go"
  width="90%"
  height="600px"
  allow="clipboard-write"
/>
```

### Embedding into the Beam documentation

Beam documentation is written in Markdown and uses [Hugo Markdown preprocessor](https://gohugo.io).
Use the custom shortcodes to embed Playground into the documentation:

- `playground` shortcode, see [a comment in it](https://github.com/apache/beam/blob/master/website/www/site/layouts/shortcodes/playground.html) for a complete example.
- `playground_snippet` shortcode, see [a comment in it](https://github.com/apache/beam/blob/master/website/www/site/layouts/shortcodes/playground_snippet.html) for all supported options.

These shortcodes generate an `iframe` with the URLs described above.

