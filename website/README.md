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

These are the main sources of the website for Apache Beam, hosted at
https://beam.apache.org/.

## About this site

The Beam website is built using [Jekyll](http://jekyllrb.com/). Additionally,
for additional formatting capabilities, this website uses
[Twitter Bootstrap](http://getbootstrap.com/).

Documentation generated from source code, such as Javadoc and Pydoc, is stored
separately on the [beam-site
repository](https://github.com/apache/beam-site/tree/release-docs).

## Active development

Website development requires Docker installed if you wish to preview changes and
run website tests.

The following command is used to build and serve the website locally.

    $ ./gradlew :website:serveWebsite

Any changes made locally will trigger a rebuild of the website.

Websites tests may be run using this command:

    $ ./gradlew :website:testWebsite

## Website push

After a PR is merged, a background Jenkins job will automatically generate and
push [website
content](https://github.com/apache/beam/tree/asf-site/website/generated-content)
to the asf-site branch. This content is later picked up and pushed to
https://beam.apache.org/.

## Additional Information

### Writing blog posts

Blog posts are created in the `_posts` directory.

If this is your first post, make sure to add yourself to `_data\authors.yml`.

While you a working on your post before the publishing time listed in its header,
add `--future` when running Jekyll in order to view your draft on your local copy of
the site.

### Adding Jekyll plugins

If you modify the site to use additional Jekyll plugins, add them in `Gemfile`
and then run `bundle update`, which will regenerate the complete `Gemfile.lock`.
Make sure that the updated `Gemfile.lock` is included in your pull request. For more information,
see the Bundler [documentation](http://bundler.io/v1.3/rationale.html).

