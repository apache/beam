# Apache Beam (incubating) website

This is the website for [Apache Beam](http://beam.incubator.apache.org)
(incubating).

### About this site
The Beam website is built using [Jekyll](http://jekyllrb.com/). Additionally,
for additional formatting capabilities, this website uses
[Twitter Bootstrap](http://getbootstrap.com/).

This website is hosted at:

    http://beam.incubator.apache.org

It is important to note there are two sets of "website code"  with respect to
the Apache Beam website.

1. The **Jekyll code** which contains all of the resources for building,
testing, styling, and tinkering with the website. Think of it as a website SDK.
1. The **static website** content which contains the content for the
website. This is the static content is what is actually hosted on the Apache 
Beam website.

### Development setup
Before working with the Jekyll code, you will need to install Jekyll:

    $ gem install jekyll
    $ gem install jekyll-redirect-from
    $ gem install html-proofer
    $ gem install bundler

*If you are on a Mac, you may need to install
[Ruby Gems](https://rubygems.org/pages/download).*

### Live development
While you are working with the website, you can test and develop live. Run the
following command in the root folder of the website:

    $ jekyll serve

Jekyll will start a webserver on port `4000`. As you make changes to the
content, Jekyll will rebuild it automatically. This is helpful if you want to see
how your changes will render in realtime.

In addition, you can run the tests via:

    $ rake test

### Generating the static website
Once you are done with your changes, you need to compile the static
content for the website. This is what is actually hosted 
on the Apache Beam website.

You can build the static content by running the following command in the root
website directory:

    $ jekyll build

Once built, it will be placed in the folder `content` inside of the root directory. 
This directory will include images, HTML, CSS, and so on. In a typical Jekyll install
this content would live in `_site` - it has been changed for the Apache Beam website
to work with the ASF Incubator publishing system.

### Apache License
---
Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
