---
layout: default
title: "Beam Blog"
permalink: /blog/
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache Beam Blog

This is the blog for the Apache Beam project. This blog contains news and updates
for the project.

{% for post in site.posts %}
{% assign authors = post.authors %}

### <a class="post-link" href="{{ post.url | prepend: site.baseurl }}">{{ post.title }}</a>
<i>{{ post.date | date: "%b %-d, %Y" }}{% if authors %} •
{% assign count = authors | size %}{% for name in authors %}{% if forloop.first == false and count > 2 %},{% endif %}{% if forloop.last and count > 1 %} &amp;{% endif %}{% assign author = site.data.authors[name] %} {{ author.name }} {% if author.twitter %}[<a href="https://twitter.com/{{ author.twitter }}">@{{ author.twitter }}</a>]{% endif %}{% endfor %}
{% endif %}</i>

{{ post.excerpt }}

<!-- Render a "read more" button if the post is longer than the excerpt -->
{% capture content_words %}
  {{ post.content | number_of_words }}
{% endcapture %}
{% capture excerpt_words %}
  {{ post.excerpt | number_of_words }}
{% endcapture %}
{% if excerpt_words != content_words %}
<p>
<a class="btn btn-default btn-sm" href="{{ post.url | prepend: site.baseurl }}" role="button">
Read more&nbsp;<span class="glyphicon glyphicon-menu-right" aria-hidden="true"></span>
</a>
</p>
{% endif %}

<hr>
{% endfor %}
